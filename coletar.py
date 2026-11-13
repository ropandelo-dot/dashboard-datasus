#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
coletar.py - Robô de coleta de dados do DATASUS.

Versão atual: SINASC (nascimentos) ATIVADO.

Uso:
    python coletar.py --base sinasc --uf RJ     # coleta um estado
    python coletar.py --consolidar              # junta tudo em dados/

Fluxo:
  1. Para cada ano, procura o arquivo DN<UF><ANO>.DBC no FTP do DATASUS.
  2. Baixa, descompacta, lê só as colunas necessárias e conta nascimentos
     por município (total, sexo, raça/cor, idade da mãe, escolaridade da mãe).
  3. Grava saida/<base>/<UF>_<ANO>.parquet (e uma versão por UF em _uf/).
  4. --consolidar junta os arquivos de saida/ em dados/<base>/ (um parquet
     por estado + um parquet "Brasil por UF"), que é o que o dashboard lê.

Organização dos arquivos finais (em dados/<base>/):
    <UF>.parquet        municípios do estado, todos os anos
    _brasil.parquet     todas as UFs (cod_mun = código de 2 dígitos da UF)
    _controle/<UF>.json assinatura dos arquivos já processados (incremental)
    resumo.json         data da atualização e anos preliminares por UF
"""

import argparse
import ftplib
import json
import os
import re
import socket
import struct
import sys
import tempfile
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd

# ----------------------------------------------------------------------
# CONFIGURAÇÕES
# ----------------------------------------------------------------------
ANO_INICIAL = 2010
ANO_FINAL = 2024
FTP_HOST = "ftp.datasus.gov.br"
PASTA_DADOS = "dados"
PASTA_SAIDA = "saida"

FTP_TIMEOUT = 180          # segundos
FTP_TENTATIVAS = 5         # tentativas por operação
FTP_ESPERA_INICIAL = 5     # segundos (dobra a cada nova tentativa)
BLOCO_REGISTROS = 400_000  # registros lidos por vez do DBF

# True  -> valores vazios/inválidos de sexo, raça/cor e escolaridade viram
#          a categoria "Ignorado" (as contagens fecham com o total).
# False -> esses registros são simplesmente descartados dessas variáveis.
INCLUIR_IGNORADOS = True

# código IBGE da UF -> sigla
UF_SIGLAS = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP",
    "17": "TO", "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB",
    "26": "PE", "27": "AL", "28": "SE", "29": "BA", "31": "MG", "32": "ES",
    "33": "RJ", "35": "SP", "41": "PR", "42": "SC", "43": "RS", "50": "MS",
    "51": "MT", "52": "GO", "53": "DF",
}
SIGLA_PARA_COD = {sigla: cod for cod, sigla in UF_SIGLAS.items()}

# ----------------------------------------------------------------------
# DEFINIÇÃO DAS BASES (somente SINASC ativo nesta versão)
# ----------------------------------------------------------------------
SINASC_PASTAS = [
    # (pasta no FTP, é preliminar?)  -> ordem de busca
    ("/dissemin/publicos/SINASC/NOV/DNRES", False),
    ("/dissemin/publicos/SINASC/1996_/Dados/DNRES", False),
    ("/dissemin/publicos/SINASC/PRELIM/DNRES", True),
]

FAIXAS_IDADE_MAE = ["Menos de 15", "15 a 19", "20 a 24", "25 a 29",
                    "30 a 34", "35 a 39", "40 ou mais"]
MAP_SEXO = {"1": "Masculino", "M": "Masculino", "2": "Feminino", "F": "Feminino"}
MAP_RACA = {"1": "Branca", "2": "Preta", "3": "Amarela", "4": "Parda", "5": "Indígena"}
MAP_ESC = {"1": "Nenhuma", "2": "1 a 3 anos", "3": "4 a 7 anos",
           "4": "8 a 11 anos", "5": "12 anos ou mais"}

BASES = {
    "sinasc": {
        "pastas": SINASC_PASTAS,
        "nome_arquivo": lambda uf, ano: f"DN{uf}{ano}.DBC",
        "colunas": ["CODMUNRES", "SEXO", "RACACOR", "IDADEMAE", "ESCMAE"],
    },
}


def log(msg):
    print(msg, flush=True)


# ----------------------------------------------------------------------
# PARQUET (funções isoladas para facilitar manutenção)
# ----------------------------------------------------------------------
def salvar_parquet(df, caminho):
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    tmp = caminho + ".tmp"
    df.to_parquet(tmp, index=False, engine="pyarrow", compression="zstd")
    os.replace(tmp, caminho)


def ler_parquet(caminho, colunas=None):
    return pd.read_parquet(caminho, columns=colunas, engine="pyarrow")


# ----------------------------------------------------------------------
# FTP
# ----------------------------------------------------------------------
def _linha_para_item(linha):
    """Interpreta uma linha de LIST (estilo Windows/IIS ou Unix).
    Retorna (nome, tamanho) ou None. Pastas retornam tamanho None."""
    partes = linha.split(None, 3)
    if len(partes) == 4 and re.match(r"\d\d-\d\d-\d\d", partes[0]):
        if partes[2].upper() == "<DIR>":
            return partes[3].strip(), None
        try:
            return partes[3].strip(), int(partes[2].replace(",", ""))
        except ValueError:
            return None
    p = linha.split(None, 8)
    if len(p) == 9:
        try:
            tam = None if p[0].startswith("d") else int(p[4])
        except ValueError:
            return None
        return p[8].strip(), tam
    return None


class FtpDatasus:
    """Conexão FTP com reconexão automática e cache de listagens."""

    def __init__(self, host=FTP_HOST):
        self.host = host
        self.ftp = None
        self.cache = {}

    def conectar(self):
        self.fechar()
        ftp = ftplib.FTP(timeout=FTP_TIMEOUT)
        ftp.connect(self.host, 21)
        ftp.login()  # anônimo
        ftp.set_pasv(True)
        self.ftp = ftp

    def fechar(self):
        if self.ftp is not None:
            try:
                self.ftp.quit()
            except Exception:
                try:
                    self.ftp.close()
                except Exception:
                    pass
        self.ftp = None

    def _com_tentativas(self, funcao, descricao):
        espera = FTP_ESPERA_INICIAL
        ultimo_erro = None
        for tentativa in range(1, FTP_TENTATIVAS + 1):
            try:
                if self.ftp is None:
                    self.conectar()
                return funcao()
            except ftplib.error_perm:
                raise  # erro "definitivo" (ex.: pasta/arquivo inexistente)
            except (ftplib.all_errors + (socket.timeout, OSError, EOFError)) as e:
                ultimo_erro = e
                log(f"  ! {descricao}: falha ({e}); tentativa {tentativa}/{FTP_TENTATIVAS}")
                self.fechar()
                if tentativa < FTP_TENTATIVAS:
                    time.sleep(espera)
                    espera *= 2
        raise RuntimeError(f"{descricao} falhou após {FTP_TENTATIVAS} tentativas: {ultimo_erro}")

    def listar(self, pasta):
        """Retorna {NOME_EM_MAIUSCULAS: (nome_real, tamanho)}; {} se a pasta não existe."""
        if pasta in self.cache:
            return self.cache[pasta]

        def _listar():
            itens = {}
            try:
                for nome, fatos in self.ftp.mlsd(pasta, facts=["size", "type"]):
                    if fatos.get("type", "file") == "file":
                        itens[nome.upper()] = (nome, int(fatos.get("size", 0)))
                return itens
            except (ftplib.error_perm, ftplib.error_reply, AttributeError):
                pass  # servidor sem MLSD -> usa LIST
            linhas = []
            self.ftp.retrlines("LIST " + pasta, linhas.append)
            for linha in linhas:
                item = _linha_para_item(linha)
                if item and item[1] is not None:
                    itens[item[0].upper()] = item
            return itens

        try:
            resultado = self._com_tentativas(_listar, f"listar {pasta}")
        except ftplib.error_perm:
            resultado = {}
        self.cache[pasta] = resultado
        return resultado

    def baixar(self, pasta, nome, destino):
        def _baixar():
            with open(destino, "wb") as f:
                self.ftp.retrbinary(f"RETR {pasta}/{nome}", f.write, blocksize=64 * 1024)

        self._com_tentativas(_baixar, f"baixar {nome}")


# ----------------------------------------------------------------------
# LEITOR DBF PRÓPRIO
# ----------------------------------------------------------------------
def ler_cabecalho_dbf(caminho):
    """Retorna (n_registros, tam_cabecalho, tam_registro, campos)
    onde campos = {NOME: (deslocamento, tamanho)} dentro do registro."""
    with open(caminho, "rb") as f:
        cab = f.read(32)
        n_reg = struct.unpack("<I", cab[4:8])[0]
        tam_cab = struct.unpack("<H", cab[8:10])[0]
        tam_reg = struct.unpack("<H", cab[10:12])[0]
        campos = {}
        deslocamento = 1  # byte 0 do registro = marca de apagado
        while True:
            desc = f.read(32)
            if len(desc) < 32 or desc[0] == 0x0D:
                break
            nome = desc[0:11].split(b"\x00")[0].decode("ascii", "ignore").strip().upper()
            tamanho = desc[16]
            campos[nome] = (deslocamento, tamanho)
            deslocamento += tamanho
    return n_reg, tam_cab, tam_reg, campos


def ler_dbf_em_blocos(caminho, colunas, bloco=BLOCO_REGISTROS):
    """Gera dicionários {coluna: np.array de texto} por bloco de registros.
    Registros apagados são descartados; colunas inexistentes vêm vazias."""
    n_reg, tam_cab, tam_reg, campos = ler_cabecalho_dbf(caminho)
    # protege contra cabeçalho que declara mais registros do que o arquivo tem
    disponivel = (os.path.getsize(caminho) - tam_cab) // tam_reg
    n_reg = max(0, min(n_reg, disponivel))
    if n_reg == 0:
        return
    mm = np.memmap(caminho, dtype=np.uint8, mode="r", offset=tam_cab, shape=(n_reg, tam_reg))
    try:
        for ini in range(0, n_reg, bloco):
            fim = min(ini + bloco, n_reg)
            dados = mm[ini:fim]
            validos = dados[:, 0] != ord("*")
            if not validos.any():
                continue
            saida = {}
            for col in colunas:
                if col not in campos:
                    saida[col] = np.full(int(validos.sum()), "", dtype="U1")
                    continue
                desloc, tam = campos[col]
                fatia = np.ascontiguousarray(dados[:, desloc:desloc + tam])
                brutos = fatia.view(f"S{tam}").ravel()[validos]
                brutos = np.char.strip(brutos, b" \x00")
                saida[col] = np.char.decode(brutos, "latin-1")
            yield saida
    finally:
        del mm


# ----------------------------------------------------------------------
# AGREGAÇÃO SINASC
# ----------------------------------------------------------------------
def normalizar_municipio(valores, uf_cod):
    """Código de 6 dígitos; inválido (ou de outra UF) -> '<UF>0000'."""
    s = pd.Series(valores, dtype="object").astype(str).str.strip()
    s = s.where(s.str.len() != 7, s.str[:6])
    valido = s.str.fullmatch(r"\d{6}") & (s.str[:2] == uf_cod)
    return np.where(valido, s, f"{uf_cod}0000")


def _contar(mun, variavel, categorias):
    """Conta registros por (município, categoria). Categoria nula é descartada."""
    df = pd.DataFrame({"cod_mun": mun, "categoria": categorias})
    df = df.dropna(subset=["categoria"])
    if df.empty:
        return pd.DataFrame(columns=["cod_mun", "variavel", "categoria", "n"])
    r = df.groupby(["cod_mun", "categoria"], observed=True).size().reset_index(name="n")
    r["variavel"] = variavel
    r["categoria"] = r["categoria"].astype(str)
    return r[["cod_mun", "variavel", "categoria", "n"]]


def _mapear(serie_txt, mapa):
    cat = serie_txt.map(mapa)
    return cat.fillna("Ignorado") if INCLUIR_IGNORADOS else cat


def agregar_bloco_sinasc(dados, uf_cod):
    mun = normalizar_municipio(dados["CODMUNRES"], uf_cod)
    n = len(mun)
    txt = {c: pd.Series(v, dtype="object") for c, v in dados.items()}

    partes = [_contar(mun, "total", pd.Series(["Total"] * n))]
    partes.append(_contar(mun, "sexo", _mapear(txt["SEXO"].str.upper(), MAP_SEXO)))
    partes.append(_contar(mun, "raca_cor", _mapear(txt["RACACOR"], MAP_RACA)))

    idade = pd.to_numeric(txt["IDADEMAE"], errors="coerce")
    faixa = pd.cut(idade, bins=[9, 14, 19, 24, 29, 34, 39, 60],
                   labels=FAIXAS_IDADE_MAE).astype(object)  # fora de 10-60 -> nulo
    partes.append(_contar(mun, "faixa_idade_mae", faixa))

    partes.append(_contar(mun, "escolaridade_mae", _mapear(txt["ESCMAE"], MAP_ESC)))
    return pd.concat(partes, ignore_index=True)


def agregar_dbf_sinasc(caminho_dbf, uf_cod, ano):
    """Lê o DBF inteiro (em blocos) e devolve o DataFrame final do município."""
    parciais = []
    for dados in ler_dbf_em_blocos(caminho_dbf, BASES["sinasc"]["colunas"]):
        parciais.append(agregar_bloco_sinasc(dados, uf_cod))
    cols = ["ano", "cod_mun", "variavel", "categoria", "n"]
    if not parciais:
        return pd.DataFrame(columns=cols)
    df = pd.concat(parciais, ignore_index=True)
    df = df.groupby(["cod_mun", "variavel", "categoria"], as_index=False)["n"].sum()
    df["ano"] = np.int16(ano)
    df["n"] = df["n"].astype("int32")
    return df[cols].sort_values(["variavel", "categoria", "cod_mun"]).reset_index(drop=True)


def agregar_por_uf(df_mun, uf_cod):
    cols = ["ano", "cod_mun", "variavel", "categoria", "n"]
    if df_mun.empty:
        return df_mun[cols].copy()
    df = df_mun.groupby(["ano", "variavel", "categoria"], as_index=False)["n"].sum()
    df["cod_mun"] = uf_cod
    df["n"] = df["n"].astype("int32")
    return df[cols]


# ----------------------------------------------------------------------
# CONTROLE INCREMENTAL
# ----------------------------------------------------------------------
def caminho_controle(base, uf):
    return os.path.join(PASTA_DADOS, base, "_controle", f"{uf}.json")


def ler_controle(base, uf):
    try:
        with open(caminho_controle(base, uf), encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def salvar_controle(base, uf, controle):
    c = caminho_controle(base, uf)
    os.makedirs(os.path.dirname(c), exist_ok=True)
    with open(c, "w", encoding="utf-8") as f:
        json.dump(controle, f, ensure_ascii=False, indent=1, sort_keys=True)


def anos_no_consolidado(base, uf):
    """Anos já presentes em dados/<base>/<UF>.parquet (vazio se não existe)."""
    c = os.path.join(PASTA_DADOS, base, f"{uf}.parquet")
    if not os.path.exists(c):
        return set()
    try:
        return set(int(a) for a in ler_parquet(c, ["ano"])["ano"].unique())
    except Exception:
        return set()


# ----------------------------------------------------------------------
# COLETA DE UM ESTADO
# ----------------------------------------------------------------------
def localizar_arquivo(ftp, cfg, uf, ano):
    """Procura o arquivo nas pastas, na ordem. Retorna (pasta, nome, tamanho, prelim) ou None."""
    alvo = cfg["nome_arquivo"](uf, ano).upper()
    for pasta, prelim in cfg["pastas"]:
        item = ftp.listar(pasta).get(alvo)
        if item:
            return pasta, item[0], item[1], prelim
    return None


def coletar_uf(base, uf, ftp=None):
    if base not in BASES:
        sys.exit(f"Base '{base}' ainda não está ativada nesta versão do coletar.py.")
    uf = uf.upper()
    if uf not in SIGLA_PARA_COD:
        sys.exit(f"UF inválida: {uf}. Use uma sigla como RJ, SP, MG...")
    cfg = BASES[base]
    uf_cod = SIGLA_PARA_COD[uf]
    ftp = ftp or FtpDatasus()
    controle = ler_controle(base, uf)
    ja_consolidados = anos_no_consolidado(base, uf)
    dir_saida = os.path.join(PASTA_SAIDA, base)
    resumo = {"novos": 0, "pulados": 0, "ausentes": 0}

    log(f"== {base.upper()} / {uf} ({ANO_INICIAL}-{ANO_FINAL}) ==")
    for ano in range(ANO_INICIAL, ANO_FINAL + 1):
        achou = localizar_arquivo(ftp, cfg, uf, ano)
        if not achou:
            log(f"  {ano}: arquivo não encontrado no FTP (ignorado)")
            resumo["ausentes"] += 1
            continue
        pasta, nome, tamanho, prelim = achou
        assinatura = f"{pasta}/{nome}:{tamanho}"
        sufixo = "_prelim" if prelim else ""
        arq_saida = os.path.join(dir_saida, f"{uf}_{ano}{sufixo}.parquet")
        arq_uf = os.path.join(dir_saida, "_uf", f"{uf}_{ano}{sufixo}.parquet")

        inalterado = controle.get(str(ano)) == assinatura
        if inalterado and (os.path.exists(arq_saida) or ano in ja_consolidados):
            log(f"  {ano}: sem mudanças (pulado)")
            resumo["pulados"] += 1
            continue

        log(f"  {ano}: baixando {nome} ({tamanho/1e6:.1f} MB)" + (" [preliminar]" if prelim else ""))
        with tempfile.TemporaryDirectory() as tmp:
            dbc = os.path.join(tmp, nome)
            dbf = os.path.join(tmp, "dados.dbf")
            ftp.baixar(pasta, nome, dbc)
            from pyreaddbc import dbc2dbf  # import tardio: só é preciso aqui
            dbc2dbf(dbc, dbf)
            df = agregar_dbf_sinasc(dbf, uf_cod, ano)

        salvar_parquet(df, arq_saida)
        salvar_parquet(agregar_por_uf(df, uf_cod), arq_uf)
        # se saiu a versão final, apaga a preliminar antiga (e vice-versa)
        outro = "" if prelim else "_prelim"
        for d in (dir_saida, os.path.join(dir_saida, "_uf")):
            velho = os.path.join(d, f"{uf}_{ano}{outro}.parquet")
            if os.path.exists(velho):
                os.remove(velho)

        controle[str(ano)] = assinatura
        salvar_controle(base, uf, controle)  # salva a cada ano: progresso não se perde
        log(f"  {ano}: ok ({len(df):,} linhas)")
        resumo["novos"] += 1

    ftp.fechar()
    log(f"Resumo {uf}: {resumo['novos']} novos, {resumo['pulados']} pulados, "
        f"{resumo['ausentes']} ausentes")
    return resumo


# ----------------------------------------------------------------------
# CONSOLIDAÇÃO
# ----------------------------------------------------------------------
def _arquivos_saida(pasta):
    """{(UF, ano): caminho}; se houver final e preliminar do mesmo ano, vale a final."""
    achados = {}
    if not os.path.isdir(pasta):
        return achados
    for nome in sorted(os.listdir(pasta)):
        m = re.fullmatch(r"([A-Z]{2})_(\d{4})(_prelim)?\.parquet", nome)
        if not m:
            continue
        chave = (m.group(1), int(m.group(2)))
        if m.group(3) and chave in achados:
            continue
        achados[chave] = os.path.join(pasta, nome)
    # garante que a final prevalece mesmo se listada depois
    for nome in os.listdir(pasta):
        m = re.fullmatch(r"([A-Z]{2})_(\d{4})\.parquet", nome)
        if m:
            achados[(m.group(1), int(m.group(2)))] = os.path.join(pasta, nome)
    return achados


def _juntar(existente, novos, anos_novos):
    """Remove de 'existente' os anos reescritos e acrescenta 'novos'."""
    if existente is not None and len(existente):
        existente = existente[~existente["ano"].isin(anos_novos)]
        df = pd.concat([existente, novos], ignore_index=True)
    else:
        df = novos
    df["ano"] = df["ano"].astype("int16")
    df["n"] = df["n"].astype("int32")
    return df.sort_values(["variavel", "categoria", "ano", "cod_mun"]).reset_index(drop=True)


def consolidar(base=None):
    bases = [base] if base else list(BASES)
    for b in bases:
        log(f"== Consolidando {b.upper()} ==")
        dir_saida = os.path.join(PASTA_SAIDA, b)
        dir_dados = os.path.join(PASTA_DADOS, b)
        os.makedirs(dir_dados, exist_ok=True)

        # municípios: um parquet por UF
        por_uf = {}
        for (uf, ano), caminho in _arquivos_saida(dir_saida).items():
            por_uf.setdefault(uf, []).append((ano, caminho))
        for uf, itens in sorted(por_uf.items()):
            novos = pd.concat([ler_parquet(c) for _, c in sorted(itens)], ignore_index=True)
            alvo = os.path.join(dir_dados, f"{uf}.parquet")
            existente = ler_parquet(alvo) if os.path.exists(alvo) else None
            df = _juntar(existente, novos, {a for a, _ in itens})
            salvar_parquet(df, alvo)
            log(f"  {uf}: {len(df):,} linhas")

        # Brasil por UF: um único parquet
        itens_uf = _arquivos_saida(os.path.join(dir_saida, "_uf"))
        if itens_uf:
            novos = pd.concat([ler_parquet(c) for _, c in sorted(itens_uf.items())],
                              ignore_index=True)
            alvo = os.path.join(dir_dados, "_brasil.parquet")
            existente = ler_parquet(alvo) if os.path.exists(alvo) else None
            if existente is not None and len(existente):
                # remove só as combinações (UF, ano) reescritas
                chaves_novas = set(zip(novos["cod_mun"], novos["ano"].astype(int)))
                manter = [(c, int(a)) not in chaves_novas
                          for c, a in zip(existente["cod_mun"], existente["ano"])]
                existente = existente[np.array(manter, dtype=bool)]
                df = pd.concat([existente, novos], ignore_index=True)
            else:
                df = novos
            df["ano"] = df["ano"].astype("int16")
            df["n"] = df["n"].astype("int32")
            df = df.sort_values(["variavel", "categoria", "ano", "cod_mun"]).reset_index(drop=True)
            salvar_parquet(df, alvo)
            log(f"  _brasil: {len(df):,} linhas")

        # resumo.json (data da atualização e anos preliminares)
        prelim = {}
        pasta_ctrl = os.path.join(dir_dados, "_controle")
        if os.path.isdir(pasta_ctrl):
            for nome in sorted(os.listdir(pasta_ctrl)):
                if nome.endswith(".json"):
                    uf = nome[:-5]
                    ctrl = ler_controle(b, uf)
                    anos = sorted(int(a) for a, s in ctrl.items() if "/PRELIM/" in s.upper())
                    if anos:
                        prelim[uf] = anos
        with open(os.path.join(dir_dados, "resumo.json"), "w", encoding="utf-8") as f:
            json.dump({"atualizado_em": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                       "anos_preliminares": prelim}, f, ensure_ascii=False, indent=1)
    log("Consolidação concluída.")


# ----------------------------------------------------------------------
# LINHA DE COMANDO
# ----------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Robô de coleta de dados do DATASUS")
    ap.add_argument("--base", default="sinasc", help="base a coletar (ativada: sinasc)")
    ap.add_argument("--uf", help="sigla do estado, ex.: RJ")
    ap.add_argument("--consolidar", action="store_true",
                    help="junta os arquivos de saida/ em dados/")
    args = ap.parse_args()

    if not args.uf and not args.consolidar:
        ap.error("informe --uf SIGLA e/ou --consolidar")
    if args.uf:
        coletar_uf(args.base.lower(), args.uf)
    if args.consolidar:
        consolidar(args.base.lower() if args.uf else None)


if __name__ == "__main__":
    main()

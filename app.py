from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

import gspread
import numpy as np
import pandas as pd
import streamlit as st
from gspread import Worksheet
from sklearn.linear_model import LinearRegression
import unicodedata
import re


# ============================================================
# CONFIGURAÇÃO GERAL
# ============================================================
SPREADSHEET_ID = "1KuD14Kcma6Ze5nIxPT_MHIECu03kcN9tAL_clGPCKcU"

SHEET_PATIENTS = "Pacientes"
SHEET_TREINO_A = "Treino A"
SHEET_TREINO_B = "Treino B"
SHEET_TREINO_C = "Treino C"
SHEET_HISTORICO = "Historico"

APP_TITLE = "🏋️ App de Treino"
APP_CAPTION = "Registro de treino com Google Sheets + análises com sklearn"

TIPO_LINHA_TREINO = "TREINO"
TIPO_LINHA_FICHA = "FICHA_USUARIO"
TIPO_LINHA_CALORIA = "CALORIAS_TREINO"

TIMESTAMP_FMT_BR = "%d/%m/%Y %H:%M:%S"
TIMESTAMP_FMT_ISO = "%Y-%m-%d %H:%M:%S"

HIST_HEADERS = [
    "Data",
    "CPF",
    "Semana",
    "Treino",
    "Exercicio",
    "Grupo",
    "Peso",
    "Reps",
    "Series",
    "Volume",
    "1RM",
    "Observacao",
]

MAP_TREINOS = {
    "Treino A": SHEET_TREINO_A,
    "Treino B": SHEET_TREINO_B,
    "Treino C": SHEET_TREINO_C,
}

MAP_GRUPOS = {
    # PEITO
    "supino": "Peito",
    "supino reto": "Peito",
    "supino inclinado": "Peito",
    "supino declinado": "Peito",
    "supino halteres": "Peito",
    "supino barra": "Peito",
    "supino máquina": "Peito",
    "supino maquina": "Peito",
    "chest press": "Peito",
    "crucifixo": "Peito",
    "crucifixo reto": "Peito",
    "crucifixo inclinado": "Peito",
    "crucifixo declinado": "Peito",
    "crucifixo máquina": "Peito",
    "crucifixo maquina": "Peito",
    "peck deck": "Peito",
    "voador": "Peito",
    "flexão": "Peito",
    "flexao": "Peito",
    "cross over": "Peito",
    "crossover": "Peito",
    "cabo alto": "Peito",
    "cabo baixo": "Peito",
    # COSTAS
    "barra fixa": "Costas",
    "barra": "Costas",
    "puxada": "Costas",
    "puxada frente": "Costas",
    "puxada atrás": "Costas",
    "puxada atras": "Costas",
    "puxada alta": "Costas",
    "pulldown": "Costas",
    "remada": "Costas",
    "remada curvada": "Costas",
    "remada baixa": "Costas",
    "remada cavalinho": "Costas",
    "remada máquina": "Costas",
    "remada maquina": "Costas",
    "remada unilateral": "Costas",
    "serrote": "Costas",
    "pullover": "Costas",
    "rack pull": "Costas",
    # OMBRO
    "elevação lateral": "Ombro",
    "elevacao lateral": "Ombro",
    "elevação frontal": "Ombro",
    "elevacao frontal": "Ombro",
    "desenvolvimento": "Ombro",
    "desenvolvimento halteres": "Ombro",
    "desenvolvimento barra": "Ombro",
    "desenvolvimento máquina": "Ombro",
    "desenvolvimento maquina": "Ombro",
    "arnold press": "Ombro",
    "crucifixo invertido": "Ombro",
    "face pull": "Ombro",
    "remada alta": "Ombro",
    "elevação lateral polia": "Ombro",
    "elevacao lateral polia": "Ombro",
    # BÍCEPS
    "rosca direta": "Bíceps",
    "rosca barra": "Bíceps",
    "rosca halteres": "Bíceps",
    "rosca alternada": "Bíceps",
    "rosca concentrada": "Bíceps",
    "rosca scott": "Bíceps",
    "rosca scott máquina": "Bíceps",
    "rosca scott maquina": "Bíceps",
    "rosca martelo": "Bíceps",
    "rosca 21": "Bíceps",
    "rosca inversa": "Bíceps",
    "curl": "Bíceps",
    # TRÍCEPS
    "tríceps testa": "Tríceps",
    "triceps testa": "Tríceps",
    "tríceps corda": "Tríceps",
    "triceps corda": "Tríceps",
    "tríceps barra": "Tríceps",
    "triceps barra": "Tríceps",
    "tríceps francês": "Tríceps",
    "triceps frances": "Tríceps",
    "tríceps banco": "Tríceps",
    "triceps banco": "Tríceps",
    "mergulho": "Tríceps",
    "paralela": "Tríceps",
    "coice tríceps": "Tríceps",
    "coice triceps": "Tríceps",
    # QUADRÍCEPS
    "agachamento": "Quadríceps",
    "agachamento livre": "Quadríceps",
    "agachamento smith": "Quadríceps",
    "agachamento frontal": "Quadríceps",
    "leg press": "Quadríceps",
    "leg": "Quadríceps",
    "cadeira extensora": "Quadríceps",
    "hack": "Quadríceps",
    "hack machine": "Quadríceps",
    "passada": "Quadríceps",
    "lunge": "Quadríceps",
    "afundo": "Quadríceps",
    "bulgarian": "Quadríceps",
    "agachamento búlgaro": "Quadríceps",
    "agachamento bulgaro": "Quadríceps",
    "sissy squat": "Quadríceps",
    # POSTERIOR
    "stiff": "Posterior",
    "peso morto": "Posterior",
    "deadlift": "Posterior",
    "mesa flexora": "Posterior",
    "flexora": "Posterior",
    "flexora em pé": "Posterior",
    "flexora em pe": "Posterior",
    "flexora sentado": "Posterior",
    "good morning": "Posterior",
    "hip hinge": "Posterior",
    "rdl": "Posterior",
    # GLÚTEO
    "hip thrust": "Glúteo",
    "glute bridge": "Glúteo",
    "abdução": "Glúteo",
    "abducao": "Glúteo",
    "glute kickback": "Glúteo",
    "coice": "Glúteo",
    "elevação pélvica": "Glúteo",
    "elevacao pelvica": "Glúteo",
    "sumô": "Glúteo",
    "sumo": "Glúteo",
    # PANTURRILHA
    "panturrilha": "Panturrilha",
    "panturrilha em pé": "Panturrilha",
    "panturrilha em pe": "Panturrilha",
    "panturrilha sentado": "Panturrilha",
    "panturrilha leg press": "Panturrilha",
    "panturrilha máquina": "Panturrilha",
    "panturrilha maquina": "Panturrilha",
    # ABDÔMEN
    "abdominal": "Abdômen",
    "abdominal infra": "Abdômen",
    "abdominal supra": "Abdômen",
    "abdominal máquina": "Abdômen",
    "abdominal maquina": "Abdômen",
    "ab wheel": "Abdômen",
    "prancha": "Abdômen",
    "crunch": "Abdômen",
    "leg raise": "Abdômen",
    "elevação de pernas": "Abdômen",
    "elevacao de pernas": "Abdômen",
}

POSSIVEIS_COLUNAS_EXERCICIO = ["Exercicio", "Exercício", "Nome", "Exercicio "]
POSSIVEIS_COLUNAS_SERIES = ["Series", "Séries", "Serie", "Série"]
POSSIVEIS_COLUNAS_REPS = ["Reps", "Repeticoes", "Repetições", "Repeticao", "Repetição"]
POSSIVEIS_COLUNAS_GRUPO = ["Grupo", "Grupo Muscular", "Músculo", "Musculo"]
POSSIVEIS_COLUNAS_NOME = ["Nome", "Paciente", "Aluno"]
POSSIVEIS_COLUNAS_TREINO = ["Treino", "Ficha"]
POSSIVEIS_COLUNAS_PESO = ["Peso", "Carga"]
POSSIVEIS_COLUNAS_DATA = ["Data", "DataHora", "Data/Hora"]
POSSIVEIS_COLUNAS_OBS = ["Observacao", "Observação", "Obs"]
POSSIVEIS_COLUNAS_CPF = ["CPF", "cpf"]
POSSIVEIS_COLUNAS_SEMANA = ["Semana", "semana"]
POSSIVEIS_COLUNAS_1RM = ["1RM", "1rm"]
POSSIVEIS_COLUNAS_VOLUME = ["Volume", "volume"]

SESSION_PREFIX_FICHA = "ficha_edit"
SESSION_CPF_ATUAL = "cpf_atual"
SESSION_TREINO_ATUAL = "treino_atual"
SESSION_PAGINA_ATUAL = "pagina_atual"

NUMERIC_STEP_PESO = 0.5
MAX_REPS_VALIDACAO = 100
MAX_SERIES_VALIDACAO = 50
MAX_PESO_VALIDACAO = 5000.0

st.set_page_config(page_title="Treinos", page_icon="🏋️", layout="wide")


# ============================================================
# FUNÇÕES DE TEXTO / NORMALIZAÇÃO
# ============================================================
def strip_accents(texto: str) -> str:
    """
    Remove acentos para facilitar comparação estável.
    """
    if texto is None:
        return ""
    texto = str(texto)
    return "".join(
        ch
        for ch in unicodedata.normalize("NFD", texto)
        if unicodedata.category(ch) != "Mn"
    )


def normalizar_texto(txt: str) -> str:
    """
    Normaliza texto para comparação.
    """
    txt = "" if txt is None else str(txt)
    txt = strip_accents(txt)
    txt = txt.strip().lower()
    txt = re.sub(r"\s+", " ", txt)
    return txt


def texto_vazio(valor: Any) -> bool:
    """
    Verifica vazio textual.
    """
    if valor is None:
        return True
    return str(valor).strip() == ""


def normalizar_nome_exercicio(exercicio: str) -> str:
    """
    Nome canônico do exercício.
    """
    return normalizar_texto(exercicio)


def label_exercicio(exercicio: str) -> str:
    """
    Mantém uma label amigável.
    """
    return str(exercicio).strip()


def exercicio_ficha_token(exercicio: str) -> str:
    """
    Token usado ao salvar ficha dentro do histórico.
    """
    return f"FICHA::{label_exercicio(exercicio)}"


def linha_observacao_tipo(tipo: str, texto_extra: str = "") -> str:
    """
    Embute tipo lógico na observação sem alterar o schema.
    """
    base = f"[TIPO={tipo}]"
    extra = str(texto_extra).strip()
    if extra:
        return f"{base} {extra}"
    return base


def extrair_tipo_da_observacao(obs: Any) -> str:
    """
    Extrai tipo embutido na observação.
    """
    texto = "" if obs is None else str(obs)
    match = re.search(r"\[TIPO=([A-Z_]+)\]", texto)
    if match:
        return match.group(1).strip()
    return ""


def remover_marcador_tipo(obs: Any) -> str:
    """
    Remove prefixo [TIPO=...] da observação para exibição.
    """
    texto = "" if obs is None else str(obs)
    texto = re.sub(r"\[TIPO=[A-Z_]+\]\s*", "", texto).strip()
    return texto


# ============================================================
# FUNÇÕES DE DATAS / PARSING
# ============================================================
def agora_timestamp_iso() -> str:
    """
    Timestamp estável.
    """
    return datetime.now().strftime(TIMESTAMP_FMT_ISO)


def agora_timestamp_br() -> str:
    """
    Timestamp BR apenas para eventual exibição.
    """
    return datetime.now().strftime(TIMESTAMP_FMT_BR)


def formatar_data_br(dt: Optional[pd.Timestamp]) -> str:
    """
    Formata data para visualização.
    """
    if dt is None or pd.isna(dt):
        return "-"
    try:
        return pd.Timestamp(dt).strftime(TIMESTAMP_FMT_BR)
    except Exception:
        return "-"


def parse_datetime_value(valor: Any) -> pd.Timestamp | pd.NaT:
    """
    Faz parsing robusto aceitando ISO, BR e valores já datetime.
    """
    if valor is None:
        return pd.NaT

    if isinstance(valor, pd.Timestamp):
        return valor

    texto = str(valor).strip()
    if not texto:
        return pd.NaT

    formatos_prioritarios = [
        TIMESTAMP_FMT_ISO,
        TIMESTAMP_FMT_BR,
        "%d/%m/%Y",
        "%Y-%m-%d",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
    ]

    for fmt in formatos_prioritarios:
        try:
            return pd.Timestamp(datetime.strptime(texto, fmt))
        except Exception:
            pass

    try:
        return pd.to_datetime(texto, errors="coerce", format="mixed", dayfirst=True)
    except Exception:
        try:
            return pd.to_datetime(texto, errors="coerce", dayfirst=True)
        except Exception:
            return pd.NaT


def parse_datetime_series(serie: pd.Series) -> pd.Series:
    """
    Faz parsing robusto em série.
    """
    if serie.empty:
        return serie
    return serie.apply(parse_datetime_value)


def iso_week_from_timestamp(ts: pd.Timestamp | datetime | None) -> int:
    """
    Semana ISO.
    """
    if ts is None or pd.isna(ts):
        return int(datetime.now().isocalendar()[1])
    ts = pd.Timestamp(ts)
    return int(ts.isocalendar().week)


def periodo_semana_texto(ts: pd.Series) -> pd.Series:
    """
    Converte para período semanal estável.
    """
    return ts.dt.to_period("W").astype(str)


# ============================================================
# NUMÉRICOS / PARSING ROBUSTO
# ============================================================
def limpar_string_numerica(valor: Any) -> str:
    """
    Trata formatos como:
    - 1000
    - 1.000
    - 1,000
    - 1.000,50
    - 1,000.50
    """
    if valor is None:
        return ""

    txt = str(valor).strip()
    if txt == "":
        return ""

    txt = txt.replace(" ", "")

    if "." in txt and "," in txt:
        # Decide pelo separador decimal mais à direita
        if txt.rfind(",") > txt.rfind("."):
            txt = txt.replace(".", "")
            txt = txt.replace(",", ".")
        else:
            txt = txt.replace(",", "")
    else:
        if txt.count(",") == 1 and txt.count(".") == 0:
            txt = txt.replace(",", ".")
        elif txt.count(".") > 1 and txt.count(",") == 0:
            txt = txt.replace(".", "")
        elif txt.count(",") > 1 and txt.count(".") == 0:
            txt = txt.replace(",", "")
        else:
            # casos já aceitáveis
            pass

    txt = re.sub(r"[^0-9\.\-]", "", txt)
    return txt


def to_float(valor: Any, default: float = 0.0) -> float:
    """
    Converte para float com tolerância.
    """
    txt = limpar_string_numerica(valor)
    if txt == "":
        return float(default)
    try:
        return float(txt)
    except Exception:
        return float(default)


def to_int(valor: Any, default: int = 0) -> int:
    """
    Converte para inteiro via arredondamento.
    """
    try:
        return int(round(to_float(valor, default)))
    except Exception:
        return int(default)


def clamp_float(valor: float, minimo: float, maximo: float) -> float:
    """
    Limita float.
    """
    return float(max(minimo, min(maximo, valor)))


def clamp_int(valor: int, minimo: int, maximo: int) -> int:
    """
    Limita inteiro.
    """
    return int(max(minimo, min(maximo, valor)))


# ============================================================
# GOOGLE SHEETS
# ============================================================
@st.cache_resource
def get_gspread_client():
    """
    Cliente do Google Sheets.
    """
    if "gcp_service_account" in st.secrets:
        return gspread.service_account_from_dict(dict(st.secrets["gcp_service_account"]))
    return gspread.service_account(filename="credenciais.json")


@st.cache_resource
def get_spreadsheet():
    """
    Abre a planilha principal.
    """
    client = get_gspread_client()
    return client.open_by_key(SPREADSHEET_ID)


def get_worksheet(sheet_name: str) -> Worksheet:
    """
    Retorna worksheet pelo nome.
    """
    return get_spreadsheet().worksheet(sheet_name)


@st.cache_data(ttl=30, show_spinner=False)
def read_sheet(sheet_name: str) -> pd.DataFrame:
    """
    Lê aba inteira.
    """
    ws = get_worksheet(sheet_name)
    values = ws.get_all_values()

    if not values:
        return pd.DataFrame()

    headers = values[0]
    rows = values[1:]

    if not headers:
        return pd.DataFrame()

    return pd.DataFrame(rows, columns=headers)


def clear_cached_data() -> None:
    """
    Limpa caches de dados.
    """
    st.cache_data.clear()


# ============================================================
# UTILITÁRIOS DE DATAFRAME
# ============================================================
def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove espaços das colunas.
    """
    if df.empty:
        return df.copy()
    novo = df.copy()
    novo.columns = [str(c).strip() for c in novo.columns]
    return novo


def achar_coluna(df: pd.DataFrame, candidatos: list[str]) -> Optional[str]:
    """
    Tenta achar coluna entre vários aliases.
    """
    if df.empty:
        return None

    cols = [str(c).strip() for c in df.columns]

    for c in candidatos:
        if c in cols:
            return c

    cols_norm = {normalizar_texto(c): c for c in cols}
    for c in candidatos:
        key = normalizar_texto(c)
        if key in cols_norm:
            return cols_norm[key]

    return None


def col_or_default(df: pd.DataFrame, nome: str, valor_padrao="") -> pd.DataFrame:
    """
    Garante coluna.
    """
    novo = df.copy()
    if nome not in novo.columns:
        novo[nome] = valor_padrao
    return novo


def dataframe_vazio_ficha() -> pd.DataFrame:
    """
    Estrutura base de ficha.
    """
    return pd.DataFrame(columns=["Exercicio", "Grupo", "Series", "Reps"])


def dataframe_vazio_historico() -> pd.DataFrame:
    """
    Estrutura base do histórico.
    """
    return pd.DataFrame(columns=HIST_HEADERS)


def ordenar_por_data_segura(df: pd.DataFrame, col_data: str, asc: bool = True) -> pd.DataFrame:
    """
    Ordena dataframe pela data parseada.
    """
    if df.empty or col_data not in df.columns:
        return df.copy()

    novo = df.copy()
    novo["_data_sort"] = parse_datetime_series(novo[col_data])
    novo = novo.sort_values("_data_sort", ascending=asc, kind="stable")
    novo = novo.drop(columns=["_data_sort"])
    novo = novo.reset_index(drop=True)
    return novo


def tem_colunas_minimas(df: pd.DataFrame, colunas: list[str]) -> bool:
    """
    Verifica colunas obrigatórias.
    """
    return all(col in df.columns for col in colunas)


# ============================================================
# REGRAS DE NEGÓCIO - EXERCÍCIOS / GRUPOS
# ============================================================
def inferir_grupo(exercicio: str) -> str:
    """
    Infere grupo muscular a partir do nome.
    """
    ex_norm = normalizar_nome_exercicio(exercicio)

    if ex_norm in {normalizar_nome_exercicio(k): v for k, v in MAP_GRUPOS.items()}:
        mapa_norm = {normalizar_nome_exercicio(k): v for k, v in MAP_GRUPOS.items()}
        return mapa_norm[ex_norm]

    for chave, grupo in MAP_GRUPOS.items():
        chave_norm = normalizar_nome_exercicio(chave)
        if chave_norm in ex_norm or ex_norm in chave_norm:
            return grupo

    return "Outro"


def lista_exercicios_padronizada() -> list[str]:
    """
    Lista ordenada e sem duplicação lógica.
    """
    vistos = set()
    final = []

    for ex in sorted(MAP_GRUPOS.keys(), key=lambda x: normalizar_nome_exercicio(x)):
        token = normalizar_nome_exercicio(ex)
        if token not in vistos:
            vistos.add(token)
            final.append(ex)

    return final


def deduplicar_ficha_por_exercicio(df_ficha: pd.DataFrame) -> pd.DataFrame:
    """
    Remove exercícios duplicados mantendo o primeiro.
    """
    if df_ficha.empty:
        return dataframe_vazio_ficha()

    novo = df_ficha.copy()
    novo["_token_ex"] = novo["Exercicio"].astype(str).apply(normalizar_nome_exercicio)
    novo = novo.drop_duplicates(subset=["_token_ex"], keep="first")
    novo = novo.drop(columns=["_token_ex"])
    novo = novo.reset_index(drop=True)
    return novo


def validar_ficha(df_ficha: pd.DataFrame) -> tuple[bool, list[str], pd.DataFrame]:
    """
    Valida ficha editada.
    """
    erros: list[str] = []

    if df_ficha.empty:
        erros.append("A ficha não pode ficar vazia.")
        return False, erros, dataframe_vazio_ficha()

    novo = df_ficha.copy()

    novo["Exercicio"] = novo["Exercicio"].astype(str).str.strip()
    novo["Grupo"] = novo["Grupo"].astype(str).str.strip()
    novo["Series"] = novo["Series"].apply(lambda x: clamp_int(to_int(x, 0), 0, MAX_SERIES_VALIDACAO))
    novo["Reps"] = novo["Reps"].apply(lambda x: clamp_int(to_int(x, 0), 0, MAX_REPS_VALIDACAO))

    novo = novo[novo["Exercicio"] != ""].reset_index(drop=True)

    if novo.empty:
        erros.append("A ficha ficou vazia após remover linhas inválidas.")
        return False, erros, dataframe_vazio_ficha()

    token_counts = novo["Exercicio"].apply(normalizar_nome_exercicio).value_counts()
    duplicados = token_counts[token_counts > 1].index.tolist()
    if duplicados:
        erros.append("Existem exercícios duplicados na ficha. Cada exercício deve aparecer apenas uma vez.")

    novo["Grupo"] = novo.apply(
        lambda row: row["Grupo"] if str(row["Grupo"]).strip() else inferir_grupo(row["Exercicio"]),
        axis=1,
    )

    ok = len(erros) == 0
    return ok, erros, novo.reset_index(drop=True)


def padronizar_ficha_df(df_ficha: pd.DataFrame) -> pd.DataFrame:
    """
    Converte qualquer layout de ficha para colunas padrão.
    """
    if df_ficha.empty:
        return dataframe_vazio_ficha()

    df = normalizar_colunas(df_ficha.copy())

    col_ex = achar_coluna(df, POSSIVEIS_COLUNAS_EXERCICIO)
    col_gr = achar_coluna(df, POSSIVEIS_COLUNAS_GRUPO)
    col_se = achar_coluna(df, POSSIVEIS_COLUNAS_SERIES)
    col_re = achar_coluna(df, POSSIVEIS_COLUNAS_REPS)

    novo = pd.DataFrame()
    novo["Exercicio"] = df[col_ex].astype(str) if col_ex else ""
    novo["Grupo"] = df[col_gr].astype(str) if col_gr else ""
    novo["Series"] = df[col_se].apply(lambda x: to_int(x, 0)) if col_se else 0
    novo["Reps"] = df[col_re].apply(lambda x: to_int(x, 0)) if col_re else 0

    novo["Exercicio"] = novo["Exercicio"].astype(str).str.strip()
    novo["Grupo"] = novo["Grupo"].astype(str).str.strip()
    novo = novo[novo["Exercicio"] != ""].reset_index(drop=True)

    if novo.empty:
        return dataframe_vazio_ficha()

    novo["Grupo"] = novo["Grupo"].replace("", pd.NA)
    novo["Grupo"] = novo["Grupo"].fillna(novo["Exercicio"].apply(inferir_grupo))
    novo["Series"] = novo["Series"].apply(lambda x: clamp_int(to_int(x, 0), 0, MAX_SERIES_VALIDACAO))
    novo["Reps"] = novo["Reps"].apply(lambda x: clamp_int(to_int(x, 0), 0, MAX_REPS_VALIDACAO))

    novo = deduplicar_ficha_por_exercicio(novo)

    return novo.reset_index(drop=True)


def serializar_ficha(df_ficha: pd.DataFrame) -> list[dict]:
    """
    Serializa ficha para session state.
    """
    df = padronizar_ficha_df(df_ficha)
    return df.to_dict("records")


def desserializar_ficha(registros: list[dict]) -> pd.DataFrame:
    """
    Desserializa ficha vinda do session state.
    """
    if not registros:
        return dataframe_vazio_ficha()
    return padronizar_ficha_df(pd.DataFrame(registros))


# ============================================================
# MÉTRICAS DE TREINO
# ============================================================
def calcular_volume(peso: float, reps: float, series: float) -> float:
    """
    Volume simples.
    """
    p = clamp_float(to_float(peso, 0.0), 0.0, MAX_PESO_VALIDACAO)
    r = clamp_float(to_float(reps, 0.0), 0.0, MAX_REPS_VALIDACAO)
    s = clamp_float(to_float(series, 0.0), 0.0, MAX_SERIES_VALIDACAO)
    return float(p * r * s)


def calcular_1rm(peso: float, reps: float) -> float:
    """
    Estimativa de 1RM pela fórmula de Epley.
    """
    p = clamp_float(to_float(peso, 0.0), 0.0, MAX_PESO_VALIDACAO)
    r = clamp_float(to_float(reps, 0.0), 0.0, MAX_REPS_VALIDACAO)

    if r <= 0:
        return 0.0

    return float(p * (1 + (r / 30.0)))


def score_progressao_percentual(primeiro: float, ultimo: float) -> Optional[float]:
    """
    Percentual de progressão.
    """
    if primeiro is None or ultimo is None:
        return None
    if primeiro <= 0:
        return None
    return ((ultimo - primeiro) / primeiro) * 100.0


# ============================================================
# MODELAGEM COM SKLEARN
# ============================================================
def regressao_linear_sklearn(y: np.ndarray) -> tuple[float, float]:
    """
    Regressão linear sobre sequência indexada.
    """
    if len(y) < 2:
        return 0.0, float(y[-1]) if len(y) else 0.0

    x = np.arange(len(y)).reshape(-1, 1)
    model = LinearRegression()
    model.fit(x, y)

    slope = float(model.coef_[0])
    prox = float(model.predict(np.array([[len(y)]]))[0])

    return slope, prox


def tendencia_forca(y: np.ndarray) -> str:
    """
    Classifica tendência.
    """
    if len(y) < 2:
        return "Dados insuficientes"

    slope, _ = regressao_linear_sklearn(y)

    if slope > 0.05:
        return "📈 Subindo"
    if slope < -0.05:
        return "📉 Regredindo"
    return "➡️ Estagnado"


def carga_sugerida(y: np.ndarray) -> float:
    """
    Sugere próxima carga respeitando coerência:
    - não negativa
    - limitada a ±5% do último treino
    - não menor que 95% do último
    - não absurdamente abaixo do melhor histórico recente
    """
    if len(y) == 0:
        return 0.0

    y = np.array([to_float(v, 0.0) for v in y], dtype=float)
    y = y[~np.isnan(y)]

    if len(y) == 0:
        return 0.0

    if len(y) == 1:
        return round(float(y[-1]) * 2) / 2

    _, prox = regressao_linear_sklearn(y)

    ultimo = float(y[-1])
    melhor = float(np.max(y))

    if prox < 0:
        prox = ultimo

    limite_superior = ultimo * 1.05
    limite_inferior = ultimo * 0.95

    prox = min(prox, limite_superior)
    prox = max(prox, limite_inferior)

    # não sugerir regressão bizarra se o histórico já consolidou acima
    if melhor > 0 and prox < melhor * 0.90:
        prox = max(prox, ultimo)

    return round(float(prox) * 2) / 2


# ============================================================
# LEITURA DAS ABAS
# ============================================================
def carregar_pacientes() -> pd.DataFrame:
    """
    Lê pacientes.
    """
    return normalizar_colunas(read_sheet(SHEET_PATIENTS))


def carregar_treino(nome_treino: str) -> pd.DataFrame:
    """
    Lê aba do treino.
    """
    return normalizar_colunas(read_sheet(MAP_TREINOS[nome_treino]))


def carregar_historico() -> pd.DataFrame:
    """
    Lê histórico.
    """
    return normalizar_colunas(read_sheet(SHEET_HISTORICO))


# ============================================================
# HIGIENIZAÇÃO DO HISTÓRICO
# ============================================================
def garantir_colunas_historico() -> None:
    """
    Garante cabeçalho do histórico.
    """
    ws = get_worksheet(SHEET_HISTORICO)
    headers = ws.row_values(1)

    if headers != HIST_HEADERS:
        ws.update("A1:L1", [HIST_HEADERS])

    clear_cached_data()


def registrar_series_em_lote(registros: list[list]) -> None:
    """
    Insere linhas no histórico.
    """
    if not registros:
        return

    ws = get_worksheet(SHEET_HISTORICO)
    ws.append_rows(registros, value_input_option="USER_ENTERED")
    clear_cached_data()


def construir_linha_historico(
    data_iso: str,
    cpf: str,
    semana: int,
    treino: str,
    exercicio: str,
    grupo: str,
    peso: Any,
    reps: Any,
    series: Any,
    volume: Any,
    rm: Any,
    observacao: str,
) -> list:
    """
    Monta linha com ordem exata do schema.
    """
    return [
        data_iso,
        str(cpf).strip(),
        int(semana),
        str(treino).strip(),
        str(exercicio).strip(),
        str(grupo).strip(),
        peso,
        reps,
        series,
        volume,
        rm,
        observacao,
    ]


def higienizar_df_historico(df_hist: pd.DataFrame) -> pd.DataFrame:
    """
    Converte histórico cru para formato consistente.
    """
    if df_hist.empty:
        return dataframe_vazio_historico()

    df = normalizar_colunas(df_hist.copy())

    for col in HIST_HEADERS:
        if col not in df.columns:
            df[col] = ""

    df = df[HIST_HEADERS].copy()

    df["DataParsed"] = parse_datetime_series(df["Data"])
    df["CPF"] = df["CPF"].astype(str).str.strip()
    df["Treino"] = df["Treino"].astype(str).str.strip()
    df["Exercicio"] = df["Exercicio"].astype(str).str.strip()
    df["Grupo"] = df["Grupo"].astype(str).str.strip()
    df["Observacao"] = df["Observacao"].astype(str)

    df["TipoLinha"] = df["Observacao"].apply(extrair_tipo_da_observacao)
    # compatibilidade com histórico antigo
    df.loc[df["Exercicio"].str.startswith("FICHA::"), "TipoLinha"] = TIPO_LINHA_FICHA
    df.loc[df["Exercicio"].str.strip() == TIPO_LINHA_CALORIA, "TipoLinha"] = TIPO_LINHA_CALORIA
    df.loc[df["TipoLinha"] == "", "TipoLinha"] = TIPO_LINHA_TREINO

    df["ExercicioToken"] = df["Exercicio"].apply(normalizar_nome_exercicio)
    df["GrupoFinal"] = df["Grupo"].replace("", pd.NA)
    df["GrupoFinal"] = df["GrupoFinal"].fillna(df["Exercicio"].apply(inferir_grupo))

    df["PesoNum"] = df["Peso"].apply(lambda x: to_float(x, np.nan))
    df["RepsNum"] = df["Reps"].apply(lambda x: to_float(x, np.nan))
    df["SeriesNum"] = df["Series"].apply(lambda x: to_float(x, np.nan))
    df["VolumeNum"] = df["Volume"].apply(lambda x: to_float(x, np.nan))
    df["RMNum"] = df["1RM"].apply(lambda x: to_float(x, np.nan))

    df["VolumeCalcSafe"] = df["VolumeNum"].fillna(
        df["PesoNum"].fillna(0.0) * df["RepsNum"].fillna(0.0) * df["SeriesNum"].fillna(0.0)
    )

    df["RMCalcSafe"] = df["RMNum"].fillna(
        df["PesoNum"].fillna(0.0) * (1 + (df["RepsNum"].fillna(0.0) / 30.0))
    )

    df = df.sort_values("DataParsed", ascending=True, kind="stable")
    df = df.reset_index(drop=True)

    return df


def limpar_fichas_duplicadas_mesmo_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicatas da ficha apenas quando forem realmente iguais
    em timestamp + cpf + treino + token do exercício.
    Mantém estável e não elimina exercícios diferentes.
    """
    if df.empty:
        return df.copy()

    novo = df.copy()

    if "DataParsed" not in novo.columns:
        col_data = achar_coluna(novo, POSSIVEIS_COLUNAS_DATA)
        if not col_data:
            return novo
        novo["DataParsed"] = parse_datetime_series(novo[col_data])

    if "CPF" not in novo.columns:
        col_cpf = achar_coluna(novo, POSSIVEIS_COLUNAS_CPF)
        if not col_cpf:
            return novo
        novo["CPF"] = novo[col_cpf].astype(str).str.strip()

    if "Treino" not in novo.columns:
        col_treino = achar_coluna(novo, POSSIVEIS_COLUNAS_TREINO)
        if not col_treino:
            return novo
        novo["Treino"] = novo[col_treino].astype(str).str.strip()

    if "ExercicioToken" not in novo.columns:
        col_ex = achar_coluna(novo, POSSIVEIS_COLUNAS_EXERCICIO)
        if not col_ex:
            return novo
        novo["ExercicioToken"] = novo[col_ex].astype(str).apply(normalizar_nome_exercicio)

    novo = novo.sort_values("DataParsed", kind="stable")
    novo = novo.drop_duplicates(
        subset=["DataParsed", "CPF", "Treino", "ExercicioToken"],
        keep="last",
    )
    novo = novo.reset_index(drop=True)
    return novo


def obter_timestamp_mais_recente(df: pd.DataFrame, col_data: str) -> Optional[pd.Timestamp]:
    """
    Último timestamp válido.
    """
    if df.empty or col_data not in df.columns:
        return None

    serie = parse_datetime_series(df[col_data]).dropna()

    if serie.empty:
        return None

    return pd.Timestamp(serie.max())


# ============================================================
# FILTROS DE HISTÓRICO
# ============================================================
def filtrar_historico_paciente(df_hist: pd.DataFrame, cpf: str) -> pd.DataFrame:
    """
    Filtra histórico por CPF.
    """
    if df_hist.empty:
        return dataframe_vazio_historico()

    df = higienizar_df_historico(df_hist)
    cpf_limpo = str(cpf).strip()

    return df[df["CPF"] == cpf_limpo].reset_index(drop=True)


def filtrar_linhas_treino(df_hist_paciente: pd.DataFrame) -> pd.DataFrame:
    """
    Mantém apenas linhas de treino normal.
    """
    if df_hist_paciente.empty:
        return dataframe_vazio_historico()
    return df_hist_paciente[df_hist_paciente["TipoLinha"] == TIPO_LINHA_TREINO].copy().reset_index(drop=True)


def filtrar_linhas_ficha(df_hist_paciente: pd.DataFrame) -> pd.DataFrame:
    """
    Mantém apenas linhas de ficha.
    """
    if df_hist_paciente.empty:
        return dataframe_vazio_historico()
    return df_hist_paciente[df_hist_paciente["TipoLinha"] == TIPO_LINHA_FICHA].copy().reset_index(drop=True)


def filtrar_linhas_caloria(df_hist_paciente: pd.DataFrame) -> pd.DataFrame:
    """
    Mantém apenas linhas de calorias.
    """
    if df_hist_paciente.empty:
        return dataframe_vazio_historico()
    return df_hist_paciente[df_hist_paciente["TipoLinha"] == TIPO_LINHA_CALORIA].copy().reset_index(drop=True)


def historico_exercicio(df_hist_paciente_treino: pd.DataFrame, exercicio: str) -> pd.DataFrame:
    """
    Histórico de um exercício específico.
    """
    if df_hist_paciente_treino.empty:
        return dataframe_vazio_historico()

    token = normalizar_nome_exercicio(exercicio)
    df = df_hist_paciente_treino.copy()
    return df[df["ExercicioToken"] == token].copy().reset_index(drop=True)


def ultima_linha_exercicio(df_hist_paciente_treino: pd.DataFrame, exercicio: str) -> Optional[pd.Series]:
    """
    Última linha cronológica do exercício.
    """
    df = historico_exercicio(df_hist_paciente_treino, exercicio)
    if df.empty:
        return None

    df = df.sort_values("DataParsed", ascending=True, kind="stable").reset_index(drop=True)
    return df.iloc[-1]


def media_peso_exercicio(df_hist_paciente_treino: pd.DataFrame, exercicio: str) -> Optional[float]:
    """
    Média de peso do exercício.
    """
    df = historico_exercicio(df_hist_paciente_treino, exercicio)
    if df.empty:
        return None

    serie = pd.to_numeric(df["PesoNum"], errors="coerce").dropna()
    if serie.empty:
        return None

    return float(serie.mean())


def melhor_peso_exercicio(df_hist_paciente_treino: pd.DataFrame, exercicio: str) -> Optional[float]:
    """
    Melhor peso do exercício.
    """
    df = historico_exercicio(df_hist_paciente_treino, exercicio)
    if df.empty:
        return None

    serie = pd.to_numeric(df["PesoNum"], errors="coerce").dropna()
    if serie.empty:
        return None

    return float(serie.max())


# ============================================================
# ESCRITA / LEITURA DA FICHA DO USUÁRIO
# ============================================================
def salvar_ficha_usuario(cpf: str, treino: str, ficha: list[dict]) -> None:
    """
    Salva uma nova versão da ficha do usuário no histórico.
    """
    df_ficha = desserializar_ficha(ficha)
    ok, erros, df_ficha = validar_ficha(df_ficha)
    if not ok:
        raise ValueError(" ".join(erros))

    timestamp = agora_timestamp_iso()
    semana = iso_week_from_timestamp(parse_datetime_value(timestamp))

    registros = []
    for _, row in df_ficha.iterrows():
        exercicio = str(row["Exercicio"]).strip()
        grupo = str(row["Grupo"]).strip() if str(row["Grupo"]).strip() else inferir_grupo(exercicio)
        reps = to_int(row["Reps"], 0)
        series = to_int(row["Series"], 0)

        registros.append(
            construir_linha_historico(
                data_iso=timestamp,
                cpf=cpf,
                semana=semana,
                treino=treino,
                exercicio=exercicio_ficha_token(exercicio),
                grupo=grupo,
                peso="",
                reps=reps,
                series=series,
                volume="",
                rm="",
                observacao=linha_observacao_tipo(TIPO_LINHA_FICHA),
            )
        )

    registrar_series_em_lote(registros)


def carregar_ficha_usuario(df_hist: pd.DataFrame, cpf: str, treino: str) -> Optional[list[dict]]:
    """
    Carrega a última ficha salva do usuário para o treino.
    """
    if df_hist.empty:
        return None

    df = filtrar_historico_paciente(df_hist, cpf)
    df = filtrar_linhas_ficha(df)

    if df.empty:
        return None

    df = df[df["Treino"].astype(str).str.strip() == str(treino).strip()].copy()

    if df.empty:
        return None

    df = df.dropna(subset=["DataParsed"]).copy()
    if df.empty:
        return None

    ultimo_timestamp = df["DataParsed"].max()
    df = df[df["DataParsed"] == ultimo_timestamp].copy()
    df = limpar_fichas_duplicadas_mesmo_timestamp(df)

    registros = []
    for _, row in df.iterrows():
        ex = str(row["Exercicio"]).replace("FICHA::", "").strip()
        if not ex:
            continue

        grupo = str(row["Grupo"]).strip() if str(row["Grupo"]).strip() else inferir_grupo(ex)
        reps = to_int(row["Reps"], 10)
        series = to_int(row["Series"], 3)

        registros.append(
            {
                "Exercicio": ex,
                "Grupo": grupo,
                "Series": series,
                "Reps": reps,
            }
        )

    if not registros:
        return None

    df_ficha = desserializar_ficha(registros)
    ok, _, df_ficha = validar_ficha(df_ficha)
    if not ok:
        return None

    return df_ficha.to_dict("records")


def atualizar_ficha_treino(nome_treino: str, df_editado: pd.DataFrame) -> None:
    """
    Atualiza a aba-base do treino.
    Mantida por compatibilidade, mas não é usada no fluxo principal.
    """
    ok, erros, df_editado = validar_ficha(padronizar_ficha_df(df_editado))
    if not ok:
        raise ValueError(" ".join(erros))

    ws = get_worksheet(MAP_TREINOS[nome_treino])
    dados = [df_editado.columns.tolist()] + df_editado.astype(str).values.tolist()

    ws.clear()
    ws.update("A1", dados)

    clear_cached_data()


# ============================================================
# PRÉ-CÁLCULOS / OTIMIZAÇÃO DO HISTÓRICO
# ============================================================
def construir_cache_exercicios(df_hist_paciente_treino: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Cria mapa token_exercicio -> histórico daquele exercício.
    Evita varrer o dataframe inteiro repetidamente.
    """
    cache: dict[str, pd.DataFrame] = {}

    if df_hist_paciente_treino.empty:
        return cache

    for token, grupo in df_hist_paciente_treino.groupby("ExercicioToken", sort=False):
        cache[str(token)] = grupo.sort_values("DataParsed", ascending=True, kind="stable").reset_index(drop=True)

    return cache


def obter_hist_exercicio_cache(cache_hist: dict[str, pd.DataFrame], exercicio: str) -> pd.DataFrame:
    """
    Busca histórico do exercício no cache.
    """
    token = normalizar_nome_exercicio(exercicio)
    return cache_hist.get(token, dataframe_vazio_historico()).copy()


def resumo_exercicio_cache(cache_hist: dict[str, pd.DataFrame], exercicio: str) -> dict[str, Any]:
    """
    Retorna resumo rápido do exercício.
    """
    hist_ex = obter_hist_exercicio_cache(cache_hist, exercicio)

    if hist_ex.empty:
        return {
            "ultima": None,
            "media_peso": None,
            "melhor_peso": None,
            "ultimo_peso": "",
            "ultimo_reps": "",
            "carga_prevista": None,
            "hist": hist_ex,
        }

    ultima = hist_ex.iloc[-1]
    media_peso = float(hist_ex["PesoNum"].dropna().mean()) if hist_ex["PesoNum"].dropna().shape[0] else None
    melhor_peso = float(hist_ex["PesoNum"].dropna().max()) if hist_ex["PesoNum"].dropna().shape[0] else None
    ultimo_peso = ultima["Peso"]
    ultimo_reps = ultima["Reps"]

    serie_cargas = pd.to_numeric(hist_ex["PesoNum"], errors="coerce").dropna().values
    carga_prevista = carga_sugerida(serie_cargas) if len(serie_cargas) >= 2 else None

    return {
        "ultima": ultima,
        "media_peso": media_peso,
        "melhor_peso": melhor_peso,
        "ultimo_peso": ultimo_peso,
        "ultimo_reps": ultimo_reps,
        "carga_prevista": carga_prevista,
        "hist": hist_ex,
    }


# ============================================================
# PREPARAÇÃO DO DF DE PROGRESSO
# ============================================================
def preparar_df_progresso(df_hist_paciente: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Prepara dataframe de progresso somente com linhas de treino válidas.
    """
    info = {
        "col_ex": "Exercicio",
        "col_peso": "PesoNum",
        "col_series": "SeriesNum",
        "col_reps": "RepsNum",
        "col_data": "DataParsed",
        "col_treino": "Treino",
        "col_grupo_hist": "GrupoFinal",
        "col_1rm": "RMCalcSafe",
        "col_volume": "VolumeCalcSafe",
    }

    if df_hist_paciente.empty:
        return pd.DataFrame(), info

    df = filtrar_linhas_treino(df_hist_paciente)

    if df.empty:
        return pd.DataFrame(), info

    df = df.copy()

    df = df.dropna(subset=["DataParsed"]).copy()
    df = df.dropna(subset=["PesoNum"]).copy()

    df = df[df["Exercicio"].astype(str).str.strip() != ""].copy()
    df = df[df["PesoNum"] >= 0].copy()

    df = df.sort_values("DataParsed", ascending=True, kind="stable").reset_index(drop=True)

    if df.empty:
        return pd.DataFrame(), info

    df["semana_periodo"] = df["DataParsed"].dt.to_period("W")
    df["semana_texto"] = df["semana_periodo"].astype(str)

    # id de sessão: um treino salvo em um timestamp
    df["SessaoID"] = (
        df["CPF"].astype(str).str.strip()
        + "||"
        + df["Treino"].astype(str).str.strip()
        + "||"
        + df["DataParsed"].astype(str)
    )

    # observação limpa para exibição
    df["ObservacaoLimpa"] = df["Observacao"].apply(remover_marcador_tipo)

    return df, info


# ============================================================
# FUNÇÕES DE SESSION STATE
# ============================================================
def limpar_session_widgets_edicao() -> None:
    """
    Limpa widgets de edição de ficha.
    """
    apagar = []
    for chave in list(st.session_state.keys()):
        if chave.startswith("edit_ex_"):
            apagar.append(chave)
        if chave.startswith("edit_series_"):
            apagar.append(chave)
        if chave.startswith("edit_reps_"):
            apagar.append(chave)
        if chave.startswith("del_"):
            apagar.append(chave)

    for chave in apagar:
        del st.session_state[chave]


def limpar_session_widgets_registro() -> None:
    """
    Limpa widgets de formulário de treino.
    """
    apagar = []
    for chave in list(st.session_state.keys()):
        if chave.startswith("series_"):
            apagar.append(chave)
        if chave.startswith("reps_"):
            apagar.append(chave)
        if chave.startswith("peso_"):
            apagar.append(chave)
        if chave.startswith("obs_"):
            apagar.append(chave)

    for chave in apagar:
        del st.session_state[chave]


def resetar_estado_paciente_treino() -> None:
    """
    Limpa tudo que depende do paciente/treino.
    """
    st.session_state.pop(SESSION_PREFIX_FICHA, None)
    limpar_session_widgets_edicao()
    limpar_session_widgets_registro()


def sincronizar_estado_sidebar(cpf: str, pagina: str, treino_escolhido: Optional[str]) -> None:
    """
    Detecta mudança real de paciente/página/treino e reseta apenas o necessário.
    """
    cpf_atual = st.session_state.get(SESSION_CPF_ATUAL)
    pagina_atual = st.session_state.get(SESSION_PAGINA_ATUAL)
    treino_atual = st.session_state.get(SESSION_TREINO_ATUAL)

    mudou_cpf = cpf_atual != cpf
    mudou_pagina = pagina_atual != pagina
    mudou_treino = treino_atual != treino_escolhido

    if mudou_cpf or mudou_treino:
        resetar_estado_paciente_treino()

    if mudou_pagina:
        limpar_session_widgets_registro()

    st.session_state[SESSION_CPF_ATUAL] = cpf
    st.session_state[SESSION_PAGINA_ATUAL] = pagina
    st.session_state[SESSION_TREINO_ATUAL] = treino_escolhido


# ============================================================
# FUNÇÕES DE DISPLAY / FORMATAÇÃO
# ============================================================
def fmt_kg(valor: Optional[float]) -> str:
    """
    Formata quilos.
    """
    if valor is None or pd.isna(valor):
        return "-"
    return f"{float(valor):.1f} kg"


def fmt_pct(valor: Optional[float]) -> str:
    """
    Formata percentual.
    """
    if valor is None or pd.isna(valor):
        return "-"
    return f"{float(valor):.2f}%"


def fmt_num(valor: Optional[float], casas: int = 1) -> str:
    """
    Formata número.
    """
    if valor is None or pd.isna(valor):
        return "-"
    return f"{float(valor):.{casas}f}"


def exibir_erros_lista(erros: list[str]) -> None:
    """
    Mostra erros.
    """
    for erro in erros:
        st.error(erro)


def dataframe_para_exibicao_historico(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepara histórico para exibição final.
    """
    if df.empty:
        return df.copy()

    novo = df.copy()
    if "ObservacaoLimpa" in novo.columns:
        novo["Observacao"] = novo["ObservacaoLimpa"]

    colunas_preferidas = [
        "DataParsed",
        "Treino",
        "Exercicio",
        "GrupoFinal",
        "PesoNum",
        "RepsNum",
        "SeriesNum",
        "VolumeCalcSafe",
        "RMCalcSafe",
        "Observacao",
    ]
    colunas_presentes = [c for c in colunas_preferidas if c in novo.columns]
    novo = novo[colunas_presentes].copy()

    renomear = {
        "DataParsed": "Data",
        "GrupoFinal": "Grupo",
        "PesoNum": "Peso",
        "RepsNum": "Reps",
        "SeriesNum": "Series",
        "VolumeCalcSafe": "Volume",
        "RMCalcSafe": "1RM",
    }
    novo = novo.rename(columns=renomear)

    if "Data" in novo.columns:
        novo["Data"] = novo["Data"].apply(formatar_data_br)

    return novo


# ============================================================
# LEITURA PRINCIPAL DE DADOS
# ============================================================
def carregar_dados_principais(pagina: str, treino_escolhido: Optional[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Carrega histórico e treino-base.
    """
    df_hist = carregar_historico()
    df_hist = normalizar_colunas(df_hist)

    if pagina == "Treino" and treino_escolhido:
        df_treino_base = carregar_treino(treino_escolhido)
        df_treino_base = normalizar_colunas(df_treino_base)
    else:
        df_treino_base = pd.DataFrame()

    return df_hist, df_treino_base


# ============================================================
# FICHA ATIVA
# ============================================================
def carregar_ficha_ativa(
    df_hist: pd.DataFrame,
    cpf: str,
    treino_escolhido: str,
    resumo_df_base: pd.DataFrame,
) -> pd.DataFrame:
    """
    Decide qual ficha será usada:
    1. session_state
    2. última ficha personalizada salva
    3. ficha base da aba
    """
    if SESSION_PREFIX_FICHA in st.session_state and st.session_state[SESSION_PREFIX_FICHA]:
        return desserializar_ficha(st.session_state[SESSION_PREFIX_FICHA])

    ficha_usuario = carregar_ficha_usuario(df_hist, cpf, treino_escolhido)
    if ficha_usuario:
        df_ficha = desserializar_ficha(ficha_usuario)
    else:
        df_ficha = padronizar_ficha_df(resumo_df_base)

    st.session_state[SESSION_PREFIX_FICHA] = serializar_ficha(df_ficha)
    return df_ficha


def salvar_ficha_em_session(df_ficha: pd.DataFrame) -> None:
    """
    Atualiza session state da ficha.
    """
    st.session_state[SESSION_PREFIX_FICHA] = serializar_ficha(df_ficha)


# ============================================================
# REGISTRO DE TREINO
# ============================================================
def montar_registros_treino_para_salvar(
    cpf: str,
    treino_escolhido: str,
    df_treino_ativo: pd.DataFrame,
    obs_geral: str,
    calorias: int,
) -> list[list]:
    """
    Coleta o formulário e cria as linhas do histórico.
    """
    data_dt = pd.Timestamp.now()
    data_agora_iso = data_dt.strftime(TIMESTAMP_FMT_ISO)
    semana = iso_week_from_timestamp(data_dt)

    registros_para_salvar: list[list] = []

    for i, row in df_treino_ativo.reset_index(drop=True).iterrows():
        exercicio = str(row.get("Exercicio", "")).strip()
        if not exercicio:
            continue

        grupo_ficha = str(row.get("Grupo", "")).strip()
        grupo_salvar = grupo_ficha if grupo_ficha else inferir_grupo(exercicio)

        series_feitas = clamp_int(to_int(st.session_state.get(f"series_{i}", 0), 0), 0, MAX_SERIES_VALIDACAO)
        reps_feitas = clamp_int(to_int(st.session_state.get(f"reps_{i}", 0), 0), 0, MAX_REPS_VALIDACAO)
        peso_usado = clamp_float(to_float(st.session_state.get(f"peso_{i}", 0.0), 0.0), 0.0, MAX_PESO_VALIDACAO)
        obs = str(st.session_state.get(f"obs_{i}", "")).strip()

        # salva apenas quando houver dado concreto de treino
        tem_dado_relevante = any(
            [
                series_feitas > 0,
                reps_feitas > 0,
                peso_usado > 0,
                obs != "",
            ]
        )

        if not tem_dado_relevante:
            continue

        volume = calcular_volume(peso_usado, reps_feitas, series_feitas)
        rm_estimado = calcular_1rm(peso_usado, reps_feitas)

        obs_parts = []
        if obs:
            obs_parts.append(obs)
        if str(obs_geral).strip():
            obs_parts.append(str(obs_geral).strip())
        obs_final_limpo = " | ".join(obs_parts).strip()

        registros_para_salvar.append(
            construir_linha_historico(
                data_iso=data_agora_iso,
                cpf=cpf,
                semana=semana,
                treino=treino_escolhido,
                exercicio=exercicio,
                grupo=grupo_salvar,
                peso=peso_usado,
                reps=reps_feitas,
                series=series_feitas,
                volume=volume,
                rm=rm_estimado,
                observacao=linha_observacao_tipo(TIPO_LINHA_TREINO, obs_final_limpo),
            )
        )

    if calorias > 0:
        registros_para_salvar.append(
            construir_linha_historico(
                data_iso=data_agora_iso,
                cpf=cpf,
                semana=semana,
                treino=treino_escolhido,
                exercicio=TIPO_LINHA_CALORIA,
                grupo="Cardio",
                peso=calorias,
                reps="",
                series="",
                volume="",
                rm="",
                observacao=linha_observacao_tipo(TIPO_LINHA_CALORIA, str(obs_geral).strip()),
            )
        )

    return registros_para_salvar


# ============================================================
# ANÁLISES DE PROGRESSO
# ============================================================
def detectar_prs(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Recordes pessoais e últimas execuções.
    """
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    pr = (
        df.groupby("Exercicio")["PesoNum"]
        .max()
        .sort_values(ascending=False)
        .to_frame("PR (kg)")
    )

    ultimos = (
        df.sort_values("DataParsed", ascending=True, kind="stable")
        .groupby("Exercicio", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )

    return pr, ultimos


def score_geral_forca(df: pd.DataFrame) -> tuple[Optional[float], pd.DataFrame]:
    """
    Calcula score médio de progressão dos exercícios.
    """
    progressao = []

    if df.empty:
        return None, pd.DataFrame()

    for ex in sorted(df["Exercicio"].dropna().unique()):
        dados = df[df["Exercicio"] == ex].sort_values("DataParsed")
        if len(dados) < 2:
            continue

        primeiro = float(dados.iloc[0]["PesoNum"])
        ultimo = float(dados.iloc[-1]["PesoNum"])

        pct = score_progressao_percentual(primeiro, ultimo)
        if pct is None:
            continue

        progressao.append(
            {
                "Exercício": ex,
                "Progressão (%)": round(pct, 2),
            }
        )

    if not progressao:
        return None, pd.DataFrame()

    df_prog = pd.DataFrame(progressao).sort_values("Progressão (%)", ascending=False).reset_index(drop=True)
    score = float(df_prog["Progressão (%)"].mean()) if not df_prog.empty else None

    return score, df_prog


def analise_ml_exercicios(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tendência, sugestão de carga e previsão de PR.
    """
    linhas = []

    if df.empty:
        return pd.DataFrame()

    for ex in sorted(df["Exercicio"].dropna().unique()):
        dados = df[df["Exercicio"] == ex].sort_values("DataParsed")
        y = dados["PesoNum"].astype(float).dropna().values

        if len(y) == 0:
            continue

        trend = tendencia_forca(y)
        sugestao = carga_sugerida(y)

        if len(y) >= 2:
            _, prox = regressao_linear_sklearn(y)
        else:
            prox = float(y[-1])

        pr_atual = float(np.max(y))
        pr_previsto = max(float(prox), pr_atual)

        linhas.append(
            {
                "Exercício": ex,
                "Tendência": trend,
                "Próxima carga sugerida": round(sugestao, 1),
                "PR atual": round(pr_atual, 1),
                "PR previsto": round(pr_previsto, 1),
            }
        )

    return pd.DataFrame(linhas)


def detectar_plato(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detecta platô considerando peso e 1RM.
    Evita falso positivo grosseiro com mesmo peso e reps melhores.
    """
    linhas = []

    if df.empty:
        return pd.DataFrame()

    for ex in sorted(df["Exercicio"].dropna().unique()):
        dados = df[df["Exercicio"] == ex].sort_values("DataParsed")
        if len(dados) < 4:
            continue

        ultimos_4 = dados.tail(4).copy()
        pesos = ultimos_4["PesoNum"].astype(float).round(2).tolist()
        rms = ultimos_4["RMCalcSafe"].astype(float).round(2).tolist()

        # só considera platô se tanto peso quanto 1RM estimado estiverem travados
        if len(set(pesos)) == 1 and len(set(rms)) <= 2:
            linhas.append(
                {
                    "Exercício": ex,
                    "Status": f"⚠️ Estagnado há {len(ultimos_4)} registros",
                }
            )

    return pd.DataFrame(linhas)


def volume_por_exercicio(df: pd.DataFrame) -> pd.Series:
    """
    Volume por exercício.
    """
    if df.empty:
        return pd.Series(dtype=float)

    return (
        df.groupby("Exercicio")["VolumeCalcSafe"]
        .sum()
        .sort_values(ascending=False)
    )


def volume_por_grupo(df: pd.DataFrame) -> pd.Series:
    """
    Volume por grupo muscular.
    """
    if df.empty:
        return pd.Series(dtype=float)

    return (
        df.groupby("GrupoFinal")["VolumeCalcSafe"]
        .sum()
        .sort_values(ascending=False)
    )


def evolucao_semanal(df: pd.DataFrame) -> pd.DataFrame:
    """
    Métricas semanais.
    Sessões conta treinos únicos, não exercícios.
    """
    if df.empty:
        return pd.DataFrame()

    semanal_base = (
        df.groupby("semana_texto")
        .agg(
            Volume_Total=("VolumeCalcSafe", "sum"),
            Carga_Média=("PesoNum", "mean"),
            RM_Médio=("RMCalcSafe", "mean"),
            Exercícios=("Exercicio", "count"),
            Sessões=("SessaoID", pd.Series.nunique),
        )
        .reset_index()
    )

    return semanal_base


def alertas_overtraining(df: pd.DataFrame) -> list[str]:
    """
    Regras simples de alerta.
    """
    alertas: list[str] = []

    if df.empty:
        return alertas

    vol_semana = (
        df.groupby("semana_texto")["VolumeCalcSafe"]
        .sum()
        .sort_index()
    )

    if len(vol_semana) >= 2:
        ultima = float(vol_semana.iloc[-1])
        anterior = float(vol_semana.iloc[-2])

        if anterior > 0:
            aumento = ((ultima - anterior) / anterior) * 100
            if aumento > 25:
                alertas.append(f"Volume semanal subiu {aumento:.1f}% em relação à semana anterior.")

    for ex in sorted(df["Exercicio"].dropna().unique()):
        dados = df[df["Exercicio"] == ex].sort_values("DataParsed")
        if len(dados) < 3:
            continue

        ultimos_3 = dados.tail(3).copy()
        media_volume = float(ultimos_3["VolumeCalcSafe"].mean())
        volume_atual = float(ultimos_3.iloc[-1]["VolumeCalcSafe"])

        y = ultimos_3["PesoNum"].astype(float).values
        trend = tendencia_forca(y)

        if media_volume > 0 and volume_atual > media_volume * 1.3 and trend == "📉 Regredindo":
            alertas.append(f"{ex}: volume recente alto com queda de desempenho.")

    return alertas


def recomendacao_deload(df: pd.DataFrame) -> tuple[bool, list[str]]:
    """
    Recomenda deload.
    """
    precisa_deload = False
    motivos: list[str] = []

    if df.empty:
        return False, motivos

    vol_semana = (
        df.groupby("semana_texto")["VolumeCalcSafe"]
        .sum()
        .sort_index()
    )

    if len(vol_semana) >= 2:
        ultima = float(vol_semana.iloc[-1])
        anterior = float(vol_semana.iloc[-2])

        if anterior > 0:
            aumento = ((ultima - anterior) / anterior) * 100
            if aumento > 30:
                precisa_deload = True
                motivos.append(f"Volume semanal aumentou {aumento:.1f}%.")

    regressao_count = 0
    for ex in sorted(df["Exercicio"].dropna().unique()):
        dados = df[df["Exercicio"] == ex].sort_values("DataParsed")
        y = dados["PesoNum"].astype(float).dropna().values
        if len(y) >= 3 and tendencia_forca(y[-3:]) == "📉 Regredindo":
            regressao_count += 1

    if regressao_count >= 3:
        precisa_deload = True
        motivos.append("Há regressão recente em vários exercícios.")

    return precisa_deload, motivos


def frequencia_treino(df: pd.DataFrame) -> pd.DataFrame:
    """
    Frequência por semana em sessões únicas.
    """
    if df.empty:
        return pd.DataFrame()

    freq = (
        df.groupby("semana_texto")["SessaoID"]
        .nunique()
        .to_frame("Treinos")
        .reset_index()
    )
    return freq


# ============================================================
# UI - SIDEBAR
# ============================================================
st.title(APP_TITLE)
st.caption(APP_CAPTION)

with st.sidebar:
    st.header("Configuração")

    pagina = st.radio("Página", ["Treino", "Progresso"], horizontal=False)
    cpf = st.text_input("CPF do paciente", placeholder="Digite o CPF")

    if pagina == "Treino":
        treino_escolhido = st.radio("Treino", list(MAP_TREINOS.keys()), horizontal=False)
    else:
        treino_escolhido = None

    sincronizar_estado_sidebar(cpf=cpf, pagina=pagina, treino_escolhido=treino_escolhido)

    df_pacientes_sidebar = carregar_pacientes()
    nome_paciente = ""

    col_paciente_cpf = achar_coluna(df_pacientes_sidebar, POSSIVEIS_COLUNAS_CPF)
    col_paciente_nome = achar_coluna(df_pacientes_sidebar, POSSIVEIS_COLUNAS_NOME)

    if cpf and not df_pacientes_sidebar.empty and col_paciente_cpf and col_paciente_nome:
        resultado = df_pacientes_sidebar[
            df_pacientes_sidebar[col_paciente_cpf].astype(str).str.strip() == str(cpf).strip()
        ]

        if not resultado.empty:
            nome_paciente = str(resultado.iloc[0][col_paciente_nome]).strip()
            st.success(f"Paciente: {nome_paciente}")
        else:
            st.warning("CPF não encontrado")

    atualizar = st.button("Atualizar dados")
    if atualizar:
        clear_cached_data()
        st.rerun()


# ============================================================
# GUARDA DE ENTRADA
# ============================================================
if not cpf:
    st.warning("Informe o CPF do paciente para continuar.")
    st.stop()


# ============================================================
# CARREGAMENTO
# ============================================================
try:
    df_hist_raw, df_treino_base = carregar_dados_principais(pagina, treino_escolhido)
except Exception as e:
    st.error("Não consegui abrir a planilha. Verifique as credenciais e o compartilhamento com a conta de serviço.")
    st.exception(e)
    st.stop()

df_hist_paciente = filtrar_historico_paciente(df_hist_raw, cpf)
df_hist_paciente_treino = filtrar_linhas_treino(df_hist_paciente)
cache_hist_exercicios = construir_cache_exercicios(df_hist_paciente_treino)


# ============================================================
# PÁGINA TREINO
# ============================================================
if pagina == "Treino":
    if df_treino_base.empty:
        st.error(f"A aba {treino_escolhido} está vazia.")
        st.stop()

    resumo_df_base = padronizar_ficha_df(df_treino_base.copy())

    if resumo_df_base.empty:
        st.error(f"A aba {treino_escolhido} precisa ter pelo menos um exercício válido.")
        st.stop()

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Paciente", nome_paciente if nome_paciente else cpf)
    with c2:
        st.metric("Treino atual", treino_escolhido)
    with c3:
        st.metric("Exercícios", int(resumo_df_base.shape[0]))

    st.divider()

    with st.expander("Ver ficha do treino"):
        st.dataframe(
            resumo_df_base[["Exercicio", "Grupo", "Series", "Reps"]],
            use_container_width=True,
            hide_index=True,
        )

    with st.expander("✏️ Editar exercícios do treino"):
        st.caption("Troque exercícios, remova ou adicione novos. Exercícios duplicados não são permitidos.")

        lista_exercicios = lista_exercicios_padronizada()

        df_ficha_ativa = carregar_ficha_ativa(
            df_hist=df_hist_raw,
            cpf=cpf,
            treino_escolhido=treino_escolhido,
            resumo_df_base=resumo_df_base,
        )

        ficha_atual_df = desserializar_ficha(serializar_ficha(df_ficha_ativa))
        nova_ficha: list[dict] = []

        for i, ex in enumerate(ficha_atual_df.to_dict("records")):
            col1, col2, col3, col4 = st.columns([3, 1, 1, 1])

            exercicio_atual = ex["Exercicio"]
            if exercicio_atual in lista_exercicios:
                idx_padrao = lista_exercicios.index(exercicio_atual)
            else:
                idx_padrao = 0

            with col1:
                exercicio = st.selectbox(
                    "Exercício",
                    lista_exercicios,
                    index=idx_padrao,
                    key=f"edit_ex_{i}",
                )

            with col2:
                series = st.number_input(
                    "Séries",
                    min_value=0,
                    max_value=MAX_SERIES_VALIDACAO,
                    step=1,
                    value=clamp_int(to_int(ex.get("Series", 0), 0), 0, MAX_SERIES_VALIDACAO),
                    key=f"edit_series_{i}",
                )

            with col3:
                reps = st.number_input(
                    "Reps",
                    min_value=0,
                    max_value=MAX_REPS_VALIDACAO,
                    step=1,
                    value=clamp_int(to_int(ex.get("Reps", 0), 0), 0, MAX_REPS_VALIDACAO),
                    key=f"edit_reps_{i}",
                )

            with col4:
                remover = st.checkbox("Excluir", key=f"del_{i}")

            if not remover:
                nova_ficha.append(
                    {
                        "Exercicio": exercicio,
                        "Grupo": inferir_grupo(exercicio),
                        "Series": series,
                        "Reps": reps,
                    }
                )

        st.divider()

        col_add, col_save = st.columns(2)

        with col_add:
            if st.button("➕ Adicionar exercício"):
                df_temp = desserializar_ficha(nova_ficha)
                existentes = set(df_temp["Exercicio"].astype(str).apply(normalizar_nome_exercicio).tolist())

                ex_novo = None
                for candidato in lista_exercicios:
                    if normalizar_nome_exercicio(candidato) not in existentes:
                        ex_novo = candidato
                        break

                if ex_novo is None:
                    st.warning("Todos os exercícios disponíveis já estão na ficha.")
                else:
                    nova_ficha.append(
                        {
                            "Exercicio": ex_novo,
                            "Grupo": inferir_grupo(ex_novo),
                            "Series": 3,
                            "Reps": 10,
                        }
                    )
                    salvar_ficha_em_session(desserializar_ficha(nova_ficha))
                    st.rerun()

        with col_save:
            if st.button("💾 Salvar ficha"):
                df_salvar = desserializar_ficha(nova_ficha)
                ok, erros, df_salvar = validar_ficha(df_salvar)

                if not ok:
                    exibir_erros_lista(erros)
                else:
                    try:
                        salvar_ficha_usuario(cpf, treino_escolhido, df_salvar.to_dict("records"))
                        salvar_ficha_em_session(df_salvar)
                        st.success("Ficha atualizada com sucesso.")
                        clear_cached_data()
                        st.rerun()
                    except Exception as e:
                        st.error("Erro ao salvar ficha.")
                        st.exception(e)

        salvar_ficha_em_session(desserializar_ficha(nova_ficha))

    st.caption(
        "A tela mostra o necessário: último peso, média, melhor peso e sugestão de próxima carga."
    )

    df_treino_ativo = carregar_ficha_ativa(
        df_hist=df_hist_raw,
        cpf=cpf,
        treino_escolhido=treino_escolhido,
        resumo_df_base=resumo_df_base,
    )

    with st.form("form_registro"):
        for i, row in df_treino_ativo.reset_index(drop=True).iterrows():
            exercicio = str(row.get("Exercicio", "")).strip()
            if not exercicio:
                continue

            grupo_salvar = str(row.get("Grupo", "")).strip() if str(row.get("Grupo", "")).strip() else inferir_grupo(exercicio)
            series_padrao = clamp_int(to_int(row.get("Series", 0), 0), 0, MAX_SERIES_VALIDACAO)
            reps_padrao = clamp_int(to_int(row.get("Reps", 0), 0), 0, MAX_REPS_VALIDACAO)

            resumo_ex = resumo_exercicio_cache(cache_hist_exercicios, exercicio)

            media_txt = fmt_kg(resumo_ex["media_peso"])
            melhor_txt = fmt_kg(resumo_ex["melhor_peso"])
            prevista_txt = fmt_kg(resumo_ex["carga_prevista"])

            ultimo_peso = resumo_ex["ultimo_peso"]
            ultimo_reps = resumo_ex["ultimo_reps"]
            ultimo_treino_texto = "-"

            if resumo_ex["ultima"] is not None:
                p = to_float(ultimo_peso, 0.0)
                r = to_int(ultimo_reps, 0)
                if p > 0 or r > 0:
                    ultimo_treino_texto = f"{p:.1f} kg x {r}"
                else:
                    ultimo_treino_texto = "-"

            st.markdown(f"### {exercicio}")

            info_cols = st.columns(5)
            with info_cols[0]:
                st.caption(f"Grupo: {grupo_salvar}")
            with info_cols[1]:
                st.caption(f"Último treino: {ultimo_treino_texto}")
            with info_cols[2]:
                st.caption(f"Média: {media_txt}")
            with info_cols[3]:
                st.caption(f"Melhor: {melhor_txt}")
            with info_cols[4]:
                st.caption(f"Sugestão: {prevista_txt}")

            entrada_cols = st.columns([1, 1, 1, 2])

            with entrada_cols[0]:
                st.number_input(
                    f"Séries - {exercicio}",
                    min_value=0,
                    max_value=MAX_SERIES_VALIDACAO,
                    step=1,
                    value=series_padrao,
                    key=f"series_{i}",
                )

            with entrada_cols[1]:
                st.number_input(
                    f"Reps - {exercicio}",
                    min_value=0,
                    max_value=MAX_REPS_VALIDACAO,
                    step=1,
                    value=reps_padrao,
                    key=f"reps_{i}",
                )

            with entrada_cols[2]:
                valor_inicial_peso = (
                    resumo_ex["carga_prevista"]
                    if resumo_ex["carga_prevista"] is not None
                    else to_float(ultimo_peso, 0.0)
                )
                valor_inicial_peso = clamp_float(valor_inicial_peso, 0.0, MAX_PESO_VALIDACAO)

                st.number_input(
                    f"Peso - {exercicio}",
                    min_value=0.0,
                    max_value=MAX_PESO_VALIDACAO,
                    step=NUMERIC_STEP_PESO,
                    value=float(valor_inicial_peso),
                    key=f"peso_{i}",
                )

            with entrada_cols[3]:
                st.text_input(
                    f"Observação - {exercicio}",
                    placeholder="Opcional",
                    key=f"obs_{i}",
                )

            st.divider()

        calorias = st.number_input(
            "Calorias gastas no treino",
            min_value=0,
            max_value=5000,
            step=1,
            value=0,
        )
        obs_geral = st.text_input("Observação geral do treino", placeholder="Opcional")
        salvar = st.form_submit_button("Salvar treino")

    if salvar:
        garantir_colunas_historico()

        try:
            registros_para_salvar = montar_registros_treino_para_salvar(
                cpf=cpf,
                treino_escolhido=treino_escolhido,
                df_treino_ativo=df_treino_ativo,
                obs_geral=obs_geral,
                calorias=int(calorias),
            )

            if not registros_para_salvar:
                st.warning("Nada foi preenchido para salvar.")
            else:
                registrar_series_em_lote(registros_para_salvar)
                st.success(
                    f"Treino salvo com sucesso. {len(registros_para_salvar)} registro(s) enviado(s) para o histórico."
                )
                clear_cached_data()
                limpar_session_widgets_registro()
                st.rerun()
        except Exception as e:
            st.error("Não consegui gravar no histórico.")
            st.exception(e)


# ============================================================
# PÁGINA PROGRESSO
# ============================================================
elif pagina == "Progresso":
    st.title("📈 Progresso geral")

    if df_hist_paciente.empty:
        st.info("Esse paciente ainda não possui histórico.")
        st.stop()

    df, info = preparar_df_progresso(df_hist_paciente)

    if df.empty:
        st.error("O histórico não possui registros de treino válidos para análise.")
        st.stop()

    # RECORDES
    st.subheader("💪 Recordes pessoais")
    pr, ultimos = detectar_prs(df)

    if pr.empty:
        st.info("Ainda não há recordes para exibir.")
    else:
        st.dataframe(pr, use_container_width=True)

        for _, row in ultimos.iterrows():
            exercicio = row["Exercicio"]
            peso = float(row["PesoNum"])
            if exercicio in pr.index:
                pr_ex = float(pr.loc[exercicio, "PR (kg)"])
                if peso >= pr_ex:
                    st.success(f"🔥 Novo recorde em {exercicio}: {peso:.1f} kg")

    st.divider()

    # SCORE GERAL DE FORÇA
    st.subheader("🏁 Score geral de força")
    score, df_prog = score_geral_forca(df)

    if df_prog.empty:
        st.info("Ainda não há dados suficientes para calcular o score geral de força.")
    else:
        c1, c2 = st.columns([1, 2])
        with c1:
            st.metric("Score geral", fmt_pct(score))
        with c2:
            st.dataframe(df_prog, use_container_width=True, hide_index=True)

    st.divider()

    # TENDÊNCIA + CARGA SUGERIDA + PREVISÃO DE PR
    st.subheader("🧠 Análise com sklearn")
    df_ml = analise_ml_exercicios(df)
    if df_ml.empty:
        st.info("Sem dados suficientes para análise preditiva.")
    else:
        st.dataframe(df_ml, use_container_width=True, hide_index=True)

    st.divider()

    # PLATÔ
    st.subheader("🧱 Detecção de platô")
    df_plato = detectar_plato(df)
    if df_plato.empty:
        st.success("Nenhum platô detectado.")
    else:
        st.dataframe(df_plato, use_container_width=True, hide_index=True)

    st.divider()

    # VOLUME
    st.subheader("🏋️ Volume por exercício")
    vol_ex = volume_por_exercicio(df)
    if vol_ex.empty:
        st.info("Sem volume para exibir.")
    else:
        st.bar_chart(vol_ex)

    st.subheader("🧩 Volume por grupo muscular")
    vol_grupo = volume_por_grupo(df)
    if vol_grupo.empty:
        st.info("Sem volume por grupo para exibir.")
    else:
        st.bar_chart(vol_grupo)

    st.divider()

    # EVOLUÇÃO SEMANAL
    st.subheader("📅 Evolução semanal")
    semanal = evolucao_semanal(df)

    if semanal.empty:
        st.info("Sem dados semanais.")
    else:
        st.dataframe(semanal, use_container_width=True, hide_index=True)

        st.markdown("**Volume total por semana**")
        st.line_chart(semanal.set_index("semana_texto")["Volume_Total"])

        st.markdown("**Carga média por semana**")
        st.line_chart(semanal.set_index("semana_texto")["Carga_Média"])

        st.markdown("**1RM médio por semana**")
        st.line_chart(semanal.set_index("semana_texto")["RM_Médio"])

        st.markdown("**Sessões por semana**")
        st.line_chart(semanal.set_index("semana_texto")["Sessões"])

    st.divider()

    # SOBRECARGA / OVERTRAINING
    st.subheader("⚠️ Risco de overtraining")
    alertas = alertas_overtraining(df)

    if alertas:
        for alerta in alertas:
            st.warning(alerta)
    else:
        st.success("Nenhum sinal importante de overtraining.")

    st.divider()

    # RECOMENDAÇÃO DE DELOAD
    st.subheader("🛌 Recomendação de deload")
    precisa_deload, motivos_deload = recomendacao_deload(df)

    if precisa_deload:
        st.warning("Recomendação: considerar uma semana de deload.")
        for motivo in motivos_deload:
            st.write(f"- {motivo}")
    else:
        st.success("Sem necessidade clara de deload no momento.")

    st.divider()

    # EVOLUÇÃO DE EXERCÍCIO
    st.subheader("📊 Evolução de carga por exercício")
    exercicios = sorted(df["Exercicio"].dropna().unique().tolist())

    if exercicios:
        ex_sel = st.selectbox("Escolha o exercício", exercicios)
        df_ex = df[df["Exercicio"] == ex_sel].sort_values("DataParsed")

        if not df_ex.empty:
            evolucao = df_ex[["DataParsed", "PesoNum"]].copy()
            evolucao = evolucao.rename(columns={"PesoNum": "Peso"})
            st.line_chart(evolucao.set_index("DataParsed"))

            tabela_ex = df_ex[
                ["DataParsed", "Treino", "PesoNum", "RepsNum", "SeriesNum", "VolumeCalcSafe", "RMCalcSafe"]
            ].copy()
            tabela_ex = tabela_ex.rename(
                columns={
                    "DataParsed": "Data",
                    "PesoNum": "Peso",
                    "RepsNum": "Reps",
                    "SeriesNum": "Series",
                    "VolumeCalcSafe": "Volume",
                    "RMCalcSafe": "1RM",
                }
            )
            tabela_ex["Data"] = tabela_ex["Data"].apply(formatar_data_br)
            st.dataframe(tabela_ex.sort_values("Data", ascending=False), use_container_width=True, hide_index=True)

    st.divider()

    # FREQUÊNCIA
    st.subheader("📆 Frequência de treino")
    freq = frequencia_treino(df)
    if freq.empty:
        st.info("Sem frequência para exibir.")
    else:
        st.bar_chart(freq.set_index("semana_texto")["Treinos"])

    st.divider()

    # CALORIAS
    st.subheader("🔥 Calorias registradas")
    df_cal = filtrar_linhas_caloria(df_hist_paciente)

    if df_cal.empty:
        st.info("Nenhum registro de calorias encontrado.")
    else:
        df_cal = df_cal.copy()
        df_cal["Data"] = df_cal["DataParsed"].apply(formatar_data_br)
        df_cal["Calorias"] = df_cal["PesoNum"]
        tabela_cal = df_cal[["Data", "Treino", "Calorias"]].sort_values("Data", ascending=False)
        st.dataframe(tabela_cal, use_container_width=True, hide_index=True)

    st.divider()

    # HISTÓRICO COMPLETO
    st.subheader("📋 Histórico completo")
    hist_exibir = dataframe_para_exibicao_historico(df.sort_values("DataParsed", ascending=False).copy())
    st.dataframe(hist_exibir, use_container_width=True, hide_index=True)


# ============================================================
# RODAPÉ
# ============================================================
st.divider()
st.caption(
    "Versão refeita com correções de data, histórico, frequência, cache por exercício, ficha personalizada, 1RM, volume, "
    "score de força, detecção de platô e análises de progressão."
)









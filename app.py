from datetime import datetime
from typing import Optional

import gspread
import numpy as np
import pandas as pd
import streamlit as st
from gspread import Worksheet
from sklearn.linear_model import LinearRegression

SPREADSHEET_ID = "1KuD14Kcma6Ze5nIxPT_MHIECu03kcN9tAL_clGPCKcU"
SHEET_PATIENTS = "Pacientes"
SHEET_TREINO_A = "Treino A"
SHEET_TREINO_B = "Treino B"
SHEET_TREINO_C = "Treino C"
SHEET_HISTORICO = "Historico"

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
    "supino": "Peito",
    "supino reto": "Peito",
    "supino inclinado": "Peito",
    "supino declinado": "Peito",
    "supino halteres": "Peito",
    "supino barra": "Peito",
    "crucifixo": "Peito",
    "crucifixo reto": "Peito",
    "crucifixo inclinado": "Peito",
    "crucifixo máquina": "Peito",
    "peck deck": "Peito",
    "voador": "Peito",
    "flexão": "Peito",
    "flexao": "Peito",

    "barra fixa": "Costas",
    "barra": "Costas",
    "puxada": "Costas",
    "puxada frente": "Costas",
    "puxada atrás": "Costas",
    "puxada alta": "Costas",
    "pulldown": "Costas",
    "remada": "Costas",
    "remada curvada": "Costas",
    "remada baixa": "Costas",
    "remada cavalinho": "Costas",
    "remada máquina": "Costas",
    "remada unilateral": "Costas",
    "pullover": "Costas",

    "elevação lateral": "Ombro",
    "elevacao lateral": "Ombro",
    "elevação frontal": "Ombro",
    "elevacao frontal": "Ombro",
    "desenvolvimento": "Ombro",
    "desenvolvimento halteres": "Ombro",
    "desenvolvimento barra": "Ombro",
    "desenvolvimento máquina": "Ombro",
    "arnold press": "Ombro",
    "crucifixo invertido": "Ombro",
    "face pull": "Ombro",
    "remada alta": "Ombro",

    "rosca direta": "Bíceps",
    "rosca barra": "Bíceps",
    "rosca halteres": "Bíceps",
    "rosca alternada": "Bíceps",
    "rosca concentrada": "Bíceps",
    "rosca scott": "Bíceps",
    "rosca scott máquina": "Bíceps",
    "rosca martelo": "Bíceps",
    "rosca 21": "Bíceps",
    "rosca inversa": "Bíceps",

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

    "stiff": "Posterior",
    "peso morto": "Posterior",
    "deadlift": "Posterior",
    "mesa flexora": "Posterior",
    "flexora": "Posterior",
    "flexora em pé": "Posterior",
    "flexora sentado": "Posterior",
    "good morning": "Posterior",
    "hip hinge": "Posterior",

    "hip thrust": "Glúteo",
    "glute bridge": "Glúteo",
    "abdução": "Glúteo",
    "abducao": "Glúteo",
    "glute kickback": "Glúteo",
    "coice": "Glúteo",
    "elevação pélvica": "Glúteo",
    "elevacao pelvica": "Glúteo",

    "panturrilha": "Panturrilha",
    "panturrilha em pé": "Panturrilha",
    "panturrilha sentado": "Panturrilha",
    "panturrilha leg press": "Panturrilha",
    "panturrilha máquina": "Panturrilha",

    "abdominal": "Abdômen",
    "abdominal infra": "Abdômen",
    "abdominal supra": "Abdômen",
    "abdominal máquina": "Abdômen",
    "ab wheel": "Abdômen",
    "prancha": "Abdômen",
    "crunch": "Abdômen",
    "leg raise": "Abdômen",
    "elevação de pernas": "Abdômen",
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

st.set_page_config(page_title="Treinos", page_icon="🏋️", layout="wide")


# =========================
# GOOGLE SHEETS
# =========================
@st.cache_resource
@st.cache_resource
def get_gspread_client():
    if "gcp_service_account" in st.secrets:
        return gspread.service_account_from_dict(dict(st.secrets["gcp_service_account"]))

    return gspread.service_account(filename="credenciais.json")


@st.cache_resource
def get_spreadsheet():
    client = get_gspread_client()
    return client.open_by_key(SPREADSHEET_ID)


def get_worksheet(sheet_name: str) -> Worksheet:
    return get_spreadsheet().worksheet(sheet_name)


@st.cache_data(ttl=30)
def read_sheet(sheet_name: str) -> pd.DataFrame:
    ws = get_worksheet(sheet_name)
    values = ws.get_all_values()

    if not values:
        return pd.DataFrame()

    headers = values[0]
    rows = values[1:]
    return pd.DataFrame(rows, columns=headers)


# =========================
# UTILITÁRIOS
# =========================
def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def normalizar_texto(txt: str) -> str:
    return " ".join(str(txt).strip().lower().split())


def achar_coluna(df: pd.DataFrame, candidatos: list[str]) -> Optional[str]:
    if df.empty:
        return None

    cols = [str(c).strip() for c in df.columns]

    for c in candidatos:
        if c in cols:
            return c

    cols_lower = {c.lower(): c for c in cols}
    for c in candidatos:
        if c.lower() in cols_lower:
            return cols_lower[c.lower()]

    return None


def inferir_grupo(exercicio: str) -> str:
    ex_norm = normalizar_texto(exercicio)

    if ex_norm in MAP_GRUPOS:
        return MAP_GRUPOS[ex_norm]

    for chave, grupo in MAP_GRUPOS.items():
        if chave in ex_norm or ex_norm in chave:
            return grupo

    return "Outro"


def to_float(valor, default=0.0) -> float:
    if valor is None:
        return default
    txt = str(valor).strip().replace(",", ".")
    if txt == "":
        return default
    try:
        return float(txt)
    except Exception:
        return default


def to_int(valor, default=0) -> int:
    return int(round(to_float(valor, default)))


def calcular_volume(peso: float, reps: float, series: float) -> float:
    return float(peso) * float(reps) * float(series)


def calcular_1rm(peso: float, reps: float) -> float:
    if reps <= 0:
        return 0.0
    return float(peso) * (1 + (float(reps) / 30))


# =========================
# MODELAGEM COM SKLEARN
# =========================
def regressao_linear_sklearn(y: np.ndarray) -> tuple[float, float]:
    if len(y) < 2:
        return 0.0, float(y[-1]) if len(y) else 0.0

    x = np.arange(len(y)).reshape(-1, 1)
    model = LinearRegression()
    model.fit(x, y)

    slope = float(model.coef_[0])
    prox = float(model.predict(np.array([[len(y)]]))[0])
    return slope, prox


def tendencia_forca(y: np.ndarray) -> str:
    if len(y) < 2:
        return "Dados insuficientes"

    slope, _ = regressao_linear_sklearn(y)

    if slope > 0.05:
        return "📈 Subindo"
    if slope < -0.05:
        return "📉 Regredindo"
    return "➡️ Estagnado"


def carga_sugerida(y: np.ndarray) -> float:
    if len(y) == 0:
        return 0.0

    if len(y) == 1:
        return float(y[-1])

    _, prox = regressao_linear_sklearn(y)

    ultimo = float(y[-1])
    if prox < 0:
        prox = ultimo

    # trava simples para não sugerir salto absurdo
    limite_superior = ultimo * 1.08
    limite_inferior = ultimo * 0.92

    prox = min(prox, limite_superior)
    prox = max(prox, limite_inferior)

    # arredonda para 0.5 kg
    return round(prox * 2) / 2


# =========================
# LEITURA DAS ABAS
# =========================
def carregar_pacientes() -> pd.DataFrame:
    return normalizar_colunas(read_sheet(SHEET_PATIENTS))


def carregar_treino(nome_treino: str) -> pd.DataFrame:
    return normalizar_colunas(read_sheet(MAP_TREINOS[nome_treino]))


def carregar_historico() -> pd.DataFrame:
    return normalizar_colunas(read_sheet(SHEET_HISTORICO))


# =========================
# HISTÓRICO / MÉTRICAS
# =========================
def filtrar_historico_paciente(df_hist: pd.DataFrame, cpf: str) -> pd.DataFrame:
    if df_hist.empty:
        return df_hist

    col_cpf = achar_coluna(df_hist, POSSIVEIS_COLUNAS_CPF)
    if not col_cpf:
        return pd.DataFrame()

    df = df_hist.copy()
    df[col_cpf] = df[col_cpf].astype(str).str.strip()
    return df[df[col_cpf] == str(cpf).strip()]


def historico_exercicio(df_hist_paciente: pd.DataFrame, exercicio: str) -> pd.DataFrame:
    if df_hist_paciente.empty:
        return df_hist_paciente

    col_ex = achar_coluna(df_hist_paciente, POSSIVEIS_COLUNAS_EXERCICIO)
    if not col_ex:
        return pd.DataFrame()

    exercicio_norm = normalizar_texto(exercicio)

    df = df_hist_paciente.copy()
    df[col_ex] = df[col_ex].astype(str).apply(normalizar_texto)

    return df[df[col_ex] == exercicio_norm]


def ultima_linha_exercicio(df_hist_paciente: pd.DataFrame, exercicio: str) -> Optional[pd.Series]:
    df = historico_exercicio(df_hist_paciente, exercicio)
    if df.empty:
        return None

    col_data = achar_coluna(df, POSSIVEIS_COLUNAS_DATA)
    if col_data:
        df = df.copy()
        df[col_data] = pd.to_datetime(df[col_data], errors="coerce", dayfirst=True)
        df = df.sort_values(col_data)

    return df.iloc[-1]


def media_peso_exercicio(df_hist_paciente: pd.DataFrame, exercicio: str) -> Optional[float]:
    df = historico_exercicio(df_hist_paciente, exercicio)
    if df.empty:
        return None

    col_peso = achar_coluna(df, POSSIVEIS_COLUNAS_PESO)
    if not col_peso:
        return None

    serie = pd.to_numeric(df[col_peso], errors="coerce").dropna()
    if serie.empty:
        return None

    return float(serie.mean())


def melhor_peso_exercicio(df_hist_paciente: pd.DataFrame, exercicio: str) -> Optional[float]:
    df = historico_exercicio(df_hist_paciente, exercicio)
    if df.empty:
        return None

    col_peso = achar_coluna(df, POSSIVEIS_COLUNAS_PESO)
    if not col_peso:
        return None

    serie = pd.to_numeric(df[col_peso], errors="coerce").dropna()
    if serie.empty:
        return None

    return float(serie.max())


# =========================
# ESCRITA NO HISTÓRICO
# =========================
def garantir_colunas_historico() -> None:
    ws = get_worksheet(SHEET_HISTORICO)
    headers = ws.row_values(1)

    if headers != HIST_HEADERS:
        ws.update("A1:L1", [HIST_HEADERS])

    st.cache_data.clear()


def registrar_series_em_lote(registros: list[list]) -> None:
    if not registros:
        return
    ws = get_worksheet(SHEET_HISTORICO)
    ws.append_rows(registros, value_input_option="USER_ENTERED")
    st.cache_data.clear()

def atualizar_ficha_treino(nome_treino: str, df_editado: pd.DataFrame):

    if df_editado.empty:
        raise ValueError("A ficha não pode ficar vazia.")

    ws = get_worksheet(MAP_TREINOS[nome_treino])

    dados = [df_editado.columns.tolist()] + df_editado.astype(str).values.tolist()

    ws.clear()
    ws.update("A1", dados)

    st.cache_data.clear()

# =========================
# PREPARAÇÃO DO DF DE PROGRESSO
# =========================
def preparar_df_progresso(df_hist_paciente: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    info = {
        "col_ex": None,
        "col_peso": None,
        "col_series": None,
        "col_reps": None,
        "col_data": None,
        "col_treino": None,
        "col_grupo_hist": None,
        "col_1rm": None,
        "col_volume": None,
    }

    if df_hist_paciente.empty:
        return pd.DataFrame(), info

    df = df_hist_paciente.copy()

    info["col_ex"] = achar_coluna(df, POSSIVEIS_COLUNAS_EXERCICIO)
    info["col_peso"] = achar_coluna(df, POSSIVEIS_COLUNAS_PESO)
    info["col_series"] = achar_coluna(df, POSSIVEIS_COLUNAS_SERIES)
    info["col_reps"] = achar_coluna(df, POSSIVEIS_COLUNAS_REPS)
    info["col_data"] = achar_coluna(df, POSSIVEIS_COLUNAS_DATA)
    info["col_treino"] = achar_coluna(df, POSSIVEIS_COLUNAS_TREINO)
    info["col_grupo_hist"] = achar_coluna(df, POSSIVEIS_COLUNAS_GRUPO)
    info["col_1rm"] = achar_coluna(df, POSSIVEIS_COLUNAS_1RM)
    info["col_volume"] = achar_coluna(df, POSSIVEIS_COLUNAS_VOLUME)

    if not all([info["col_ex"], info["col_peso"], info["col_data"]]):
        return pd.DataFrame(), info

    df = df[df[info["col_ex"]].astype(str).str.strip() != "CALORIAS_TREINO"].copy()

    df[info["col_peso"]] = pd.to_numeric(df[info["col_peso"]], errors="coerce")

    if info["col_series"]:
        df[info["col_series"]] = pd.to_numeric(df[info["col_series"]], errors="coerce")
    else:
        df["Series"] = np.nan
        info["col_series"] = "Series"

    if info["col_reps"]:
        df[info["col_reps"]] = pd.to_numeric(df[info["col_reps"]], errors="coerce")
    else:
        df["Reps"] = np.nan
        info["col_reps"] = "Reps"

    df[info["col_data"]] = pd.to_datetime(df[info["col_data"]], errors="coerce", dayfirst=True)
    df = df.dropna(subset=[info["col_peso"], info["col_data"]])

    if info["col_grupo_hist"]:
        df[info["col_grupo_hist"]] = df[info["col_grupo_hist"]].astype(str).replace("", pd.NA)
        df["GrupoFinal"] = df[info["col_grupo_hist"]].fillna(df[info["col_ex"]].apply(inferir_grupo))
    else:
        df["GrupoFinal"] = df[info["col_ex"]].apply(inferir_grupo)

    if info["col_1rm"]:
        df["1RM_calc"] = pd.to_numeric(df[info["col_1rm"]], errors="coerce")
        df["1RM_calc"] = df["1RM_calc"].fillna(
            df[info["col_peso"]] * (1 + (df[info["col_reps"]].fillna(0) / 30))
        )
    else:
        df["1RM_calc"] = df[info["col_peso"]] * (1 + (df[info["col_reps"]].fillna(0) / 30))

    if info["col_volume"]:
        df["volume_calc"] = pd.to_numeric(df[info["col_volume"]], errors="coerce")
        df["volume_calc"] = df["volume_calc"].fillna(
            df[info["col_peso"]] * df[info["col_reps"]].fillna(0) * df[info["col_series"]].fillna(0)
        )
    else:
        df["volume_calc"] = df[info["col_peso"]] * df[info["col_reps"]].fillna(0) * df[info["col_series"]].fillna(0)

    df["semana_periodo"] = df[info["col_data"]].dt.to_period("W")
    df["semana_texto"] = df["semana_periodo"].astype(str)

    return df, info


# =========================
# TELA
# =========================
st.title("🏋️ App de Treino")
st.caption("Registro de treino com Google Sheets + análises com sklearn")

with st.sidebar:
    st.header("Configuração")

    pagina = st.radio("Página", ["Treino", "Progresso"], horizontal=False)
    cpf = st.text_input("CPF do paciente", placeholder="Digite o CPF")

    df_pacientes = carregar_pacientes()
    nome_paciente = ""

    col_paciente_cpf = achar_coluna(df_pacientes, POSSIVEIS_COLUNAS_CPF)
    col_paciente_nome = achar_coluna(df_pacientes, POSSIVEIS_COLUNAS_NOME)

    if cpf and not df_pacientes.empty and col_paciente_cpf and col_paciente_nome:
        resultado = df_pacientes[
            df_pacientes[col_paciente_cpf].astype(str).str.strip() == str(cpf).strip()
        ]

        if not resultado.empty:
            nome_paciente = str(resultado.iloc[0][col_paciente_nome]).strip()
            st.success(f"Paciente: {nome_paciente}")
        else:
            st.warning("CPF não encontrado")

    if pagina == "Treino":
        treino_escolhido = st.radio("Treino", list(MAP_TREINOS.keys()), horizontal=False)
    else:
        treino_escolhido = None

    atualizar = st.button("Atualizar dados")
    if atualizar:
        st.cache_data.clear()
        st.rerun()

if not cpf:
    st.warning("Informe o CPF do paciente para continuar.")
    st.stop()

try:
    df_hist = carregar_historico()
    if pagina == "Treino":
        df_treino = carregar_treino(treino_escolhido)
    else:
        df_treino = pd.DataFrame()
except Exception as e:
    st.error("Não consegui abrir a planilha. Verifique as credenciais e o compartilhamento com a conta de serviço.")
    st.exception(e)
    st.stop()

df_hist = normalizar_colunas(df_hist)
df_treino = normalizar_colunas(df_treino)
df_hist_paciente = filtrar_historico_paciente(df_hist, cpf)

# =========================
# PÁGINA TREINO
# =========================
if pagina == "Treino":
    if df_treino.empty:
        st.error(f"A aba {treino_escolhido} está vazia.")
        st.stop()

    col_exercicio = achar_coluna(df_treino, POSSIVEIS_COLUNAS_EXERCICIO)
    col_series = achar_coluna(df_treino, POSSIVEIS_COLUNAS_SERIES)
    col_reps = achar_coluna(df_treino, POSSIVEIS_COLUNAS_REPS)
    col_grupo = achar_coluna(df_treino, POSSIVEIS_COLUNAS_GRUPO)

    if not col_exercicio:
        st.error(f"A aba {treino_escolhido} precisa ter uma coluna de exercício.")
        st.stop()

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Paciente", nome_paciente if nome_paciente else cpf)
    with c2:
        st.metric("Treino atual", treino_escolhido)
    with c3:
        total_exercicios = int(
            df_treino[col_exercicio]
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .shape[0]
        )
        st.metric("Exercícios", total_exercicios)

    st.divider()

    resumo_df = df_treino.copy()
    cols_mostrar = [c for c in [col_exercicio, col_grupo, col_series, col_reps] if c]

    with st.expander("Ver ficha do treino"):
        st.dataframe(resumo_df[cols_mostrar], use_container_width=True, hide_index=True)       

    with st.expander("✏️ Editar exercícios do treino"):

        st.caption("Troque exercícios, remova ou adicione novos.")

        lista_exercicios = sorted(MAP_GRUPOS.keys())

        if "ficha_edit" not in st.session_state:
            st.session_state.ficha_edit = resumo_df[cols_mostrar].to_dict("records")

        nova_ficha = []

        for i, ex in enumerate(st.session_state.ficha_edit):

            col1, col2, col3, col4 = st.columns([3,1,1,1])

            with col1:
                exercicio = st.selectbox(
                    "Exercício",
                    lista_exercicios,
                    index=lista_exercicios.index(ex[col_exercicio]) if ex[col_exercicio] in lista_exercicios else 0,
                    key=f"edit_ex_{i}"
                )

            with col2:
                series = st.number_input(
                    "Séries",
                    min_value=0,
                    step=1,
                    value=to_int(ex.get(col_series, 0)),
                    key=f"edit_series_{i}"
                )

            with col3:
                reps = st.number_input(
                    "Reps",
                    min_value=0,
                    step=1,
                    value=to_int(ex.get(col_reps, 0)),
                    key=f"edit_reps_{i}"
                )

            with col4:
                remover = st.checkbox("Excluir", key=f"del_{i}")

            if not remover:
                nova_ficha.append({
                    col_exercicio: exercicio,
                    col_grupo: MAP_GRUPOS.get(normalizar_texto(exercicio), "Outro"),
                    col_series: series,
                    col_reps: reps
                })

        st.divider()

        if st.button("➕ Adicionar exercício"):
            nova_ficha.append({
                col_exercicio: lista_exercicios[0],
                col_grupo: MAP_GRUPOS[lista_exercicios[0]],
                col_series: 3,
                col_reps: 10
            })
            st.session_state.ficha_edit = nova_ficha
            st.rerun()
            
        if st.button("💾 Salvar ficha"):

            if len(nova_ficha) == 0:
                st.error("A ficha não pode ficar vazia.")
                st.stop()

            df_salvar = pd.DataFrame(nova_ficha)

            try:
                atualizar_ficha_treino(treino_escolhido, df_salvar)
                st.success("Ficha atualizada com sucesso.")
                st.session_state.ficha_edit = df_salvar.to_dict("records")
                st.rerun()

            except Exception as e:
                st.error("Erro ao salvar ficha.")
                st.exception(e)

        st.session_state.ficha_edit = nova_ficha
    st.caption("A tela mostra só o necessário: último peso, média, melhor peso e sugestão de próxima carga.")

    registros_para_salvar = []

    with st.form("form_registro"):
        for i, row in df_treino.reset_index(drop=True).iterrows():
            exercicio = str(row.get(col_exercicio, "")).strip()
            if not exercicio:
                continue

            grupo_ficha = str(row.get(col_grupo, "")).strip() if col_grupo else ""
            grupo_salvar = grupo_ficha if grupo_ficha else inferir_grupo(exercicio)

            series_padrao = row.get(col_series, "") if col_series else ""
            reps_padrao = row.get(col_reps, "") if col_reps else ""

            ultima = ultima_linha_exercicio(df_hist_paciente, exercicio)
            media_peso = media_peso_exercicio(df_hist_paciente, exercicio)
            melhor_peso = melhor_peso_exercicio(df_hist_paciente, exercicio)

            hist_ex = historico_exercicio(df_hist_paciente, exercicio)
            col_hist_peso = achar_coluna(hist_ex, POSSIVEIS_COLUNAS_PESO) if not hist_ex.empty else None

            ultimo_peso = ""
            ultimo_reps = ""
            carga_prevista = None

            if ultima is not None:
                if col_hist_peso and col_hist_peso in ultima.index:
                    ultimo_peso = ultima[col_hist_peso]

                col_hist_reps = achar_coluna(hist_ex, POSSIVEIS_COLUNAS_REPS)
                if col_hist_reps and col_hist_reps in ultima.index:
                    ultimo_reps = ultima[col_hist_reps]

            if not hist_ex.empty and col_hist_peso:
                serie_cargas = pd.to_numeric(hist_ex[col_hist_peso], errors="coerce").dropna().values
                if len(serie_cargas) >= 2:
                    carga_prevista = carga_sugerida(serie_cargas)

            media_txt = "-" if media_peso is None else f"{media_peso:.1f} kg"
            melhor_txt = "-" if melhor_peso is None else f"{melhor_peso:.1f} kg"
            prevista_txt = "-" if carga_prevista is None else f"{carga_prevista:.1f} kg"

            st.markdown(f"### {exercicio}")

            info_cols = st.columns(5)
            with info_cols[0]:
                st.caption(f"Grupo: {grupo_salvar}")
            with info_cols[1]:
                st.caption(f"Último treino: {ultimo_peso} kg x {ultimo_reps}")
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
                    step=1,
                    value=to_int(series_padrao, 0),
                    key=f"series_{i}",
                )

            with entrada_cols[1]:
                st.number_input(
                    f"Reps - {exercicio}",
                    min_value=0,
                    step=1,
                    value=to_int(reps_padrao, 0),
                    key=f"reps_{i}",
                )

            with entrada_cols[2]:
                valor_inicial_peso = carga_prevista if carga_prevista is not None else to_float(ultimo_peso, 0.0)
                st.number_input(
                    f"Peso - {exercicio}",
                    min_value=0.0,
                    step=0.5,
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

        calorias = st.number_input("Calorias gastas no treino", min_value=0, step=1, value=0)
        obs_geral = st.text_input("Observação geral do treino", placeholder="Opcional")
        salvar = st.form_submit_button("Salvar treino")

    if salvar:
        garantir_colunas_historico()
        data_dt = datetime.now()
        data_agora = data_dt.strftime("%d/%m/%Y %H:%M:%S")
        semana = int(data_dt.isocalendar()[1])

        for i, row in df_treino.reset_index(drop=True).iterrows():
            exercicio = str(row.get(col_exercicio, "")).strip()
            if not exercicio:
                continue

            grupo_ficha = str(row.get(col_grupo, "")).strip() if col_grupo else ""
            grupo_salvar = grupo_ficha if grupo_ficha else inferir_grupo(exercicio)

            series_feitas = to_int(st.session_state.get(f"series_{i}", 0), 0)
            reps_feitas = to_int(st.session_state.get(f"reps_{i}", 0), 0)
            peso_usado = to_float(st.session_state.get(f"peso_{i}", 0.0), 0.0)
            obs = str(st.session_state.get(f"obs_{i}", "")).strip()

            if series_feitas or reps_feitas or peso_usado or obs:
                volume = calcular_volume(peso_usado, reps_feitas, series_feitas)
                rm_estimado = calcular_1rm(peso_usado, reps_feitas)

                obs_final = obs
                if str(obs_geral).strip():
                    obs_final = f"{obs_final} | {str(obs_geral).strip()}".strip(" |")

                registros_para_salvar.append([
                    data_agora,
                    str(cpf).strip(),
                    semana,
                    treino_escolhido,
                    exercicio,
                    grupo_salvar,
                    peso_usado,
                    reps_feitas,
                    series_feitas,
                    volume,
                    rm_estimado,
                    obs_final,
                ])

        if calorias > 0:
            registros_para_salvar.append([
                data_agora,
                str(cpf).strip(),
                semana,
                treino_escolhido,
                "CALORIAS_TREINO",
                "Cardio",
                calorias,
                "",
                "",
                "",
                "",
                str(obs_geral).strip(),
            ])

        if not registros_para_salvar:
            st.warning("Nada foi preenchido para salvar.")
        else:
            try:
                registrar_series_em_lote(registros_para_salvar)
                st.success(f"Treino salvo com sucesso. {len(registros_para_salvar)} registro(s) enviado(s) para o histórico.")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error("Não consegui gravar no histórico.")
                st.exception(e)

# =========================
# PÁGINA PROGRESSO
# =========================
elif pagina == "Progresso":
    st.title("📈 Progresso geral")

    if df_hist_paciente.empty:
        st.info("Esse paciente ainda não possui histórico.")
        st.stop()

    df, info = preparar_df_progresso(df_hist_paciente)

    if df.empty:
        st.error("O histórico não possui as colunas mínimas necessárias.")
        st.stop()

    col_ex = info["col_ex"]
    col_peso = info["col_peso"]
    col_data = info["col_data"]

    # RECORDES
    st.subheader("💪 Recordes pessoais")
    pr = (
        df.groupby(col_ex)[col_peso]
        .max()
        .sort_values(ascending=False)
        .to_frame("PR (kg)")
    )
    st.dataframe(pr, use_container_width=True)

    ultimos = df.sort_values(col_data).groupby(col_ex).tail(1)
    for _, row in ultimos.iterrows():
        exercicio = row[col_ex]
        peso = float(row[col_peso])
        if exercicio in pr.index:
            pr_ex = float(pr.loc[exercicio, "PR (kg)"])
            if peso >= pr_ex:
                st.success(f"🔥 Novo recorde em {exercicio}: {peso:.1f} kg")

    st.divider()

    # SCORE GERAL DE FORÇA
    st.subheader("🏁 Score geral de força")
    progressao = []

    for ex in sorted(df[col_ex].dropna().unique()):
        dados = df[df[col_ex] == ex].sort_values(col_data)
        if len(dados) < 2:
            continue

        primeiro = float(dados.iloc[0][col_peso])
        ultimo = float(dados.iloc[-1][col_peso])

        if primeiro <= 0:
            continue

        pct = ((ultimo - primeiro) / primeiro) * 100
        progressao.append({
            "Exercício": ex,
            "Progressão (%)": round(pct, 2)
        })

    if progressao:
        df_prog = pd.DataFrame(progressao).sort_values("Progressão (%)", ascending=False)
        score = float(df_prog["Progressão (%)"].mean())
        c1, c2 = st.columns([1, 2])
        with c1:
            st.metric("Score geral", f"{score:.2f}%")
        with c2:
            st.dataframe(df_prog, use_container_width=True, hide_index=True)
    else:
        st.info("Ainda não há dados suficientes para calcular o score geral de força.")

    st.divider()

    # TENDÊNCIA + CARGA SUGERIDA + PREVISÃO DE PR
    st.subheader("🧠 Análise com sklearn")
    analise_ml = []

    for ex in sorted(df[col_ex].dropna().unique()):
        dados = df[df[col_ex] == ex].sort_values(col_data)
        y = dados[col_peso].astype(float).dropna().values

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

        analise_ml.append({
            "Exercício": ex,
            "Tendência": trend,
            "Próxima carga sugerida": round(sugestao, 1),
            "PR atual": round(pr_atual, 1),
            "PR previsto": round(pr_previsto, 1),
        })

    if analise_ml:
        st.dataframe(pd.DataFrame(analise_ml), use_container_width=True, hide_index=True)

    st.divider()

    # PLATÔ
    st.subheader("🧱 Detecção de platô")
    plato = []

    for ex in sorted(df[col_ex].dropna().unique()):
        dados = df[df[col_ex] == ex].sort_values(col_data)
        if len(dados) < 4:
            continue

        ultimos_4 = dados.tail(4)[col_peso].astype(float).round(2).tolist()
        if len(set(ultimos_4)) == 1:
            plato.append({
                "Exercício": ex,
                "Status": f"⚠️ Estagnado há {len(ultimos_4)} treinos"
            })

    if plato:
        st.dataframe(pd.DataFrame(plato), use_container_width=True, hide_index=True)
    else:
        st.success("Nenhum platô detectado.")

    st.divider()

    # VOLUME
    st.subheader("🏋️ Volume por exercício")
    vol_ex = (
        df.groupby(col_ex)["volume_calc"]
        .sum()
        .sort_values(ascending=False)
    )
    st.bar_chart(vol_ex)

    st.subheader("🧩 Volume por grupo muscular")
    vol_grupo = (
        df.groupby("GrupoFinal")["volume_calc"]
        .sum()
        .sort_values(ascending=False)
    )
    st.bar_chart(vol_grupo)

    st.divider()

    # EVOLUÇÃO SEMANAL
    st.subheader("📅 Evolução semanal")
    semanal = (
        df.groupby("semana_texto")
        .agg(
            Volume_Total=("volume_calc", "sum"),
            Carga_Média=(col_peso, "mean"),
            RM_Médio=("1RM_calc", "mean"),
            Sessões=(col_ex, "count"),
        )
        .reset_index()
    )

    if not semanal.empty:
        st.dataframe(semanal, use_container_width=True, hide_index=True)

        st.markdown("**Volume total por semana**")
        st.line_chart(semanal.set_index("semana_texto")["Volume_Total"])

        st.markdown("**Carga média por semana**")
        st.line_chart(semanal.set_index("semana_texto")["Carga_Média"])

        st.markdown("**1RM médio por semana**")
        st.line_chart(semanal.set_index("semana_texto")["RM_Médio"])

    st.divider()

    # SOBRECARGA / OVERTRAINING
    st.subheader("⚠️ Risco de overtraining")
    alertas = []

    vol_semana = (
        df.groupby("semana_texto")["volume_calc"]
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

    for ex in sorted(df[col_ex].dropna().unique()):
        dados = df[df[col_ex] == ex].sort_values(col_data)
        if len(dados) < 3:
            continue

        ultimos_3 = dados.tail(3).copy()
        media_volume = float(ultimos_3["volume_calc"].mean())
        volume_atual = float(ultimos_3.iloc[-1]["volume_calc"])

        y = ultimos_3[col_peso].astype(float).values
        trend = tendencia_forca(y)

        if media_volume > 0 and volume_atual > media_volume * 1.3 and trend == "📉 Regredindo":
            alertas.append(f"{ex}: volume recente alto com queda de desempenho.")

    if alertas:
        for alerta in alertas:
            st.warning(alerta)
    else:
        st.success("Nenhum sinal importante de overtraining.")

    st.divider()

    # RECOMENDAÇÃO DE DELOAD
    st.subheader("🛌 Recomendação de deload")
    precisa_deload = False
    motivos_deload = []

    if len(vol_semana) >= 2:
        ultima = float(vol_semana.iloc[-1])
        anterior = float(vol_semana.iloc[-2])

        if anterior > 0:
            aumento = ((ultima - anterior) / anterior) * 100
            if aumento > 30:
                precisa_deload = True
                motivos_deload.append(f"Volume semanal aumentou {aumento:.1f}%.")

    regressao_count = 0
    for ex in sorted(df[col_ex].dropna().unique()):
        dados = df[df[col_ex] == ex].sort_values(col_data)
        y = dados[col_peso].astype(float).dropna().values
        if len(y) >= 3 and tendencia_forca(y[-3:]) == "📉 Regredindo":
            regressao_count += 1

    if regressao_count >= 3:
        precisa_deload = True
        motivos_deload.append("Há regressão recente em vários exercícios.")

    if precisa_deload:
        st.warning("Recomendação: considerar uma semana de deload.")
        for motivo in motivos_deload:
            st.write(f"- {motivo}")
    else:
        st.success("Sem necessidade clara de deload no momento.")

    st.divider()

    # EVOLUÇÃO DE EXERCÍCIO
    st.subheader("📊 Evolução de carga por exercício")
    exercicios = sorted(df[col_ex].dropna().unique().tolist())
    ex_sel = st.selectbox("Escolha o exercício", exercicios)

    df_ex = df[df[col_ex] == ex_sel].sort_values(col_data)
    if not df_ex.empty:
        evolucao = df_ex[[col_data, col_peso]].copy()
        st.line_chart(evolucao.set_index(col_data))

    st.divider()

    # FREQUÊNCIA
    st.subheader("📆 Frequência de treino")
    freq = df.groupby("semana_texto").size().to_frame("Treinos")
    st.bar_chart(freq)

    st.divider()

    # HISTÓRICO COMPLETO
    st.subheader("📋 Histórico completo")
    hist = df.sort_values(col_data, ascending=False).copy()
    st.dataframe(hist, use_container_width=True, hide_index=True)

st.divider()

st.caption("Versão refeita com sklearn, volume, 1RM, progressive overload, overtraining, score de força e deload.")

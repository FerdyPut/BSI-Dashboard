import streamlit as st
import pandas as pd
from pathlib import Path
import duckdb

# =====================================
# CONFIG
# =====================================
PARQUET_DIR = Path("data/parquet")
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

st.set_page_config(page_title="Excel → Parquet Viewer", layout="wide")

# =====================================
# APP
# =====================================
def app():

    tab1, tab2 = st.tabs(["📥 Import Excel", "👀 View Parquet"])

    # ==================================================
    # TAB 1 — IMPORT EXCEL
    # ==================================================
    with tab1:
        st.subheader("Upload Excel (Multi File, Multi Sheet)")

        uploaded_files = st.file_uploader(
            "Upload file Excel",
            type=["xlsx", "xls", "xlsb"],
            accept_multiple_files=True
        )

        if uploaded_files:
            for uploaded in uploaded_files:
                st.markdown(f"### 📄 {uploaded.name}")

                # ambil daftar sheet
                xls = pd.ExcelFile(uploaded)
                sheet = st.selectbox(
                    f"Pilih sheet untuk {uploaded.name}",
                    xls.sheet_names,
                    key=uploaded.name
                )

                if st.button(f"▶ Generate Parquet ({uploaded.name})"):
                    df = pd.read_excel(uploaded, sheet_name=sheet)

                    # =============================
                    # 🔒 SAFETY LAYER (WAJIB)
                    # =============================
                    df = df.astype("string")  # SEMUA STRING → ANTI MIXED TYPE

                    output = PARQUET_DIR / f"{uploaded.name}_{sheet}.parquet"
                    df.to_parquet(output, index=False)

                    st.success(f"✅ Saved: {output.name}")

    # ==================================================
    # TAB 2 — VIEW PARQUET
    # ==================================================
    with tab2:
        st.subheader("View Parquet Data")

        parquet_files = sorted(PARQUET_DIR.glob("*.parquet"))

        if not parquet_files:
            st.warning("⚠️ Belum ada file Parquet.")
            st.stop()

        selected = st.selectbox(
            "Pilih file Parquet",
            parquet_files,
            format_func=lambda x: x.name
        )

        @st.cache_data
        def load_parquet(path):
            return duckdb.query(
                f"SELECT * FROM '{path}' LIMIT 1000"
            ).df()

        df_view = load_parquet(selected)

        st.caption("Preview 1000 baris pertama")
        st.dataframe(df_view, use_container_width=True)

        st.caption("Schema")
        st.code(
            duckdb.query(
                f"DESCRIBE SELECT * FROM '{selected}'"
            ).df()
        )

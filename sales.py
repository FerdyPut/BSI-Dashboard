import streamlit as st
import pandas as pd
from pathlib import Path
import duckdb
import uuid

PARQUET_DIR = Path("data/parquet/sales")
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

st.set_page_config(page_title="Excel → Parquet", layout="wide")

def sales():

    tab1, tab2 = st.tabs(["📥 Import Excel", "📊 View Data"])

    # =====================================
    # TAB 1 — IMPORT
    # =====================================
    with tab1:
        st.subheader("Upload Excel → Select Sheet → Append All")

        uploaded_files = st.file_uploader(
            "Upload Excel",
            type=["xlsx", "xls", "xlsb"],
            accept_multiple_files=True
        )

        # simpan pilihan sheet
        if "sheet_map" not in st.session_state:
            st.session_state.sheet_map = {}

        if uploaded_files:
            for uploaded in uploaded_files:
                xls = pd.ExcelFile(uploaded)

                sheet = st.selectbox(
                    f"Sheet untuk {uploaded.name}",
                    xls.sheet_names,
                    key=f"sheet_{uploaded.name}"
                )

                st.session_state.sheet_map[uploaded.name] = {
                    "file": uploaded,
                    "sheet": sheet
                }

            st.divider()

            if st.button("🚀 Append ALL Files"):
                for meta in st.session_state.sheet_map.values():
                    df = pd.read_excel(meta["file"], sheet_name=meta["sheet"])

                    # 🔒 SAFE MODE
                    df = df.astype("string")

                    # metadata (opsional)
                    df["_source_file"] = meta["file"].name
                    df["_source_sheet"] = meta["sheet"]

                    output = PARQUET_DIR / f"part-{uuid.uuid4().hex}.parquet"
                    df.to_parquet(output, index=False)

                st.success("✅ Semua file berhasil di-append ke dataset")

                # reset pilihan
                st.session_state.sheet_map = {}

    # =====================================
    # TAB 2 — VIEW
    # =====================================
    with tab2:
        st.subheader("Parquet Dataset Preview")

        parquet_files = list(PARQUET_DIR.glob("*.parquet"))
        if not parquet_files:
            st.warning("⚠️ Dataset kosong")
            st.stop()

        @st.cache_data
        def preview():
            return duckdb.query(
                f"SELECT * FROM '{PARQUET_DIR}/*.parquet' LIMIT 1000"
            ).df()

        df = preview()
        st.dataframe(df, use_container_width=True)

        st.caption("Schema")
        st.code(
            duckdb.query(
                f"DESCRIBE SELECT * FROM '{PARQUET_DIR}/*.parquet'"
            ).df()
        )


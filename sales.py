import streamlit as st
import pandas as pd
from pathlib import Path
import duckdb
import uuid

PARQUET_DIR = Path("data/parquet/sales")
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

def sales():

    tab1, tab2 = st.tabs(["📥 Import Excel", "📊 View Data"])

    # ========================
    # TAB 1 — IMPORT
    # ========================
    with tab1:
        st.subheader("Upload Excel → One Parquet Dataset")

        uploaded_files = st.file_uploader(
            "Upload Excel",
            type=["xlsx", "xls", "xlsb"],
            accept_multiple_files=True
        )

        if uploaded_files:
            for uploaded in uploaded_files:
                xls = pd.ExcelFile(uploaded)

                sheet = st.selectbox(
                    f"Pilih sheet ({uploaded.name})",
                    xls.sheet_names,
                    key=uploaded.name
                )

                if st.button(f"▶ Append ({uploaded.name})"):
                    df = pd.read_excel(uploaded, sheet_name=sheet)

                    # 🔒 SAFE MODE
                    df = df.astype("string")

                    # metadata (opsional tapi berguna)
                    df["_source_file"] = uploaded.name
                    df["_source_sheet"] = sheet

                    # append sebagai part file
                    output = PARQUET_DIR / f"part-{uuid.uuid4().hex}.parquet"
                    df.to_parquet(output, index=False)

                    st.success(f"✅ Appended: {output.name}")

    # ========================
    # TAB 2 — VIEW
    # ========================
    with tab2:
        st.subheader("Sales Dataset")

        parquet_files = list(PARQUET_DIR.glob("*.parquet"))
        if not parquet_files:
            st.warning("⚠️ Dataset masih kosong")
            st.stop()

        @st.cache_data
        def load_preview():
            return duckdb.query(
                f"SELECT * FROM '{PARQUET_DIR}/*.parquet' LIMIT 1000"
            ).df()

        df = load_preview()
        st.dataframe(df, use_container_width=True)

        st.caption("Schema")
        st.code(
            duckdb.query(
                f"DESCRIBE SELECT * FROM '{PARQUET_DIR}/*.parquet'"
            ).df()
        )


import streamlit as st
import pandas as pd
from pathlib import Path
import duckdb
import uuid
import tempfile

# =====================================
# CONFIG
# =====================================
PARQUET_DIR = Path("data/parquet/sales")
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

st.set_page_config(page_title="Excel → Parquet Dataset", layout="wide")

# =====================================
# APP
# =====================================
def sales():

    tab1, tab2 = st.tabs(["📥 Import Excel", "📊 View & Download"])

    # ==================================================
    # TAB 1 — IMPORT EXCEL
    # ==================================================
    with tab1:
        st.subheader("Upload Excel → Pilih Sheet → Append ALL")

        uploaded_files = st.file_uploader(
            "Upload file Excel",
            type=["xlsx", "xls", "xlsb", "csv"],
            accept_multiple_files=True
        )

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

                    # 🔒 SAFE MODE (ANTI MIXED TYPE)
                    df = df.astype("string")

                    # metadata
                    df["_source_file"] = meta["file"].name
                    df["_source_sheet"] = meta["sheet"]

                    output = PARQUET_DIR / f"part-{uuid.uuid4().hex}.parquet"
                    df.to_parquet(output, index=False)

                st.success("✅ Semua file berhasil di-append ke dataset")
                st.session_state.sheet_map = {}

    # ==================================================
    # TAB 2 — VIEW & DOWNLOAD
    # ==================================================
    with tab2:
        st.subheader("Parquet Dataset")

        parquet_files = list(PARQUET_DIR.glob("*.parquet"))
        if not parquet_files:
            st.warning("⚠️ Dataset masih kosong")
            st.stop()

        con = duckdb.connect(database=":memory:")

        # ==========================
        # INFO TOTAL BARIS
        # ==========================
        total_rows = con.execute(
            f"SELECT COUNT(*) FROM '{PARQUET_DIR}/*.parquet'"
        ).fetchone()[0]

        st.metric("📊 Total Rows (All Data)", f"{total_rows:,}")

        # ==========================
        # PREVIEW DATA
        # ==========================
        @st.cache_data
        def preview():
            return con.execute(
                f"SELECT * FROM '{PARQUET_DIR}/*.parquet' LIMIT 1000"
            ).df()

        st.caption("Preview 1.000 baris pertama")
        st.dataframe(preview(), use_container_width=True)

        # ==========================
        # SCHEMA
        # ==========================
        st.caption("Schema Dataset")
        st.code(
            con.execute(
                f"DESCRIBE SELECT * FROM '{PARQUET_DIR}/*.parquet'"
            ).df()
        )

        st.divider()

        # ==========================
        # DOWNLOAD ALL DATA
        # ==========================
        st.subheader("⬇️ Download All Data")

        download_format = st.selectbox(
            "Pilih format download",
            ["Parquet (recommended)", "CSV"]
        )

        if st.button("⬇️ Generate Download File"):

            with tempfile.NamedTemporaryFile(delete=False) as tmp:

                if download_format == "Parquet (recommended)":
                    out_path = tmp.name + ".parquet"
                    con.execute(f"""
                        COPY (
                            SELECT * FROM '{PARQUET_DIR}/*.parquet'
                        )
                        TO '{out_path}'
                        (FORMAT PARQUET)
                    """)

                else:
                    out_path = tmp.name + ".csv"
                    con.execute(f"""
                        COPY (
                            SELECT * FROM '{PARQUET_DIR}/*.parquet'
                        )
                        TO '{out_path}'
                        (HEADER, DELIMITER ',')
                    """)

                with open(out_path, "rb") as f:
                    st.download_button(
                        label="⬇️ Download File",
                        data=f,
                        file_name=Path(out_path).name,
                        mime="application/octet-stream"
                    )


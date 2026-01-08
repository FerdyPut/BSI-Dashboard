import streamlit as st
import pandas as pd
from pathlib import Path
import duckdb

def sales():

    tab1, tab2 = st.tabs(["📥 Import Data", "👀 View Data"])

    out = Path("data/parquet")
    out.mkdir(parents=True, exist_ok=True)

    # =========================
    # TAB 1 — IMPORT DATA
    # =========================
    with tab1:

        st.subheader("Upload Excel → Parquet (View Only)")

        uploaded_files = st.file_uploader(
            "Upload file Excel",
            type=["xlsx", "xls", "xlsb"],
            accept_multiple_files=True
        )

        sheet_map = {}

        if uploaded_files:
            st.markdown("### Pilih Sheet")

            for uploaded in uploaded_files:
                xls = pd.ExcelFile(uploaded)
                sheet = st.selectbox(
                    f"Sheet untuk **{uploaded.name}**",
                    xls.sheet_names,
                    key=f"sheet_{uploaded.name}"
                )
                sheet_map[uploaded] = sheet

        if uploaded_files and st.button("▶ Generate Parquet"):
            for uploaded, sheet in sheet_map.items():
                df = pd.read_excel(uploaded, sheet_name=sheet)

                # simpan apa adanya (tanpa asumsi kolom)
                filename = uploaded.name.replace(".", "_")
                df.to_parquet(out / f"{filename}.parquet", index=False)

            st.success("✅ Data berhasil disimpan (view only)")

    # =========================
    # TAB 2 — VIEW DATA
    # =========================
    with tab2:

        st.title("👀 View Data")

        parquet_files = list(out.glob("*.parquet"))

        if not parquet_files:
            st.warning("⚠️ Belum ada data. Upload Excel dulu.")
            st.stop()

        selected_file = st.selectbox(
            "Pilih dataset",
            parquet_files,
            format_func=lambda x: x.name
        )

        @st.cache_data
        def load_parquet(path):
            return duckdb.query(f"SELECT * FROM '{path}' LIMIT 500").df()

        df = load_parquet(str(selected_file))

        st.write("Preview (500 baris pertama)")
        st.dataframe(df, use_container_width=True)

        st.write("Kolom:")
        st.code(list(df.columns))

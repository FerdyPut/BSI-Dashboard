import streamlit as st
import pandas as pd
from pathlib import Path
import duckdb

def sales():

    tab1, tab2 = st.tabs(["📥 Import Data", "📊 View Data"])

    # =========================
    # TAB 1 — IMPORT DATA
    # =========================
    with tab1:

        st.subheader("Upload Excel → Parquet")

        uploaded_files = st.file_uploader(
            "Upload file Excel",
            type=["xlsx", "xls", "xlsb"],
            accept_multiple_files=True
        )

        out = Path("data/parquet")
        out.mkdir(parents=True, exist_ok=True)

        if uploaded_files and st.button("▶ Generate Parquet"):

            for uploaded in uploaded_files:
                df = pd.read_excel(uploaded)
                df["Tanggal"] = pd.to_datetime(df["Tanggal"])

                year = df["Tanggal"].dt.year.mode()[0]
                output = out / f"sales_{year}.parquet"

                # append aman
                if output.exists():
                    old = pd.read_parquet(output)
                    df = pd.concat([old, df], ignore_index=True)

                df.to_parquet(output, index=False)

            st.success("✅ Parquet berhasil digenerate")

    # =========================
    # TAB 2 — DASHBOARD
    # =========================
    with tab2:
        st.title("📊 Sales Dashboard")

        parquet_files = list(Path("data/parquet").glob("*.parquet"))

        if not parquet_files:
            st.warning("⚠️ Belum ada data. Silakan upload Excel di tab Import Data.")
            st.stop()

        @st.cache_data
        def query(sql):
            return duckdb.query(sql).df()

        year = st.selectbox("Year", [2024, 2025, 2026])

        df = query(f"""
            SELECT
                Cabang,
                SUM(Qty) AS total_qty,
                SUM(Value) AS total_value
            FROM 'data/parquet/*.parquet'
            WHERE year(Tanggal) = {year}
            GROUP BY Cabang
        """)

        st.dataframe(df, use_container_width=True)

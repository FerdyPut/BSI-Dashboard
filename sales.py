import streamlit as st
import pandas as pd
from pathlib import Path
import duckdb
import uuid

PARQUET_DIR = Path("data/parquet/sales")
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

st.set_page_config(page_title="Excel / CSV → Parquet", layout="wide")

def sales():

    tab1, tab2 = st.tabs(["📥 Import Data", "📊 View & Download"])

    # ==================================================
    # TAB 1 — IMPORT + SUMMARY
    # ==================================================
    with tab1:
        st.subheader("Upload Data → Summary → Append ALL")

        uploaded_files = st.file_uploader(
            "Upload Excel / CSV",
            type=["xlsx", "xls", "xlsb", "csv"],
            accept_multiple_files=True
        )

        if "file_map" not in st.session_state:
            st.session_state.file_map = {}

        if uploaded_files:
            summaries = []  # ⬅️ simpan summary per file

            for uploaded in uploaded_files:

                # =====================
                # EXCEL
                # =====================
                if uploaded.name.lower().endswith(("xlsx", "xls", "xlsb")):
                    xls = pd.ExcelFile(uploaded)

                    sheet = st.selectbox(
                        f"Sheet untuk {uploaded.name}",
                        xls.sheet_names,
                        key=f"sheet_{uploaded.name}"
                    )

                    df_tmp = pd.read_excel(uploaded, sheet_name=sheet)

                # =====================
                # CSV
                # =====================
                else:
                    delimiter = st.selectbox(
                        f"Delimiter ({uploaded.name})",
                        [",", ";", "|", "\t"],
                        key=f"delim_{uploaded.name}"
                    )

                    df_tmp = pd.read_csv(uploaded, delimiter=delimiter)

                # =====================
                # SUMMARY (SAFE NUMERIC)
                # =====================
                if "Value" in df_tmp.columns:
                    value_sum = pd.to_numeric(
                        df_tmp["Value"],
                        errors="coerce"
                    ).sum()
                else:
                    value_sum = None

                summaries.append({
                    "File": uploaded.name,
                    "Rows": len(df_tmp),
                    "Sum Value": value_sum
                })

                # simpan metadata buat append
                st.session_state.file_map[uploaded.name] = {
                    "file": uploaded,
                    "df": df_tmp
                }

            # =====================
            # TAMPILKAN SUMMARY
            # =====================
            st.divider()
            st.subheader("📊 Summary Data (Before Append)")

            summary_df = pd.DataFrame(summaries)
            st.dataframe(summary_df, use_container_width=True)

            # =====================
            # APPEND ALL
            # =====================
            if st.button("🚀 Append ALL Files"):
                for meta in st.session_state.file_map.values():

                    df = meta["df"].astype("string")

                    df["_source_file"] = meta["file"].name

                    output = PARQUET_DIR / f"part-{uuid.uuid4().hex}.parquet"
                    df.to_parquet(output, index=False)

                st.success("✅ Semua file berhasil di-append ke dataset")
                st.session_state.file_map = {}

    # ==================================================
    # TAB 2 — VIEW + METRIC
    # ==================================================
    with tab2:
        st.subheader("Parquet Dataset Info")

        parquet_files = list(PARQUET_DIR.glob("*.parquet"))
        if not parquet_files:
            st.warning("⚠️ Dataset kosong")
            st.stop()

        con = duckdb.connect(":memory:")

        # ==========================
        # TOTAL ROWS
        # ==========================
        total_rows = con.execute(
            f"SELECT COUNT(*) FROM '{PARQUET_DIR}/*.parquet'"
        ).fetchone()[0]

        # ==========================
        # TOTAL VALUE (SAFE CAST)
        # ==========================
        total_value = con.execute(
            f"""
            SELECT
                SUM(TRY_CAST(Value AS DOUBLE))
            FROM '{PARQUET_DIR}/*.parquet'
            """
        ).fetchone()[0]

        col1, col2 = st.columns(2)
        col1.metric("📊 Total Rows (All Data)", f"{total_rows:,}")
        col2.metric("💰 Total Value (All Data)", f"{total_value:,.2f}")

        # ==========================
        # PREVIEW
        # ==========================
        df_preview = con.execute(
            f"SELECT * FROM '{PARQUET_DIR}/*.parquet' LIMIT 1000"
        ).df()

        st.dataframe(df_preview, use_container_width=True)



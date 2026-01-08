import streamlit as st
import pandas as pd
from pathlib import Path
import duckdb
import uuid
import shutil
import tempfile

# =========================
# CONFIG
# =========================
st.set_page_config(
    page_title="Excel / CSV → Parquet",
    layout="wide"
)

PARQUET_DIR = Path("data/parquet/sales")
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# APP
# =========================
def sales():

    tab1, tab2, tab3 = st.tabs(["📥 Import Data", "📊 View & Download", "Analytics"])

    # ==================================================
    # TAB 1 — IMPORT
    # ==================================================
    with tab1:
        st.subheader("Upload Excel / CSV → Append ke Dataset")

        uploaded_files = st.file_uploader(
            "Upload file",
            type=["xlsx", "xls", "xlsb", "csv"],
            accept_multiple_files=True
        )

        if "files" not in st.session_state:
            st.session_state.files = {}

        if uploaded_files:
            for uploaded in uploaded_files:

                st.markdown(f"### 📄 {uploaded.name}")

                # -------- EXCEL --------
                if uploaded.name.lower().endswith(("xlsx", "xls", "xlsb")):
                    xls = pd.ExcelFile(uploaded)

                    sheet = st.selectbox(
                        "Pilih sheet",
                        xls.sheet_names,
                        key=f"sheet_{uploaded.name}"
                    )

                    st.session_state.files[uploaded.name] = {
                        "type": "excel",
                        "file": uploaded,
                        "sheet": sheet
                    }

                # -------- CSV --------
                else:
                    delimiter = st.selectbox(
                        "Delimiter",
                        [",", ";", "|", "\t"],
                        key=f"delim_{uploaded.name}"
                    )

                    st.session_state.files[uploaded.name] = {
                        "type": "csv",
                        "file": uploaded,
                        "delimiter": delimiter
                    }

                st.divider()

        # =========================
        # APPEND ALL
        # =========================
        if uploaded_files and st.button("🚀 Append ALL Files"):

            for meta in st.session_state.files.values():

                # read file
                if meta["type"] == "excel":
                    df = pd.read_excel(
                        meta["file"],
                        sheet_name=meta["sheet"]
                    )
                else:
                    df = pd.read_csv(
                        meta["file"],
                        delimiter=meta["delimiter"]
                    )

                # 🔒 SAFE MODE: semua STRING (anti ArrowTypeError)
                df = df.astype("string")

                # metadata
                df["_source_file"] = meta["file"].name

                # write parquet part
                out = PARQUET_DIR / f"part-{uuid.uuid4().hex}.parquet"
                df.to_parquet(out, index=False)

            st.success("✅ Semua file berhasil di-append")
            st.session_state.files = {}

        # =========================
        # RESET DATASET
        # =========================
        st.divider()
        st.subheader("🧹 Reset Dataset")

        if st.button("⚠️ Hapus SEMUA Data Parquet"):
            shutil.rmtree(PARQUET_DIR)
            PARQUET_DIR.mkdir(parents=True, exist_ok=True)
            st.session_state.files = {}
            st.success("✅ Dataset berhasil di-reset")

    # ==================================================
    # TAB 2 — VIEW & DOWNLOAD
    # ==================================================
    with tab2:
        st.subheader("📊 Dataset Info")

        parquet_files = list(PARQUET_DIR.glob("*.parquet"))
        if not parquet_files:
            st.warning("⚠️ Dataset masih kosong")
            st.stop()

        con = duckdb.connect(":memory:")

        # =========================
        # METRICS
        # =========================
        total_rows = con.execute(
            f"SELECT COUNT(*) FROM '{PARQUET_DIR}/*.parquet'"
        ).fetchone()[0]

        total_value = con.execute(
            f"""
            SELECT SUM(TRY_CAST(Value AS DOUBLE))
            FROM '{PARQUET_DIR}/*.parquet'
            """
        ).fetchone()[0]

        col1, col2 = st.columns(2)
        col1.metric("📊 Total Rows", f"{total_rows:,}")
        col2.metric("💰 Total Value", f"{total_value:,.2f}" if total_value else "—")

        # =========================
        # PREVIEW
        # =========================
        st.divider()
        st.caption("Preview 1.000 baris pertama")

        df_preview = con.execute(
            f"SELECT * FROM '{PARQUET_DIR}/*.parquet' LIMIT 1000"
        ).df()

        st.dataframe(df_preview, use_container_width=True)

        # =========================
        # SCHEMA
        # =========================
        st.caption("Schema Dataset")
        st.code(
            con.execute(
                f"DESCRIBE SELECT * FROM '{PARQUET_DIR}/*.parquet'"
            ).df()
        )

        # =========================
        # DOWNLOAD
        # =========================
        st.divider()
        st.subheader("⬇️ Download All Data")

        fmt = st.selectbox(
            "Format",
            ["Parquet (recommended)", "CSV"]
        )

        if st.button("⬇️ Generate Download"):
            with tempfile.NamedTemporaryFile(delete=False) as tmp:

                if fmt == "Parquet (recommended)":
                    out = tmp.name + ".parquet"
                    con.execute(f"""
                        COPY (
                            SELECT * FROM '{PARQUET_DIR}/*.parquet'
                        )
                        TO '{out}'
                        (FORMAT PARQUET)
                    """)
                else:
                    out = tmp.name + ".csv"
                    con.execute(f"""
                        COPY (
                            SELECT * FROM '{PARQUET_DIR}/*.parquet'
                        )
                        TO '{out}'
                        (HEADER, DELIMITER ',')
                    """)

                with open(out, "rb") as f:
                    st.download_button(
                        "⬇️ Download File",
                        data=f,
                        file_name=Path(out).name,
                        mime="application/octet-stream"
                    )
    # ==================================================
    # TAB 3 — ANALYTICS
    # ==================================================
    with tab3:
        st.subheader("📈 Analytics – Pivot 3 Bulan Terakhir")

        parquet_files = list(PARQUET_DIR.glob("*.parquet"))
        if not parquet_files:
            st.warning("⚠️ Dataset kosong")
            st.stop()

        con = duckdb.connect(":memory:")

        # =========================
        # FILTER OPTIONS (SAFE)
        # =========================
        def get_distinct(col_sql):
            return con.execute(
                f"""
                SELECT DISTINCT {col_sql}
                FROM '{PARQUET_DIR}/*.parquet'
                WHERE {col_sql} IS NOT NULL
                ORDER BY {col_sql}
                """
            ).df().iloc[:, 0].dropna().tolist()

        filters = {
            label: st.multiselect(label, get_distinct(col_sql))
            for label, col_sql in FILTER_COLUMNS.items()
        }

        # =========================
        # BUILD WHERE CLAUSE
        # =========================
        where_clause = []
        for label, values in filters.items():
            if values:
                col_sql = FILTER_COLUMNS[label]
                quoted_vals = ", ".join([f"'{v}'" for v in values])
                where_clause.append(f"{col_sql} IN ({quoted_vals})")

        where_sql = " AND ".join(where_clause)
        if where_sql:
            where_sql = "AND " + where_sql

        # =========================
        # PIVOT 3 BULAN TERAKHIR
        # =========================
        query = f"""
        WITH base AS (
            SELECT
                SKU,
                DATE_TRUNC('month', CAST(Tanggal AS DATE)) AS bulan,
                TRY_CAST(Value AS DOUBLE) AS value
            FROM '{PARQUET_DIR}/*.parquet'
            WHERE
                CAST(Tanggal AS DATE) >=
                    DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '2 months'
                {where_sql}
        )
        SELECT *
        FROM base
        PIVOT (
            SUM(value)
            FOR bulan IN (
                DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '2 months',
                DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month',
                DATE_TRUNC('month', CURRENT_DATE)
            )
        )
        ORDER BY SKU
        """

        df_pivot = con.execute(query).df()

        # =========================
        # RENAME BULAN → YYYY-MM
        # =========================
        df_pivot.columns = [
            "SKU" if c == "SKU" else str(c)[:7]
            for c in df_pivot.columns
        ]

        st.dataframe(df_pivot, use_container_width=True)
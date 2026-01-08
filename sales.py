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
        st.subheader("📊 Analytics — 3 Bulan Terakhir + AVG 12M")

        con = duckdb.connect(":memory:")

        # =========================
        # HELPER: distinct values
        # =========================
        @st.cache_data
        def get_distinct(col):
            return (
                con.execute(
                    f'SELECT DISTINCT "{col}" FROM \'{PARQUET_DIR}/*.parquet\' WHERE "{col}" IS NOT NULL'
                )
                .df()[col]
                .dropna()
                .sort_values()
                .tolist()
            )

        # =========================
        # FILTERS
        # =========================
        col1, col2, col3 = st.columns(3)

        with col1:
            filters = {
                "REGION": st.multiselect("REGION", get_distinct("REGION")),
                "DISTRIBUTOR": st.multiselect("DISTRIBUTOR", get_distinct("DISTRIBUTOR")),
            }

        with col2:
            filters.update({
                "AREA": st.multiselect("AREA", get_distinct("AREA")),
                "SALES OFFICE": st.multiselect("SALES OFFICE", get_distinct("SALES OFFICE")),
            })

        with col3:
            filters.update({
                "GROUP": st.multiselect("GROUP", get_distinct("GROUP")),
                "TIPE": st.multiselect("TIPE", get_distinct("TIPE")),
            })

        # =========================
        # PERIODE (BULAN & TAHUN)
        # =========================
        tahun_options = sorted(
            con.execute(f"SELECT DISTINCT TAHUN FROM '{PARQUET_DIR}/*.parquet'").df()["TAHUN"]
        )

        tahun_akhir = st.selectbox("Tahun Terakhir (Closed Month)", tahun_options, index=len(tahun_options)-1)
        tahun_akhir = int(tahun_akhir)

        bulan_akhir = st.selectbox("Bulan Terakhir (Closed Month)", list(range(1, 13)), index=11)
        bulan_akhir = int(bulan_akhir)

        # =========================
        # HITUNG 3 BULAN TERAKHIR (exclude bulan akhir)
        # =========================
        periods = []
        start_month = bulan_akhir - 1
        start_year = tahun_akhir
        if start_month == 0:
            start_month = 12
            start_year -= 1

        for i in [2, 1, 0]:
            m = start_month - i
            y = start_year
            if m <= 0:
                m += 12
                y -= 1
            periods.append((y, m))

        # =========================
        # WHERE FILTER SQL
        # =========================
        where_clauses = []
        for col, vals in filters.items():
            if vals:
                safe_vals = ",".join([f"'{v}'" for v in vals])
                where_clauses.append(f'"{col}" IN ({safe_vals})')

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        # =========================
        # BUILD PIVOT COLUMNS
        # =========================
        month_exprs = []
        month_labels = []

        # 3 bulan terakhir
        for y, m in periods:
            label = f"{y}-{m:02d}"
            month_labels.append(label)

            month_exprs.append(f"""
                SUM(
                    CASE
                        WHEN TAHUN = {y} AND MONTH = {m}
                        THEN TRY_CAST(Value AS DOUBLE)
                    END
                ) AS "{label}"
            """)

        # rolling 12 bulan -> AVG_12M
        end_index = tahun_akhir * 12 + bulan_akhir
        start_index = end_index - 11

        month_exprs.append(f"""
            SUM(
                CASE
                    WHEN (TAHUN * 12 + MONTH)
                        BETWEEN {start_index} AND {end_index}
                    THEN TRY_CAST(Value AS DOUBLE)
                END
            ) / 12 AS "AVG_12M"
        """)

        # =========================
        # FINAL QUERY + GRAND TOTAL
        # =========================
        sql = f"""
        WITH base AS (
            SELECT
                SKU,
                {",".join(month_exprs)}
            FROM '{PARQUET_DIR}/*.parquet'
            {where_sql}
            GROUP BY SKU
        ),
        grand_total AS (
            SELECT
                'GRAND TOTAL' AS SKU,
                {",".join([
                    f'SUM("{lbl}") AS "{lbl}"' for lbl in month_labels
                ])},
                AVG("AVG_12M") AS "AVG_12M"
            FROM base
        ),
        final AS (
            SELECT * FROM base
            UNION ALL
            SELECT * FROM grand_total
        )
        SELECT *
        FROM final
        ORDER BY
            CASE WHEN SKU = 'GRAND TOTAL' THEN 0 ELSE 1 END,
            SKU
        """

        df = con.execute(sql).df()

        # =========================
        # FORMAT RUPIAH
        # =========================
        def format_rupiah(x):
            if pd.isna(x):
                return "-"
            return f"Rp {x:,.0f}".replace(",", ".")

        df_display = df.copy()
        for c in df_display.columns:
            if c != "SKU":
                df_display[c] = df_display[c].apply(format_rupiah)

        # =========================
        # SHOW TABLE
        # =========================
        st.caption(f"Periode Pivot: {month_labels[0]} → {month_labels[-1]} (Closed Month)")
        st.dataframe(
            df_display.style.apply(
                lambda r: ["font-weight: bold"]*len(r) if r["SKU"]=="GRAND TOTAL" else [""]*len(r),
                axis=1
            ),
            use_container_width=True
        )

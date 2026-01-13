import streamlit as st
import pandas as pd
from pathlib import Path
import duckdb
import uuid
import shutil
import tempfile
import calendar
from datetime import date, timedelta
import datetime
from streamlit import column_config

# =========================
# CONFIG
# =========================
st.set_page_config(
    page_title="Dashboard Sales - BSI",
    layout="wide"
)

# =========================
# APP
# =========================
def sales():

    tab1, tab2, tab03, tab3 = st.tabs([
        "📥 Import Data",
        "📊 Data Sales View",
        "📊 Data Target View",
        "📈 Analytics"
    ])

    # ==================================================
    # DIRECTORY SETUP
    # ==================================================
    PARQUET_DIR_SALES  = Path("data/parquet/sales")
    PARQUET_DIR_TARGET = Path("data/parquet/target")

    EXCEL_DIR_SALES  = Path("data/excel/sales")
    EXCEL_DIR_TARGET = Path("data/excel/target")

    for p in [
        PARQUET_DIR_SALES, PARQUET_DIR_TARGET,
        EXCEL_DIR_SALES, EXCEL_DIR_TARGET
    ]:
        p.mkdir(parents=True, exist_ok=True)

    # ==================================================
    # TAB 1 — IMPORT DATA (SALES / TARGET)
    # ==================================================
    with tab1:
        st.subheader("Upload Data → Gabungkan ke Dataset")

        # =========================
        # PILIH JENIS DATA
        # =========================
        data_type = st.radio(
            "Jenis Data",
            ["Sales", "Target"],
            horizontal=True
        )

        PARQUET_DIR_ACTIVE = (
            PARQUET_DIR_SALES if data_type == "Sales"
            else PARQUET_DIR_TARGET
        )

        # =========================
        # UPLOADER
        # =========================
        uploaded_files = st.file_uploader(
            f"Upload file {data_type} (Parquet / Excel / CSV)",
            type=["parquet", "xlsx", "xls", "xlsb", "csv"],
            accept_multiple_files=True
        )

        if "files" not in st.session_state:
            st.session_state.files = {}

        if uploaded_files:
            for uploaded in uploaded_files:
                st.markdown(f"### 📄 {uploaded.name}")

                if uploaded.name.lower().endswith("parquet"):
                    st.session_state.files[uploaded.name] = {
                        "type": "parquet",
                        "file": uploaded
                    }

                elif uploaded.name.lower().endswith(("xlsx", "xls", "xlsb")):
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

                else:  # CSV
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
        if uploaded_files and st.button(f"🚀 Append ALL {data_type}"):

            for meta in st.session_state.files.values():

                # ---------- READ ----------
                if meta["type"] == "parquet":
                    df = pd.read_parquet(meta["file"])
                elif meta["type"] == "excel":
                    df = pd.read_excel(meta["file"], sheet_name=meta["sheet"])
                else:
                    df = pd.read_csv(meta["file"], delimiter=meta["delimiter"])

                # 🔒 SAFE MODE
                df = df.astype("string")

                # metadata
                df["_source_file"] = getattr(meta["file"], "name", "uploaded_data")
                df["_data_type"] = data_type.lower()

                # ---------- SAVE PARQUET ----------
                out = PARQUET_DIR_ACTIVE / f"part-{uuid.uuid4().hex}.parquet"
                df.to_parquet(out, index=False)

            st.success(f"✅ Semua file {data_type} berhasil digabung")
            st.session_state.files = {}

        # =========================
        # RESET DATA (OPTIONAL)
        # =========================
        st.divider()
        st.subheader("🧹 Reset Dataset")

        col1, col2 = st.columns(2)

        with col1:
            if st.button("⚠️ Reset Data Sales"):
                shutil.rmtree(PARQUET_DIR_SALES, ignore_errors=True)
                PARQUET_DIR_SALES.mkdir(parents=True, exist_ok=True)
                st.success("✅ Data Sales di-reset")

        with col2:
            if st.button("⚠️ Reset Data Target"):
                shutil.rmtree(PARQUET_DIR_TARGET, ignore_errors=True)
                PARQUET_DIR_TARGET.mkdir(parents=True, exist_ok=True)
                st.success("✅ Data Target di-reset")

    # ==================================================
    # TAB 2 — VIEW & DOWNLOAD (WITH CLEANING)
    # ==================================================
    with tab2:
        st.subheader("📊 Dataset Info")

        parquet_files = list(PARQUET_DIR_SALES.glob("*.parquet"))
        if not parquet_files:
            st.warning("⚠️ Dataset masih kosong")
            st.stop()

        con = duckdb.connect(":memory:")

        # =========================
        # CLEANING OPTION
        # =========================
        cleaning_on = st.checkbox(
            "🧹 Cleaning Data (TRIM + UPPER untuk semua kolom text)",
            value=True
        )

        # =========================
        # READ SCHEMA
        # =========================
        schema_df = con.execute(
            f"DESCRIBE SELECT * FROM '{PARQUET_DIR_SALES}/*.parquet'"
        ).df()

        all_cols = schema_df["column_name"].tolist()

        string_cols = schema_df[
            schema_df["column_type"].str.contains("VARCHAR|TEXT", case=False)
        ]["column_name"].tolist()

        # =========================
        # BUILD SELECT SQL (SAFE FOR SPACES)
        # =========================
        select_exprs = []

        for col in all_cols:
            col_quoted = f'"{col}"'

            if cleaning_on and col in string_cols:
                select_exprs.append(
                    f"TRIM(UPPER({col_quoted})) AS {col_quoted}"
                )
            else:
                select_exprs.append(col_quoted)

        select_sql = ", ".join(select_exprs)

        # =========================
        # METRICS
        # =========================
        total_rows = con.execute(
            f"""
            SELECT COUNT(*)
            FROM '{PARQUET_DIR_SALES}/*.parquet'
            """
        ).fetchone()[0]

        total_value = con.execute(
            f"""
            SELECT SUM(TRY_CAST(Value AS DOUBLE))
            FROM (
                SELECT {select_sql}
                FROM '{PARQUET_DIR_SALES}/*.parquet'
            )
            """
        ).fetchone()[0]

        col1, col2 = st.columns(2)
        col1.metric("📊 Total Rows", f"{total_rows:,}")
        col2.metric("💰 Total Value", f"{total_value:,.2f}" if total_value else "—")

        # =========================
        # PREVIEW
        # =========================
        st.divider()
        st.caption("Preview 1.000 baris pertama (setelah cleaning)")

        df_preview = con.execute(
            f"""
            SELECT {select_sql}
            FROM '{PARQUET_DIR_SALES}/*.parquet'
            LIMIT 1000
            """
        ).df()

        st.dataframe(df_preview, use_container_width=True)

        # =========================
        # SCHEMA
        # =========================
        st.caption("Schema Dataset")
        st.code(schema_df)

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
                            SELECT {select_sql}
                            FROM '{PARQUET_DIR_SALES}/*.parquet'
                        )
                        TO '{out}'
                        (FORMAT PARQUET)
                    """)
                else:
                    out = tmp.name + ".csv"
                    con.execute(f"""
                        COPY (
                            SELECT {select_sql}
                            FROM '{PARQUET_DIR_SALES}/*.parquet'
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
    # TAB 03 — VIEW & DOWNLOAD (WITH CLEANING)
    # ==================================================
    with tab03:
        st.subheader("📊 Dataset Info")

        parquet_files = list(PARQUET_DIR_TARGET.glob("*.parquet"))
        if not parquet_files:
            st.warning("⚠️ Dataset masih kosong")
            st.stop()

        con = duckdb.connect(":memory:")

        # =========================
        # CLEANING OPTION
        # =========================
        cleaning_on = st.checkbox(
            "🧹 Cleaning Data (TRIM + UPPER untuk semua kolom text)",
            value=True, key='checktarget'
        )

        # =========================
        # READ SCHEMA
        # =========================
        schema_df = con.execute(
            f"DESCRIBE SELECT * FROM '{PARQUET_DIR_TARGET}/*.parquet'"
        ).df()

        all_cols = schema_df["column_name"].tolist()

        string_cols = schema_df[
            schema_df["column_type"].str.contains("VARCHAR|TEXT", case=False)
        ]["column_name"].tolist()

        # =========================
        # BUILD SELECT SQL (SAFE FOR SPACES)
        # =========================
        select_exprs = []

        for col in all_cols:
            col_quoted = f'"{col}"'

            if cleaning_on and col in string_cols:
                select_exprs.append(
                    f"TRIM(UPPER({col_quoted})) AS {col_quoted}"
                )
            else:
                select_exprs.append(col_quoted)

        select_sql = ", ".join(select_exprs)

        # =========================
        # METRICS
        # =========================
        total_rows = con.execute(
            f"""
            SELECT COUNT(*)
            FROM '{PARQUET_DIR_TARGET}/*.parquet'
            """
        ).fetchone()[0]

        total_value = con.execute(
            f"""
            SELECT SUM(TRY_CAST(Value AS DOUBLE))
            FROM (
                SELECT {select_sql}
                FROM '{PARQUET_DIR_TARGET}/*.parquet'
            )
            """
        ).fetchone()[0]

        col1, col2 = st.columns(2)
        col1.metric("📊 Total Rows", f"{total_rows:,}")
        col2.metric("💰 Total Value Target", f"{total_value:,.2f}" if total_value else "—")

        # =========================
        # PREVIEW
        # =========================
        st.divider()
        st.caption("Preview 1.000 baris pertama (setelah cleaning)")

        df_preview = con.execute(
            f"""
            SELECT {select_sql}
            FROM '{PARQUET_DIR_TARGET}/*.parquet'
            LIMIT 1000
            """
        ).df()

        st.dataframe(df_preview, use_container_width=True)

        # =========================
        # SCHEMA
        # =========================
        st.caption("Schema Dataset")
        st.code(schema_df)

        # =========================
        # DOWNLOAD
        # =========================
        st.divider()
        st.subheader("⬇️ Download All Data")

        fmt = st.selectbox(
            "Format",
            ["Parquet (recommended)", "CSV"], key='selectformattarget'
        )

        if st.button("⬇️ Generate Download", key='targetdownload'):
            with tempfile.NamedTemporaryFile(delete=False) as tmp:

                if fmt == "Parquet (recommended)":
                    out = tmp.name + ".parquet"
                    con.execute(f"""
                        COPY (
                            SELECT {select_sql}
                            FROM '{PARQUET_DIR_TARGET}/*.parquet'
                        )
                        TO '{out}'
                        (FORMAT PARQUET)
                    """)
                else:
                    out = tmp.name + ".csv"
                    con.execute(f"""
                        COPY (
                            SELECT {select_sql}
                            FROM '{PARQUET_DIR_TARGET}/*.parquet'
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
        st.markdown(
                    f"""
                    <style>
                    .hover-box2 {{
                        border: 1px solid #005461;
                        border-radius: 10px;
                        padding: 5px;
                        text-align: center;
                        background-color: #005461;
                        color: white;
                        transition: 0.3s;
                        position: relative;
                        margin-top: 1px;
                        font-size: 18px;
                        font-family: 'Poppins', sans-serif;
                    }}
                    .hover-box2:hover {{
                        background-color: #005461;
                        transform: scale(1.01);
                    }}
                    .download-btn {{
                        display: none;
                        margin-top: 10px;
                    }}
                    .hover-box2:hover .download-btn {{
                        display: block;
                    }}
                    a.download-link {{
                        color: white;
                        text-decoration: none;
                        padding: 5px 10px;
                        background-color: #005461;
                        border-radius: 5px;
                        font-weight: bold;
                    }}
                    </style>

                    <div class="hover-box2">
                        <strong>TYPES OF FILTER</strong>
                    </div>
                    <p></p>
                    """, unsafe_allow_html=True
                )
        st.subheader("📊 Analytics Advanced")

        con = duckdb.connect(":memory:")
        

        # =========================
        # HELPER: distinct values
        # =========================
        @st.cache_data
        def get_distinct(col):
            return (
                con.execute(
                    f"""
                    SELECT DISTINCT
                        TRIM(UPPER("{col}")) AS val
                    FROM '{PARQUET_DIR_SALES}/*.parquet'
                    WHERE "{col}" IS NOT NULL
                    """
                )
                .df()["val"]
                .dropna()
                .sort_values()
                .tolist()
            )


        @st.cache_data
        def get_distinct_target(col):
            return (
                con.execute(
                    f"""
                    SELECT DISTINCT
                        TRIM(UPPER("{col}")) AS val
                    FROM '{PARQUET_DIR_TARGET}/*.parquet'
                    WHERE "{col}" IS NOT NULL
                    """
                )
                .df()["val"]
                .dropna()
                .sort_values()
                .tolist()
            )
        # =========================
        # FILTERS
        # =========================
        with st.container(border=True):
            col1, col2, col3 = st.columns(3)

            with col1:
                filters = {
                    "REGION": st.multiselect("REGION", get_distinct("REGION")),
                    "SALES OFFICE": st.multiselect("SALES OFFICE", get_distinct("SALES OFFICE")),
                }

            with col2:
                filters.update({
                    "AREA": st.multiselect("AREA", get_distinct("AREA")),
                    "GROUP": st.multiselect("GROUP", get_distinct("GROUP")),
                })

            with col3:
                filters.update({
                    "DISTRIBUTOR": st.multiselect("DISTRIBUTOR", get_distinct("DISTRIBUTOR")),
                    "TIPE": st.multiselect("TIPE", get_distinct("TIPE")),
                })

        # =========================
        # TAHUN & BULAN CLOSED
        # =========================
        parquet_path = str(PARQUET_DIR_SALES / "*.parquet")
        tahun_options = sorted(
            con.execute(f"SELECT DISTINCT CAST(TAHUN AS INTEGER) AS TAHUN FROM '{PARQUET_DIR_SALES}/*.parquet'").df()["TAHUN"]
        )

        st.markdown(
                    f"""
                    <style>
                    .hover-box {{
                        border: 1px solid #215E61;
                        border-radius: 10px;
                        padding: 5px;
                        text-align: center;
                        background-color: #215E61;
                        color: white;
                        transition: 0.3s;
                        position: relative;
                        margin-top: 1px;
                        font-size: 18px;
                        font-family: 'Poppins', sans-serif;
                    }}
                    .hover-box:hover {{
                        background-color: #215E61;
                        transform: scale(1.01);
                    }}
                    .download-btn {{
                        display: none;
                        margin-top: 10px;
                    }}
                    .hover-box:hover .download-btn {{
                        display: block;
                    }}
                    a.download-link {{
                        color: white;
                        text-decoration: none;
                        padding: 5px 10px;
                        background-color: #215E61;
                        border-radius: 5px;
                        font-weight: bold;
                    }}
                    </style>

                    <div class="hover-box">
                        <strong>CLOSING DATE</strong>
                    </div>
                    <p></p>
                    """, unsafe_allow_html=True
                )

        with st.container(border=True):
            tahun_akhir = st.selectbox(
                "Tahun Terakhir (Closed Month)",
                tahun_options,
                index=len(tahun_options)-1
            )
            tahun_akhir = int(tahun_akhir)

            bulan_akhir = st.selectbox(
                "Bulan Terakhir (Closed Month)",
                list(range(1,13)),
                index=11
            )
            bulan_akhir = int(bulan_akhir)

        st.badge(f"Closingan Date dipilih: Bulan ke-{bulan_akhir} Tahun {tahun_akhir}", color='blue')

        # =========================
        # Container Input Historical Week
        # =========================
        st.markdown(
                    f"""
                    <style>
                    .hover-box1 {{
                        border: 1px solid #233D4D;
                        border-radius: 10px;
                        padding: 5px;
                        text-align: center;
                        background-color: #233D4D;
                        color: white;
                        transition: 0.3s;
                        position: relative;
                        margin-top: 1px;
                        font-size: 18px;
                        font-family: 'Poppins', sans-serif;
                    }}
                    .hover-box1:hover {{
                        background-color: #233D4D;
                        transform: scale(1.01);
                    }}
                    .download-btn {{
                        display: none;
                        margin-top: 10px;
                    }}
                    .hover-box1:hover .download-btn {{
                        display: block;
                    }}
                    a.download-link {{
                        color: white;
                        text-decoration: none;
                        padding: 5px 10px;
                        background-color: #233D4D;
                        border-radius: 5px;
                        font-weight: bold;
                    }}
                    </style>

                    <div class="hover-box1">
                        <strong>HISTORICAL WEEK UPDATE</strong>
                    </div>
                    <p></p>
                    """, unsafe_allow_html=True
                )
        with st.container(border=True):
            
            col1, col2 = st.columns(2)  # buat 2 kolom: Tahun | Bulan
            with col1:
                tahun_hist = st.selectbox(
                    "Pilih Tahun",
                    options=list(range(2024, 2030)),
                    index=2  # default 2026
                )
            with col2:
                bulan_hist = st.selectbox(
                    "Pilih Bulan",
                    options=list(range(1, 13))
                )
            
            # pastikan tipe integer
            tahun_hist = int(tahun_hist)
            bulan_hist = int(bulan_hist)

        # Sekarang tahun_hist dan bulan_hist siap dipakai di query
        st.badge(f"Historical Week dipilih: Bulan ke-{bulan_hist} Tahun {tahun_hist}", color='blue')
        
        # =========================
        # Generate ISO week calendar otomatis
        # =========================
        def generate_iso_calendar(year):
            first_monday = date(year-1, 12, 29)
            iso_weeks = []
            week_num = 1
            d = first_monday
            while d.year <= year or (d.year == year+1 and week_num <= 53):
                iso_weeks.append({
                    "ISO_WEEK": week_num,
                    "MONDAY": d,
                    "SUNDAY": d + timedelta(days=6),
                    "MONTH": d.month
                })
                d += timedelta(days=7)
                week_num += 1
            return pd.DataFrame(iso_weeks)

        df_iso = generate_iso_calendar(tahun_hist)

        # =========================
        # HITUNG 3 BULAN TERAKHIR (exclude bulan closed)
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
            # ganti label ke format "Mon-YYYY", misal "Okt-2025"
            month_name = calendar.month_abbr[m]  # 'Jan', 'Feb', 'Mar', ...
            label = f"{month_name}-{y}"
            month_labels.append(label)

            month_exprs.append(f"""
                COALESCE(SUM(
                    CASE
                        WHEN EXTRACT(YEAR FROM DT) = {y}
                        AND EXTRACT(MONTH FROM DT) = {m}
                        THEN Value
                    END
                ),0) AS "{label}"
            """)

        # AVG 12 BULAN (rolling)
        # Hitung start dan end bulan/tahun
        end_year, end_month = tahun_akhir, bulan_akhir
        end_index = tahun_akhir * 12 + bulan_akhir
        start_index = end_index - 11

        # start bulan/tahun
        start_month = end_month - 11
        start_year = end_year
        if start_month <= 0:
            start_month += 12
            start_year -= 1

        # Label kolom AVG 12M
        start_label = f"{calendar.month_abbr[start_month]}-{start_year}"
        end_label = f"{calendar.month_abbr[end_month]}-{end_year}"
        avg12m_label = f"Avg Sales Per ({start_label} until {end_label})"

        # Kolom AVG 12M, jika tidak ada data → 0
        month_exprs.append(f"""
            COALESCE(
                SUM(
                    CASE
                        WHEN (EXTRACT(YEAR FROM DT) * 12 + EXTRACT(MONTH FROM DT))
                            BETWEEN {start_index} AND {end_index}
                        THEN Value
                    END
                ) / 12,
            0) AS "{avg12m_label}"
        """)

        # AVG 3 BULAN TERAKHIR → tambahkan setelah AVG_12M
        avg3m_label = f"Avg Sales Per ({month_labels[0]} until {month_labels[-1]})"
        month_exprs.append(f"""
            ({' + '.join([f'COALESCE("{lbl}",0)' for lbl in month_labels])}) / 3 AS "{avg3m_label}"
        """)

        # Fungsi buat generate semua Senin 2025-2026 dengan ISO week, Month ISO, dan WEEK_IN_MONTH
        def generate_iso_week_table_parquet(start_year=2025, end_year=2026):
            rows = []
            start_date = datetime.date(start_year-1, 12, 29)  # Senin terakhir tahun sebelumnya
            end_date = datetime.date(end_year, 12, 31)
            
            current = start_date
            while current <= end_date:
                if current.weekday() == 0:  # Senin
                    iso_year, iso_week, iso_weekday = current.isocalendar()
                    # Month ISO = bulan dari Kamis di minggu ISO
                    month_iso = (current + datetime.timedelta(days=3)).month
                    rows.append({
                        "DT": current,
                        "ISO_Week": iso_week,
                        "Tahun_ISO": iso_year,
                        "Month_ISO": month_iso,
                        "WEEK_IN_MONTH": 0
                    })
                current += datetime.timedelta(days=1)
            
            # Hitung WEEK_IN_MONTH per Month ISO
            df = pd.DataFrame(rows)
            df = df.sort_values(['Tahun_ISO','Month_ISO','DT']).reset_index(drop=True)
            df['WEEK_IN_MONTH'] = df.groupby(['Tahun_ISO','Month_ISO']).cumcount() + 1
            return df

        # Generate dataframe
        df_iso_week = generate_iso_week_table_parquet(2025, 2026)

        # Simpan ke Parquet
        df_iso_week.to_parquet("iso_week_2025_2026.parquet", index=False)


        # =========================
        # FINAL QUERY + GRAND TOTAL
        # =========================
        sql = f"""
        WITH base AS (
                SELECT
                    UPPER(TRIM(SKU)) AS SKU,
                    REGION,
                    AREA,
                    DISTRIBUTOR,
                    "SALES OFFICE",
                    "GROUP",
                    TRY_CAST(Value AS DOUBLE) AS Value,
                    CAST(WEEK AS INTEGER)  AS ISO_WEEK,
                    CAST(TAHUN AS INTEGER) AS TAHUN,
                    DATE '1899-12-30' + CAST(TANGGAL AS INTEGER) AS DT
                FROM "{parquet_path}"
                {where_sql}
        ),

        -- =========================
        -- LIST SKU (gabungan sales + target)
        -- =========================
        sku_list AS (
            SELECT DISTINCT SKU FROM base
        ),


        -- =========================
        -- AGGREGATE BULANAN
        -- =========================
        monthly_agg AS (
            SELECT
                SKU,
                {','.join(month_exprs)},
                AVG(
                    CASE
                        WHEN (TAHUN * 12 + EXTRACT(MONTH FROM DT))
                        BETWEEN ({tahun_akhir} * 12 + {bulan_akhir} - 11)
                            AND ({tahun_akhir} * 12 + {bulan_akhir})
                        THEN Value
                    END
                ) AS "{avg12m_label}",
                AVG(
                    CASE
                        WHEN (TAHUN * 12 + EXTRACT(MONTH FROM DT))
                        BETWEEN ({tahun_akhir} * 12 + {bulan_akhir} - 2)
                            AND ({tahun_akhir} * 12 + {bulan_akhir})
                        THEN Value
                    END
                ) AS "{avg3m_label}"
            FROM base
            GROUP BY SKU
        ),

        -- =========================
        -- AGGREGATE BULANAN (previous year)
        -- =========================
        monthly_agg_prev AS (
            SELECT
                SKU,
                {','.join(month_exprs)}
            FROM base
            WHERE TAHUN = {tahun_hist}-1
            GROUP BY SKU
        ),

        -- =========================
        -- BACA ISO WEEK TABLE DARI EXCEL
        -- =========================
        month_week_map AS (
            SELECT *
            FROM 'iso_week_2025_2026.parquet'
        ),

        -- =========================
        -- MAP DATA KE WEEK BULAN TERPILIH
        -- =========================
        week_map AS (
            SELECT
                b.SKU,
                b.Value,
                m.WEEK_IN_MONTH
            FROM base b
            JOIN month_week_map m
                ON b.ISO_WEEK = m.ISO_Week
                AND b.TAHUN = m.Tahun_ISO
            WHERE m.Month_ISO = {bulan_hist}   -- bulan ISO yang dipilih
            AND m.Tahun_ISO = {tahun_hist}   -- tahun ISO yang dipilih
        ),
        -- =========================
        -- AGGREGATE WEEKLY
        -- =========================
        weekly_agg AS (
            SELECT
                SKU,
                SUM(CASE WHEN WEEK_IN_MONTH = 1 THEN Value END) AS W1,
                SUM(CASE WHEN WEEK_IN_MONTH = 2 THEN Value END) AS W2,
                SUM(CASE WHEN WEEK_IN_MONTH = 3 THEN Value END) AS W3,
                SUM(CASE WHEN WEEK_IN_MONTH = 4 THEN Value END) AS W4,
                SUM(CASE WHEN WEEK_IN_MONTH = 5 THEN Value END) AS W5
            FROM week_map
            GROUP BY SKU
        ),

        
        -- =========================
        -- TARGET BY SKU
        -- =========================
        target_agg AS (
            SELECT
                UPPER(TRIM(SKU)) AS SKU,
                COALESCE(SUM(TRY_CAST(Value AS DOUBLE)), 0) AS Target
            FROM 'data/parquet/target/*.parquet'
            WHERE CAST(TAHUN AS INTEGER) = {tahun_hist}
            AND CAST(MONTH AS INTEGER) = {bulan_hist}
            GROUP BY SKU
        ),


        -- =========================
        -- PIVOT FINAL + GROWTH PER SKU
        -- =========================
        pivoted AS (
            SELECT
                s.SKU,
                {','.join([f'm."{lbl}"' for lbl in month_labels])},
                m."{avg12m_label}",
                m."{avg3m_label}",

                COALESCE(w.W1,0) AS "Historical Week: W1 {calendar.month_abbr[bulan_hist]}-{tahun_hist}",
                COALESCE(w.W2,0) AS "Historical Week: W2 {calendar.month_abbr[bulan_hist]}-{tahun_hist}",
                COALESCE(w.W3,0) AS "Historical Week: W3 {calendar.month_abbr[bulan_hist]}-{tahun_hist}",
                COALESCE(w.W4,0) AS "Historical Week: W4 {calendar.month_abbr[bulan_hist]}-{tahun_hist}",
                COALESCE(w.W5,0) AS "Historical Week: W5 {calendar.month_abbr[bulan_hist]}-{tahun_hist}",

                COALESCE(w.W1,0)
                + COALESCE(w.W2,0)
                + COALESCE(w.W3,0)
                + COALESCE(w.W4,0)
                + COALESCE(w.W5,0)
                    AS "Total Historical Week",

                COALESCE(t.Target, 0) AS Target,

                -- GROWTH (%)
                CASE
                    WHEN COALESCE(m_prev."{month_labels[bulan_hist-1]}", 0) = 0 THEN NULL
                    ELSE ((COALESCE(m."{month_labels[bulan_hist-1]}",0) - COALESCE(m_prev."{month_labels[bulan_hist-1]}",0))
                        / COALESCE(m_prev."{month_labels[bulan_hist-1]}",0)) * 100
                END AS "Growth (%)"

            FROM sku_list s
            LEFT JOIN monthly_agg m ON s.SKU = m.SKU
            LEFT JOIN monthly_agg_prev m_prev ON s.SKU = m_prev.SKU
            LEFT JOIN weekly_agg w ON s.SKU = w.SKU
            LEFT JOIN target_agg t ON s.SKU = t.SKU
        ),

        -- =========================
        -- GRAND TOTAL
        -- =========================
        grand_total AS (
            SELECT
                'GRAND TOTAL' AS SKU,
                {','.join([f'SUM("{lbl}") AS "{lbl}"' for lbl in month_labels])},
                SUM("{avg12m_label}") AS "{avg12m_label}",
                SUM("{avg3m_label}") AS "{avg3m_label}",
                SUM("Historical Week: W1 {calendar.month_abbr[bulan_hist]}-{tahun_hist}") AS "W1 {calendar.month_abbr[bulan_hist]}-{tahun_hist}",
                SUM("Historical Week: W2 {calendar.month_abbr[bulan_hist]}-{tahun_hist}") AS "W2 {calendar.month_abbr[bulan_hist]}-{tahun_hist}",
                SUM("Historical Week: W3 {calendar.month_abbr[bulan_hist]}-{tahun_hist}") AS "W3 {calendar.month_abbr[bulan_hist]}-{tahun_hist}",
                SUM("Historical Week: W4 {calendar.month_abbr[bulan_hist]}-{tahun_hist}") AS "W4 {calendar.month_abbr[bulan_hist]}-{tahun_hist}",
                SUM("Historical Week: W5 {calendar.month_abbr[bulan_hist]}-{tahun_hist}") AS "W5 {calendar.month_abbr[bulan_hist]}-{tahun_hist}",
                SUM("Total Historical Week") AS "Total Historical Week",
                SUM(Target) AS Target,
                NULL AS "Growth (%)"
            FROM pivoted
        ),


        final AS (
            SELECT * FROM pivoted
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

        # pastikan numeric
        for c in df.columns:
            if c != "SKU":
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # =========================
        # SHOW TABLE
        # =========================
        st.markdown(
                    f"""
                    <style>
                    .hover-box3 {{
                        border: 1px solid #1D546D;
                        border-radius: 10px;
                        padding: 5px;
                        text-align: center;
                        background-color: #1D546D;
                        color: white;
                        transition: 0.3s;
                        position: relative;
                        margin-top: 1px;
                        font-size: 18px;
                        font-family: 'Poppins', sans-serif;
                    }}
                    .hover-box3:hover {{
                        background-color: #1D546D;
                        transform: scale(1.01);
                    }}
                    .download-btn {{
                        display: none;
                        margin-top: 10px;
                    }}
                    .hover-box3:hover .download-btn {{
                        display: block;
                    }}
                    a.download-link {{
                        color: white;
                        text-decoration: none;
                        padding: 5px 10px;
                        background-color: #1D546D;
                        border-radius: 5px;
                        font-weight: bold;
                    }}
                    </style>

                    <div class="hover-box3">
                        <strong>SUMMARY</strong>
                    </div>
                    <p></p>
                    """, unsafe_allow_html=True
                )
        st.badge(f"Periode Pivot: {month_labels[0]} → {month_labels[-1]} (Closed Month)", color='blue')
        df_display = df.copy()
        df_display["SKU"] = df_display["SKU"].astype(str)

        df_display.loc[
            df_display["SKU"] == "GRAND TOTAL",
            "SKU"
        ] = "🔹 GRAND TOTAL"
        st.dataframe(
            df_display,
            use_container_width=True
        )

        # =========================
        # DOWNLOAD BUTTON (angka tetap numeric)
        # =========================
        df_download = df.copy()

        # =========================
        # Siapkan df_download
        # =========================
        df_download = df.copy()

        # Loop semua kolom kecuali SKU
        for c in df_download.columns:
            if c != "SKU":
                # Convert ke numeric, coerce jika ada error
                df_download[c] = pd.to_numeric(df_download[c], errors='coerce')
                # Bulatkan 2 desimal (atau gunakan .0 untuk bulat)
                df_download[c] = df_download[c].round(2)  # pakai 0 untuk integer: .round(0)
                
        st.download_button(
            label="📥 Download Historical Summary",
            data=df_download.to_csv(index=False),
            file_name=f"Historical Summary Sales Tahun {tahun_hist}.csv",
            mime="text/csv"
        )


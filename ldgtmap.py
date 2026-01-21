import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px
import numpy as np
from pathlib import Path



def normalize_for_parquet(df):
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)
    return df

def ldgtmap():
    LDGT_DIR = Path("data/ldgt")
    LDGT_DIR.mkdir(parents=True, exist_ok=True)

    PARQUET_FILE = LDGT_DIR / "latest.parquet"
    DEFAULT_PARQUET = LDGT_DIR / "default.parquet"
    
    st.title("LDGT Dashboard")

    # =========================
    # Buat 3 tabs
    # =========================
    tab1, tab2, tab3 = st.tabs(["Upload Data", "View Data", "Analytics"])

    # =========================
    # TAB 1: Upload File
    # =========================
    with tab1:
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
                                    <strong>UPLOAD FILE</strong>
                                </div>
                                <p></p>
                                """, unsafe_allow_html=True
                            )
        st.info(
            "Harap memasukkan file yang akan di mapping dengan struktur kolomnya: "
            "Tahun | Month | Distributor | Cabang | SKU | Value | SKU | KET"
        )
        st.info("Jika terdapat data baru maka upload file tipe excel untuk lakukan replace")

        with st.container(border=True):
            uploaded_file = st.file_uploader(
                "Pilih file",
                type=["csv", "xlsx"]
            )

            # =========================
            # LOAD DATA EXISTING (AUTO)
            # =========================
            if uploaded_file is None:
                if 'df' not in st.session_state:
                    if PARQUET_FILE.exists():
                        st.session_state['df'] = pd.read_parquet(PARQUET_FILE)
                        st.success("📦 Data terakhir berhasil dimuat (Parquet)")
                    elif DEFAULT_PARQUET.exists():
                        st.session_state['df'] = pd.read_parquet(DEFAULT_PARQUET)
                        st.info("📂 Menggunakan data default")
                    else:
                        st.warning("Belum ada data tersimpan")

            # =========================
            # PROSES FILE BARU
            # =========================
            if uploaded_file is not None:
                st.write(f"📄 File dipilih: **{uploaded_file.name}**")

                if st.button("🚀 Proses & Simpan", key="proses_ldgt"):
                    with st.spinner("Memproses data..."):
                        # baca file
                        if uploaded_file.name.endswith(".csv"):
                            df = pd.read_csv(uploaded_file)
                        else:
                            df = pd.read_excel(uploaded_file)

                        
                        # 🔥 WAJIB: normalisasi object → string
                        for col in df.select_dtypes(include=['object']).columns:
                            df[col] = df[col].astype(str)

                        # simpan parquet (overwrite)
                        df.to_parquet(PARQUET_FILE, index=False)

                        # simpan ke session
                        st.session_state['df'] = df

                    st.success(
                        f"✅ Data berhasil disimpan "
                        f"({len(df):,} baris)"
                    )


    # =========================
    # TAB 2: View Data
    # =========================0
    with tab2:
        if 'df' in st.session_state:
            df = st.session_state['df']
            

            with st.container(border=True):
                col1, col2 = st.columns(2)
                total_value = df['NET VALUE'].sum()
                col1.metric("📊 Total Data", f"{len(df):,}")
                col2.metric("💰 Total Value", f"{total_value:,.2f}" if total_value else "—")

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
                        <strong>TABEL DATASET</strong>
                    </div>
                    <p></p>
                    """, unsafe_allow_html=True
                )
            st.dataframe(df)

            # Tombol Download CSV
            csv_data = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Data as CSV",
                data=csv_data,
                file_name='Data Sales LDGT Resize.csv',
                mime='text/csv'
            )

        else:
            st.info("Silakan upload file dulu di tab Upload Data.")




    # =========================
    # TAB 3: Analytics - Mapping (PyDeck)
    # =========================
    with tab3:

        st.markdown(
            """
            <style>
            .hover-box1 {
                border: 1px solid #233D4D;
                border-radius: 10px;
                padding: 6px;
                text-align: center;
                background-color: #233D4D;
                color: white;
                font-size: 18px;
                font-family: 'Poppins', sans-serif;
            }
            </style>

            <div class="hover-box1">
                <strong>MAPPING SALES LDGT BY AREA SUMATERA</strong>
            </div>
            <p></p>
            """,
            unsafe_allow_html=True
        )

        if 'df' not in st.session_state:
            st.info("Silakan upload file dulu di tab Upload Data.")
            st.stop()

        df = st.session_state['df'].copy()

        # =====================================================
        # NORMALISASI DATA (ANTI ERROR)
        # =====================================================
        df['Tahun'] = pd.to_numeric(df['Tahun'], errors='coerce')
        df['Month'] = pd.to_numeric(df['Month'], errors='coerce')
        df['NET VALUE'] = pd.to_numeric(df['NET VALUE'], errors='coerce')

        df = df.dropna(subset=['Tahun', 'Month'])

        # =====================================================
        # FILTER SECTION
        # =====================================================
        st.subheader("🎛️ Filter Data")

        colf1, colf2, colf3 = st.columns(3)
        colf4, colf5 = st.columns(2)

        # ---------- UNIQUE VALUES ----------
        list_cabang = sorted(df['CABANG'].dropna().unique())
        list_dist   = sorted(df['DISTRIBUTOR'].dropna().unique())
        list_sku    = sorted(df['SKU'].dropna().unique())

        # ---------- MULTISELECT ----------
        with colf1:
            f_cabang = st.multiselect("Cabang", list_cabang, default=list_cabang)

        with colf2:
            f_dist = st.multiselect("Distributor", list_dist, default=list_dist)

        with colf3:
            f_sku = st.multiselect("SKU", list_sku, default=list_sku)

        # ---------- RANGE SLIDER ----------
        min_year, max_year = int(df['Tahun'].min()), int(df['Tahun'].max())
        min_month, max_month = 1, 12

        with colf4:
            year_range = st.slider(
                "Range Tahun",
                min_value=min_year,
                max_value=max_year,
                value=(min_year, max_year)
            )

        with colf5:
            month_range = st.slider(
                "Range Month",
                min_value=min_month,
                max_value=max_month,
                value=(min_month, max_month)
            )

        # =====================================================
        # APPLY FILTER
        # =====================================================
        df = df[
            df['CABANG'].isin(f_cabang) &
            df['DISTRIBUTOR'].isin(f_dist) &
            df['SKU'].isin(f_sku) &
            df['Tahun'].between(year_range[0], year_range[1]) &
            df['Month'].between(month_range[0], month_range[1])
        ]

        if df.empty:
            st.warning("Data kosong setelah filter.")
            st.stop()

        # =====================================================
        # LOOKUP CABANG → LAT/LON
        # =====================================================
        cabang_lookup = {
            "Banda Aceh": (5.5483, 95.3238),
            "Bengkulu": (-3.8000, 102.2650),
            "Lampung": (-5.4296, 105.2620),
            "Jambi": (-1.6100, 103.6100),
            "Kotabumi": (-5.4547, 105.7716),
            "Lhokseumawe": (5.1919, 97.1456),
            "Medan": (3.5952, 98.6722),
            "Metro": (-5.1156, 105.2983),
            "Padang": (-0.9491, 100.3543),
            "Palembang": (-2.9761, 104.7754),
            "Pekanbaru": (0.5333, 101.4500),
            "Pematang Siantar": (2.9639, 99.0621)
        }

        df['lat'] = df['CABANG'].map(lambda x: cabang_lookup.get(x, (None, None))[0])
        df['lon'] = df['CABANG'].map(lambda x: cabang_lookup.get(x, (None, None))[1])

        # =====================================================
        # AGGREGATION
        # =====================================================
        agg = (
            df.dropna(subset=['lat', 'lon'])
            .groupby(['CABANG', 'KET', 'lat', 'lon'], as_index=False)
            .agg(
                jumlah=('NET VALUE', 'count'),
                total_value=('NET VALUE', 'sum')
            )
        )

        if agg.empty:
            st.warning("Data tidak cukup untuk ditampilkan.")
            st.stop()

        # =====================================================
        # STYLE MAP
        # =====================================================
        agg['KET'] = agg['KET'].astype(str).str.strip().str.upper()

        color_map = {
            "SELL IN":  [220, 38, 38, 180],
            "SELL OUT": [234, 179, 8, 180]
        }

        agg['color'] = agg['KET'].map(color_map)
        agg['radius'] = np.sqrt(agg['jumlah']) * 500

        agg['value_rp'] = (
            agg['total_value']
            .fillna(0)
            .astype(int)
            .apply(lambda x: f"Rp {x:,.0f}".replace(",", "."))
        )

        # =====================================================
        # PYDECK LAYER
        # =====================================================
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=agg,
            get_position='[lon, lat]',
            get_radius='radius',
            get_fill_color='color',
            pickable=True,
            auto_highlight=True
        )

        view_state = pdk.ViewState(
            latitude=agg['lat'].mean(),
            longitude=agg['lon'].mean(),
            zoom=6.5
        )

        tooltip = {
            "html": """
                <b>{CABANG}</b><br/>
                TYPE: {KET}<br/>
                NET VALUE: {value_rp}
            """,
            "style": {"color": "white"}
        }

        st.pydeck_chart(
            pdk.Deck(
                layers=[layer],
                initial_view_state=view_state,
                tooltip=tooltip
            )
        )
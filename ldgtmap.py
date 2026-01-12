import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px
import numpy as np
from pathlib import Path



LDGT_DIR = Path("data/ldgt")
LDGT_DIR.mkdir(parents=True, exist_ok=True)

LDGT_FILE = LDGT_DIR / "latest.xlsx"
DEFAULT_FILE = LDGT_DIR / "default.xlsx"

def ldgtmap():
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

        with st.container(border=True):
            uploaded_file = st.file_uploader(
                "Pilih file",
                type=["csv", "xlsx"]
            )

            if uploaded_file is not None:
                st.write(f"📄 File dipilih: **{uploaded_file.name}**")

                if st.button("🚀 Proses", key="prosesldgt"):
                    # =========================
                    # Baca file
                    # =========================
                    if uploaded_file.name.endswith(".csv"):
                        df = pd.read_csv(uploaded_file)
                    else:
                        df = pd.read_excel(uploaded_file)

                    # =========================
                    # Simpan ke storage (overwrite)
                    # =========================
                    df.to_excel(LDGT_FILE, index=False)

                    # =========================
                    # Simpan ke session
                    # =========================
                    st.session_state['df'] = df

                    st.success(
                        f"✅ Data berhasil diproses & disimpan "
                        f"({len(df):,} baris)"
                    )
            else:
                st.warning("Silakan pilih file terlebih dahulu.")


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
                            <strong>MAPPING SALES LDGT BY AREA SUMATERA</strong>
                        </div>
                        <p></p>
                        """, unsafe_allow_html=True
                    )

        if 'df' in st.session_state:
            df = st.session_state['df']

            # =========================
            # Lookup CABANG → lat/lon
            # =========================
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

            # =========================
            # Tambah lat/lon
            # =========================
            if 'lat' not in df.columns or 'lon' not in df.columns:
                df['lat'] = df['CABANG'].map(lambda x: cabang_lookup.get(x, (None, None))[0])
                df['lon'] = df['CABANG'].map(lambda x: cabang_lookup.get(x, (None, None))[1])
                st.session_state['df'] = df

            # =========================
            # Clean & aggregate
            # =========================
            df['NET VALUE'] = pd.to_numeric(df['NET VALUE'], errors='coerce')

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

            # =========================
            # Warna berdasarkan KET
            # =========================
            agg['KET'] = (
                    agg['KET']
                    .astype(str)
                    .str.strip()
                    .str.upper()
                )
            color_map = {
                "SELL IN": [220, 38, 38, 180],
                "SELL OUT":  [234, 179, 8, 180]
            }

            agg['color'] = agg['KET'].map(color_map)
            # =========================
            # Bubble Layer
            # =========================
            agg['radius'] = np.sqrt(agg['jumlah']) * 500

            # Pastikan total_value numeric
            agg['total_value'] = pd.to_numeric(agg['total_value'], errors='coerce').fillna(0)
            # === BUAT value_rp SEBELUM layer ===
            agg['value_rp'] = (
                agg['total_value']
                .fillna(0)
                .astype(float)
                .round(0)
                .astype(int)
                .apply(lambda x: f"Rp {x:,.0f}".replace(",", "."))
            )
            layer = pdk.Layer(
                "ScatterplotLayer",
                data=agg,
                get_position='[lon, lat]',
                get_radius='radius',
                get_fill_color='color',
                pickable=True,
                auto_highlight=True
            )

            # =========================
            # View State (lebih dekat)
            # =========================
            view_state = pdk.ViewState(
                latitude=agg['lat'].mean(),
                longitude=agg['lon'].mean(),
                zoom=6.5,
                pitch=0
            )

            # =========================
            # Tooltip
            # =========================
            tooltip = {
                "html": """
                <b>{CABANG}</b><br/>
                DATA: {KET}<br/>
                NET VALUE: {value_rp}
                """,
                "style": {"color": "white"}
            }

            # =========================
            # Render
            # =========================
            st.pydeck_chart(
                pdk.Deck(
                    layers=[layer],
                    initial_view_state=view_state,
                    tooltip=tooltip
                )
            )

        else:
            st.info("Silakan upload file dulu di tab Upload Data.")

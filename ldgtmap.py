import streamlit as st
import pandas as pd




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
        st.subheader("Upload File LDGT (CSV / Excel)")
        uploaded_file = st.file_uploader("Pilih file", type=["csv", "xlsx"])

        if uploaded_file is not None:
            if uploaded_file.name.endswith(".csv"):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)

            st.success("File berhasil diupload!")
            # Simpan di session state supaya bisa dipakai tab lain
            st.session_state['df'] = df

    # =========================
    # TAB 2: View Data
    # =========================
    with tab2:
        if 'df' in st.session_state:
            df = st.session_state['df']
            st.subheader("Dataframe LDGT")
            st.dataframe(df)

            st.markdown(f"**Total Baris:** {len(df)}")

            # Asumsi ada kolom 'Value' untuk total value
            if 'Value' in df.columns:
                total_value = df['Value'].sum()
                st.markdown(f"**Total Value Keseluruhan:** {total_value}")
            else:
                st.warning("Kolom 'Value' tidak ditemukan.")
        else:
            st.info("Silakan upload file dulu di tab Upload Data.")

    # =========================
    # TAB 3: Analytics - Mapping
    # =========================
    with tab3:
        if 'df' in st.session_state:
            df = st.session_state['df']

            st.subheader("Mapping LDGT")

            # Pastikan ada kolom lat & lon
            if 'lat' in df.columns and 'lon' in df.columns:
                # Pilihan kategori (opsional)
                if 'Kategori' in df.columns:
                    kategori_list = df['Kategori'].unique().tolist()
                    selected_kategori = st.multiselect("Pilih Kategori", kategori_list, default=kategori_list)
                    map_data = df[df['Kategori'].isin(selected_kategori)]
                else:
                    map_data = df

                st.map(map_data[['lat', 'lon']])
            else:
                st.warning("Kolom 'lat' dan 'lon' harus ada di data untuk menampilkan peta.")
        else:
            st.info("Silakan upload file dulu di tab Upload Data.")
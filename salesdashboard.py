from streamlit_option_menu import option_menu
import streamlit as st

# Page config
st.set_page_config(page_title="BSI Dashboard", layout="wide")

# NAVIGATION BAR (ATAS)
st.title(" BSI Dashboard")
st.info("Selamat Datang di Website BSI Dashboard!")
selected = option_menu(
    menu_title= None,
    options=["Introduction","Dashboard Sales HCO","Dashboard Mapping LD GT"],
    icons=["calendar", "book", "archive"],
    orientation="horizontal",
    styles={
        "container": {"padding": "0!important", "background-color": "#f0f2f6"},
        "icon": {"color": "black", "font-size": "18px"},
        "nav-link": {
            "font-size": "16px",
            "text-align": "center",
            "margin": "0px",
            "--hover-color": "#eee",
        },
        "nav-link-selected": {"background-color": "#ff4b4b", "color": "white"},
    }
)

# SECTION CONTROL
if selected == "Introduction":
    st.warning("Perhatikan dengan cermat!")
    st.markdown("""
        <style>
            .hover-zoom-box {
                background-color: #28a745; /* Warna hijau */
                color: white;
                padding: 10px;
                border-radius: 5px;
                transition: transform 0.3s ease, background-color 0.3s ease;
            }
            .hover-zoom-box:hover {
                transform: scale(1.05);
                background-color: #218838; /* Warna hijau lebih gelap */
            }
        </style>
                
        <div class="hover-zoom-box">
            - Website BSI Dashboard ini maintain by BSI Team.<br>
            - BSI Dashboard digunakan untuk summary secara umum dari Sales STT.<br>
            - Diharapkan digunakan dengan baik.<br>
            - Penanggung Jawab : <b> Nuril F. Amanah (Koordinator Business Support Information) </b>
        </div>

    """, unsafe_allow_html=True)
elif selected == "Dashboard Sales HCO":
    import sales
    sales.sales()
elif selected == "Dashboard Mapping LD GT":
    import ldgtmap
    ldgtmap.ldgtmap()

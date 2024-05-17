import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px


st.markdown(
    """
    <style>
    /* Adjust the sidebar width */
    .css-1d391kg {
        width: 20% !important; /* Adjust this percentage as needed */
    }
    /* Adjust the main content width */
    .css-1d3bbye {
        width: 80% !important; /* Adjust this percentage as needed */
        left: 20% !important;  /* This should match the sidebar width */
    }
    /* Adjust the new layout for the main block */
    .css-1y0tads {
        margin-left: 0% !important; /* Adjust this percentage to move content to the left */
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Sidebar with introductory text
st.sidebar.title("Introduction")
st.sidebar.write("""
O Boletim Epidemiológico HIV e Aids, do Departamento de HIV, Aids, Tuberculose, Hepatites Virais e Infecções
Sexualmente Transmissíveis, da Secretaria de Vigilância em Saúde e Ambiente do Ministério da Saúde (Dathi/SVSA/
MS), publicado anualmente, apresenta informações sobre os casos de HIV em gestantes/parturientes, puérperas e
crianças expostas ao risco de transmissão vertical, de infecção pelo HIV e de aids no Brasil, regiões, estados e capitais.
As informações apresentadas descrevem o perfil epidemiológico dessas doenças na visão dos indicadores de saúde
mais relevantes.

As fontes utilizadas para a obtenção dos dados incluem as notificações compulsórias de casos de HIV e aids
no Sistema de Informação de Agravos de Notificação (Sinan) e os registros de óbitos atribuídos à aids no Sistema
de Informações sobre Mortalidade (SIM), além dos dados do Sistema de Controle de Exames Laboratoriais (Siscel)
e do Sistema de Controle Logístico de Medicamentos (Siclom). É importante destacar que algumas variáveis, como
a categoria de exposição, são analisadas exclusivamente com dados oriundos do Sinan, dada a ausência dessas
informações em outros sistemas.

A infecção pelo HIV e a aids fazem parte da Lista Nacional de Notificação Compulsória de Doenças (Portaria
nº 420, de 2 de março de 2022), sendo que a aids é de notificação compulsória desde 1986; a infeção pelo HIV em
gestante, parturiente ou puérpera e criança exposta ao risco de transmissão vertical do HIV, desde 2000 (Portaria nº
993, de 4 de setembro de 2000); e a infecção pelo HIV, desde 2014 (Portaria nº 1.271, de 6 de junho de 2014). Assim, na
ocorrência de casos de infecção pelo HIV ou de aids, estes devem ser reportados às autoridades de saúde. Contudo,
apesar dessa obrigatoriedade, com o emprego do método probabilístico de relacionamento de bases de dados
utilizado na geração das informações constantes neste Boletim, tem-se observado ao longo dos anos uma diminuição
do percentual de casos de aids oriundos do Sinan; assim, no ano de 2022, dos 36.753 casos de aids detectados, 48,2%
provieram do Sinan, 9,0% do SIM e 42,8% do Siscel.
""")

# Create some data
df = pd.DataFrame({
    'Column A': np.random.randint(1, 100, 100),
    'Column B': np.random.randint(1, 100, 100),
    'Category': np.random.choice(['Category 1', 'Category 2', 'Category 3'], 100)
})

# Create tabs
tab1, tab2 = st.tabs(["Main View", "Data Exploration"])

with tab1:
    st.title('My Streamlit App with Columns')
    st.write("""
        This app demonstrates how to use columns in Streamlit to organize content.
        At the top, we have a title and a description. Below, we have two columns:
        the first column contains a table and an interactive slider, while the
        second column contains an interactive Plotly figure.
    """)

    # Create columns
    col1, col2 = st.columns(2)

    # First column with table and slider
    with col1:
        st.write("### Data Table")
        st.write(df)

        slider_value = st.slider('Select a value from Column A', 0, 100, 50)
        filtered_df = df[df['Column A'] >= slider_value]
        st.write("### Filtered Data Table")
        st.write(filtered_df)

    # Second column with interactive Plotly figure
    with col2:
        st.write("### Interactive Plotly Figure")
        fig = px.scatter(df, x='Column A', y='Column B', color='Category', title='Scatter Plot of Column A vs Column B')
        st.plotly_chart(fig)

with tab2:
    st.title('Data Exploration')
    st.write("""
        This tab allows for a more detailed exploration of the data, including
        filtering options and summary statistics.
    """)

    # Filter options
    category_filter = st.selectbox('Select Category', df['Category'].unique())
    filtered_df = df[df['Category'] == category_filter]

    st.write("### Filtered Data Table")
    st.write(filtered_df)

    st.write("### Summary Statistics")
    st.write(filtered_df.describe())

    st.write("### Interactive Plotly Figure")
    fig = px.histogram(filtered_df, x='Column A', title='Histogram of Column A')
    st.plotly_chart(fig)
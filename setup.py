from setuptools import setup, find_packages

setup(
    name="lente-ist",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "numpy",
        "pandas",
        "sqlalchemy",
        "simpledbf",
        "datetime",
        "python-dotenv",
        "streamlit",
        "plotly",
        "tkinter",
        "recordlinkage",
        "re",
        "ujson"
    ],
    author="Higor S. Monteiro",
    author_email="higormonteiros@gmail.com",
    description='''Lente-IST: Biblioteca em python para processamento e avaliação de qualidade 
                   dos bancos de dados do DATASUS relacionados às infecções sexualmente transmissíveis. 
                   De início, os bancos de interesse serão relacionados aos agravos de HIV e AIDS.
                   Lente-IST: Python library for processing and quality check of information contained
                   in DATASUS databases regarding the sexually transmitted infections. At first, the
                   analysis will be focused for the diseases of HIV and AIDS.''',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/higorsmonteiro/lente-ist",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9',
)
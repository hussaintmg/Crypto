import streamlit as st
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.dashboard import CryptoDashboard
from src.etl_pipeline import ETLPipeline


def main():

    # Run ETL once
    if "etl_started" not in st.session_state:
        pipeline = ETLPipeline()
        pipeline.run_etl()
        st.session_state.etl_started = True

    dashboard = CryptoDashboard()
    dashboard.run()


if __name__ == "__main__":
    main()
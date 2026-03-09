import streamlit as st
import sys
import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(ROOT_DIR)
sys.path.append(os.path.join(ROOT_DIR, "src"))

from src.dashboard import CryptoDashboard
from src.etl_pipeline import ETLPipeline


def main():
    pipeline = ETLPipeline()
    pipeline.run_etl()

    dashboard = CryptoDashboard()
    dashboard.run()


if __name__ == "__main__":
    main()
import streamlit as st
import sys
import os

# Ensure the root directory is in the path so imports work correctly
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.dashboard import CryptoDashboard

def main():
    dashboard = CryptoDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()

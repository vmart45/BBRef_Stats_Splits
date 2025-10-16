import streamlit as st
import pandas as pd
from Main import get_splits

st.title("Baseball-Reference Splits Downloader ⚾")

st.write("Enter a Baseball-Reference Player ID, Year, and choose whether you want pitching splits.")

# --- Inputs ---
playerid = st.text_input("Player ID (e.g., troutmi01, skenepa01):")
year = st.number_input("Year (leave 0 for Career)", min_value=0, max_value=2100, value=2024)
pitching = st.checkbox("Pitching Splits", value=False)

if st.button("Get Splits"):
    if not playerid:
        st.warning("Please enter a valid player ID.")
    else:
        with st.spinner("Fetching data... this may take a few seconds ⏳"):
            # Convert year=0 to None for 'Career'
            year_param = None if year == 0 else year

            try:
                # Run the scraper
                result = get_splits(playerid, year=year_param, pitching_splits=pitching)

                # Handle different return types
                if isinstance(result, tuple):
                    data = result[0]
                    level_data = result[1]

                    # Save both CSVs to memory
                    csv_main = data.to_csv().encode("utf-8")
                    csv_level = level_data.to_csv().encode("utf-8")

                    st.success("✅ Data fetched successfully!")

                    st.download_button(
                        label="📥 Download Main Splits CSV",
                        data=csv_main,
                        file_name=f"{playerid}_{year or 'career'}_splits.csv",
                        mime="text/csv",
                    )

                    st.download_button(
                        label="📥 Download Game-Level Splits CSV",
                        data=csv_level,
                        file_name=f"{playerid}_{year or 'career'}_gamelevel.csv",
                        mime="text/csv",
                    )

                else:
                    data = result
                    csv = data.to_csv().encode("utf-8")

                    st.success("✅ Data fetched successfully!")
                    st.download_button(
                        label="📥 Download Splits CSV",
                        data=csv,
                        file_name=f"{playerid}_{year or 'career'}_splits.csv",
                        mime="text/csv",
                    )

            except Exception as e:
                st.error(f"❌ Error: {e}")

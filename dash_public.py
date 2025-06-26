import pandas as pd
import streamlit as st
import os
import duckdb # Import DuckDB

# --- Configuration ---
# Set the page configuration for a wider layout and a title
st.set_page_config(layout="wide", page_title="Caste Data Dashboard")

# Define the path to your PARQUET dataset.
# IMPORTANT: This will now be the direct public URL to your Parquet file.
DATA_PATH = 'https://storage.googleapis.com/jati-data/new_replication_panel.parquet'

# --- Initialize DuckDB Connection ---
# We'll use a simple in-memory connection for querying the Parquet file directly.
con = duckdb.connect(database=':memory:', read_only=False)

# --- Month Name Mapping ---
MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
    7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December"
}

# --- Load Metadata for Dropdowns (optimized for Parquet) ---
@st.cache_data # Cache the metadata loading
def load_and_prepare_metadata(path):
    """
    Checks for file existence and loads unique values for state, year, and month dropdowns
    directly from Parquet using DuckDB. Districts will be loaded dynamically per state.
    """
    try:
        # Use DuckDB to efficiently read distinct values for dropdowns from Parquet
        # DuckDB can directly read from HTTP(s) paths
        # Added force_download=true as suggested by the error message for large files over HTTP
        states = con.execute(f"SELECT DISTINCT state FROM read_parquet('{path}', {{force_download: true}}) ORDER BY state").fetchdf()['state'].tolist()
        years = con.execute(f"SELECT DISTINCT date_year FROM read_parquet('{path}', {{force_download: true}}) ORDER BY date_year").fetchdf()['date_year'].tolist()
        months = con.execute(f"SELECT DISTINCT date_month FROM read_parquet('{path}', {{force_download: true}}) ORDER BY date_month").fetchdf()['date_month'].tolist()

        # For pre-selecting the most common state, we still need a quick query.
        try:
            mode_state_query = f"SELECT state FROM read_parquet('{path}', {{force_download: true}}) GROUP BY state ORDER BY COUNT(*) DESC LIMIT 1"
            mode_state_result = con.execute(mode_state_query).fetchdf()
            most_common_state = mode_state_result['state'].iloc[0] if not mode_state_result.empty else (states[0] if states else None)
        except Exception:
            most_common_state = states[0] if states else None # Fallback if mode query fails

        return states, years, months, most_common_state
    except Exception as e:
        st.error(f"Error preparing metadata from Parquet: {e}. Please ensure the Dropbox link is a direct download link and publicly accessible. 'force_download=true' has been added to queries.")
        st.stop()

# Get the unique values for dropdowns and pre-selection from DuckDB queries on Parquet
all_states, all_years, all_months, most_common_state_for_dropdown = load_and_prepare_metadata(DATA_PATH)

# If no data loaded for dropdowns, stop the app.
if not all_states or not all_years or not all_months:
    st.error("Could not load unique values for filters. Please ensure your Parquet file contains data and the path is correct.")
    st.stop()

# --- Dashboard Title and Introduction ---
st.title("State-wise Caste Distribution Dashboard (Parquet & DuckDB Optimized)")
st.markdown("""
This interactive dashboard explores caste distribution and dominant jatis using a highly optimized
Parquet data file and DuckDB for lightning-fast queries.
Use the dropdowns on the sidebar to filter the data.
""")

# --- Sidebar Filters ---
st.sidebar.header("Filter Data")

# Dropdown for State
selected_state = st.sidebar.selectbox(
    "Select a State:",
    all_states,
    index=all_states.index(most_common_state_for_dropdown) if most_common_state_for_dropdown in all_states else 0
)

# Checkbox for Entire Panel
view_entire_panel = st.sidebar.checkbox("View Entire Panel (All Months & Years)")

# Dropdown for Year (conditionally enabled)
selected_year = st.sidebar.selectbox(
    "Select a Year:",
    all_years,
    index=len(all_years) - 1 if all_years else 0, # Pre-select the latest year
    disabled=view_entire_panel # Disable if "View Entire Panel" is checked
)

# Dropdown for Month (conditionally enabled)
selected_month = st.sidebar.selectbox(
    "Select a Month:",
    all_months,
    index=all_months.index(1) if 1 in all_months else (len(all_months) - 1 if all_months else 0), # Pre-select January if available, else latest
    format_func=lambda x: MONTH_NAMES.get(x, str(x)), # Format numeric month to name
    disabled=view_entire_panel # Disable if "View Entire Panel" is checked
)

# --- Dynamic District Loading ---
@st.cache_data
def get_districts_for_state(path, state):
    """
    Loads unique districts for the selected state from the Parquet file.
    Includes 'Entire State' option.
    """
    try:
        # Query DuckDB for districts specific to the selected state
        # Added force_download=true here as well
        district_query = f"SELECT DISTINCT district FROM read_parquet('{path}', {{force_download: true}}) WHERE state = '{state}' ORDER BY district;"
        districts = con.execute(district_query).fetchdf()['district'].tolist()
        
        # Add 'Entire State' option at the beginning
        if 'Entire State' not in districts:
            districts.insert(0, 'Entire State')
        return districts
    except Exception as e:
        st.error(f"Error loading districts for '{state}': {e}")
        return ['Entire State'] # Fallback

# Get districts relevant to the currently selected state
all_districts_for_selected_state = get_districts_for_state(DATA_PATH, selected_state)

# Dropdown for District
selected_district = st.sidebar.selectbox(
    "Select a District:",
    all_districts_for_selected_state,
    index=0 # 'Entire State' is always at index 0
)

# --- Data Processing with DuckDB on Parquet (Single Optimized Query) ---
@st.cache_data # Cache the results of DuckDB queries for chosen filters
def get_caste_distribution_duckdb_optimized(path, state, year, month, view_entire_panel_flag, district):
    """
    Queries the Parquet file using DuckDB with a single, highly optimized SQL query
    to get filtered data, caste distribution, percentage, and rank.
    Counts unique households if 'view_entire_panel_flag' is true.
    """
    try:
        where_clauses = [
            f"state = '{state}'"
        ]

        # Determine if we count distinct hh_id or all hh_id based on panel view
        count_hh_id_clause = "COUNT(DISTINCT hh_id)" if view_entire_panel_flag else "COUNT(hh_id)"
        sum_count_over_clause = f"SUM({count_hh_id_clause}) OVER ()"

        if not view_entire_panel_flag:
            where_clauses.append(f"date_year = {year}")
            where_clauses.append(f"date_month = {month}")

        if district and district != 'Entire State':
            where_clauses.append(f"district = '{district}'")

        where_str = " AND ".join(where_clauses)

        query = f"""
        SELECT
            caste,
            caste_category,
            {count_hh_id_clause} AS "Households Count",
            ROUND(CAST({count_hh_id_clause} AS DOUBLE) * 100.0 / {sum_count_over_clause}, 2) AS "Percentage (%)",
            ROW_NUMBER() OVER (ORDER BY {count_hh_id_clause} DESC) AS "Rank"
        FROM
            read_parquet('{path}', {{force_download: true}}) -- Added force_download=true here
        WHERE
            {where_str}
        GROUP BY
            caste, caste_category
        ORDER BY
            "Households Count" DESC;
        """
        result_df = con.execute(query).fetchdf()

        # Calculate total households based on the "Households Count" from the result_df
        # which will already be unique if view_entire_panel_flag is True
        total_households_in_state = result_df["Households Count"].sum() if not result_df.empty else 0

        return result_df, total_households_in_state

    except Exception as e:
        st.error(f"Error querying data with DuckDB (Parquet): {e}")
        return pd.DataFrame(), 0 # Return empty DataFrame and 0 households on error

# Get results using the highly optimized DuckDB powered function
ranked_jatis, total_households_in_state = get_caste_distribution_duckdb_optimized(
    DATA_PATH, selected_state, selected_year, selected_month, view_entire_panel, selected_district
)

# --- Display Results ---
if ranked_jatis.empty:
    st.warning("No data found for the selected filters. Please adjust your selections.")
else:
    location_text = f"in {selected_state}"
    if selected_district and selected_district != 'Entire State':
        location_text = f"in {selected_district}, {selected_state}"

    time_text = ""
    if view_entire_panel:
        time_text = "across the entire panel"
    else:
        time_text = f"for {MONTH_NAMES.get(selected_month, str(selected_month))}/{selected_year}"

    st.header(f"Data {location_text} {time_text}")

    st.subheader(f"Total Households {location_text} {time_text}: {total_households_in_state}")

    # Rename columns for clarity in the table display
    ranked_jatis_display = ranked_jatis[[
        'Rank',
        'caste',
        'Households Count',
        'Percentage (%)',
        'caste_category'
    ]].rename(columns={
        'caste': 'Jati (Ranked)',
        'Households Count': 'Number of Households',
        'Percentage (%)': 'Percentage of Households',
        'Caste Category': 'Caste Category' # Corrected this column name, it was 'caste_category' earlier in rename
    })

    # --- Dominant Jati Section ---
    st.markdown("---")
    st.subheader(f"Dominant Jati {location_text} {time_text}")
    if not ranked_jatis.empty:
        dominant_jati_info = ranked_jatis.iloc[0] # The results are already sorted by count descending
        st.info(f"""
        The dominant Jati is **{dominant_jati_info['caste']}** (Caste Category: **{dominant_jati_info['caste_category']}**).
        It represents **{dominant_jati_info['Percentage (%)']}%** of the households {location_text}
        {time_text}.
        """)
    else:
        st.warning("Could not determine dominant Jati as no caste data is available for this period.")

    # --- Full Ranking Table ---
    st.markdown("---")
    st.subheader(f"Relative Rankings of Other Jatis {location_text} {time_text}")
    st.markdown(f"Here is a detailed breakdown of all jatis {location_text} {time_text}:")
    st.dataframe(ranked_jatis_display, use_container_width=True)

    st.markdown("---")
    st.markdown("Data provided by the Culture and Debt Project (CPHS).")

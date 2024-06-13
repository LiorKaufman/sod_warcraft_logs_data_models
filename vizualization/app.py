import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import duckdb
import os
import json
import numpy as np



db_path = os.path.join(os.path.dirname(__file__), '../transform/wcl_dbt/dev.duckdb')

# Load the dataset
con = duckdb.connect(db_path)
data = con.execute('SELECT * FROM rpt_encounters').df()  # Replace with your table name

# Set the title of the Streamlit app
st.title("Basic Analysis of Encounter Report Data")

# Display the dataset
# Explain the data
st.write("""
## About the Data
This dataset contains encounter reports from Warcraft Logs. Each row represents a combat encounter with various attributes such as encounter name, boss ID, kill time, and more.
""")
st.write(data)

# Visualization: Distribution of Kill Times
st.write("## Distribution of Kill Times")

fig, ax = plt.subplots(figsize=(12, 6))
kill_times = data[['fight_name', 'longest_kill_time', 'fastest_kill_time', 'avg_kill_time', 'median_kill_time']]
kill_times.set_index('fight_name').plot(kind='bar', ax=ax)
ax.set_xlabel('Fight Name')
ax.set_ylabel('Time (seconds)')
ax.set_title('Distribution of Kill Times')
st.pyplot(fig)



# Extract the relevant columns
boss_column = 'fight_name'
kills_column = 'kills_per_char'
wipes_column = 'wipes_per_char'

# Create lists to hold parsed data
parsed_kills_data_list = []
parsed_wipes_data_list = []

# Iterate through each row to parse the kills and wipes per character
for index, row in data.iterrows():
    boss = row[boss_column]
    kills_per_char = row[kills_column]
    wipes_per_char = row[wipes_column] if pd.notnull(row[wipes_column]) else {"key": [], "value": []}

    # If kills_per_char is a string, parse it
    if isinstance(kills_per_char, str):
        kills_per_char = json.loads(kills_per_char)
    if isinstance(wipes_per_char, str):
        wipes_per_char = json.loads(wipes_per_char)
    
    # Combine boss and kills per character
    for char, kills in zip(kills_per_char['key'], kills_per_char['value']):
        parsed_kills_data_list.append({'boss': boss, 'character': char, 'kills': kills})

    # Combine boss and wipes per character
    for char, wipes in zip(wipes_per_char['key'], wipes_per_char['value']):
        parsed_wipes_data_list.append({'boss': boss, 'character': char, 'wipes': wipes})

# Convert parsed data to DataFrames
parsed_kills_df = pd.DataFrame(parsed_kills_data_list)
parsed_wipes_df = pd.DataFrame(parsed_wipes_data_list)

# Find the player with the most kills for each boss
max_kills_per_boss = parsed_kills_df.loc[parsed_kills_df.groupby('boss')['kills'].idxmax()]

# Find the player with the most wipes for each boss
max_wipes_per_boss = parsed_wipes_df.loc[parsed_wipes_df.groupby('boss')['wipes'].idxmax()]

# Create columns for side-by-side layout
col1, col2 = st.columns(2)

# Display the tables side by side
with col1:
    st.write("## Player with Most Kills per Boss")
    st.write(max_kills_per_boss)

with col2:
    st.write("## Player with Most Wipes per Boss")
    st.write(max_wipes_per_boss)


# Ensure session state for character selection
if 'selected_char' not in st.session_state:
    st.session_state['selected_char'] = parsed_kills_df['character'].unique()[0]

def update_selected_char():
    st.session_state['selected_char'] = st.session_state.char_select

st.write("## Breakdown by Individual Character")
# Create a selection box for character names
char_names = parsed_kills_df['character'].unique()
selected_char = st.selectbox('Select Character', char_names, index=list(char_names).index(st.session_state['selected_char']), key='char_select', on_change=update_selected_char)

# Filter data based on selected character
filtered_data = parsed_kills_df[parsed_kills_df['character'] == st.session_state['selected_char']]

# Display the filtered data
st.write(f"## Kills per Boss for Character: {st.session_state['selected_char']}")
st.write(filtered_data)

# Visualization: Kills per Boss for the Selected Character
st.write("## Visualization: Kills per Boss for the Selected Character")
fig, ax = plt.subplots(figsize=(10, 8))
filtered_data.plot(kind='bar', x='boss', y='kills', legend=False, ax=ax)
ax.set_xlabel('Boss')
ax.set_ylabel('Number of Kills')
ax.set_title(f"Kills per Boss for {st.session_state['selected_char']}")
st.pyplot(fig)


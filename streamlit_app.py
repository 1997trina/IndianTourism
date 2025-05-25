import streamlit as st
from snowflake.snowpark import Session
import pandas as pd
import plotly.express as px
from streamlit_plotly_events import plotly_events

st.set_page_config(layout="wide")

# Create Snowflake session
conn = st.secrets["connections"]["snowflake"]
session = Session.builder.configs(conn).create()

tab1, tab2 = st.tabs(["Festivals and Pilgrimage", "Experience & Adventure Sports"])



with tab1:
    
    st.title("🎉 Indian Cultural Insights Dashboard")

    # Get distinct states for slicer
    states_df = session.sql("""
    SELECT STATE FROM (
        SELECT DISTINCT STATE FROM PRASHAD
        UNION
        SELECT DISTINCT STATE FROM FAIRSANDCARNIVALSBYSTATE
        UNION
        SELECT DISTINCT  CASE 
        WHEN STATE = 'Uttrakhand' THEN 'Uttarakhand' 
        ELSE STATE 
    END AS STATE 
FROM TRAVELPROVIDERS
    ) ORDER BY STATE
""").to_pandas()

    state_list = states_df["STATE"].tolist()
    selected_state = st.selectbox("Select a State", ["All"] + state_list)

    # Define dynamic WHERE clause
    where_clause = f"WHERE STATE = '{selected_state}'" if selected_state != "All" else ""
    state_label = selected_state if selected_state != "All" else "All States"

    # Queries for unified summary table
    fairs_summary_query = f"""
        SELECT SANCTIONYEAR,
               SUM(AMOUNTRELEASED) AS AMOUNT_RELEASED_BY_GOV,
               COUNT(*) AS PROJECT_OR_FESTIVAL_COUNT,
               'Festival' AS CATEGORY
        FROM FAIRSANDCARNIVALSBYSTATE
        {where_clause}
        GROUP BY SANCTIONYEAR
    """

    prashad_summary_query = f"""
        SELECT SANCTIONYEAR,
               SUM(APPROVEDCOST) AS AMOUNT_RELEASED_BY_GOV,
               COUNT(*) AS PROJECT_OR_FESTIVAL_COUNT,
               'Pilgrimage' AS CATEGORY
        FROM PRASHAD
        GROUP BY SANCTIONYEAR
    """

    # Top 6 entries
    fairs_top_query = f"""
    SELECT STATE,
           NAMEOFFAIRS AS NAME,
           SUM(AMOUNTRELEASED) AS AMOUNT
    FROM FAIRSANDCARNIVALSBYSTATE
    {where_clause}
    GROUP BY STATE, NAMEOFFAIRS
    ORDER BY AMOUNT DESC
    LIMIT 6
    """


    prashad_top_query = f"""
        SELECT PROJECTNAME AS NAME,
               SUM(APPROVEDCOST) AS AMOUNT
        FROM PRASHAD
        {where_clause}
        GROUP BY PROJECTNAME
        ORDER BY AMOUNT DESC
        LIMIT 6
    """

    # Load data
    df_fairs_summary = session.sql(fairs_summary_query).to_pandas()
    df_prashad_summary = session.sql(prashad_summary_query).to_pandas()

    df_summary = pd.concat([df_fairs_summary, df_prashad_summary], ignore_index=True).sort_values(by=["SANCTIONYEAR", "CATEGORY"])

    df_fairs_top = session.sql(fairs_top_query).to_pandas()
    df_prashad_top = session.sql(prashad_top_query).to_pandas()
    df_prashad_top = df_prashad_top[~df_prashad_top["NAME"].str.contains("TOTAL", case=False, na=False)]


    # Display combined summary table
    st.subheader("🧾 Combined Yearly Summary")
    st.dataframe(df_summary, use_container_width=True)

    # Festival chart
    st.subheader(f"🎭 Top 6 Funded Festivals in {state_label}")
    fig_fairs = px.bar(
    df_fairs_top.sort_values("AMOUNT", ascending=False),
    x="AMOUNT",
    y="NAME",
    orientation="h",
    labels={"AMOUNT": "₹ Funding (in lakh)", "NAME": "Festival"},
    title="Top 6 Festivals by Government Funding",
    hover_data=["STATE"]  # 👈 this adds state info to the tooltip
    )
    fig_fairs.update_layout(
    yaxis=dict(autorange="reversed"),
    margin=dict(t=40, b=40),
    height=400,
    )
    st.plotly_chart(fig_fairs, use_container_width=True)


    # Pilgrimage chart
    st.subheader(f"🛕 Top 6 Pilgrimage Projects in {state_label}")
    fig_prashad = px.bar(
        df_prashad_top.sort_values("AMOUNT", ascending=False),
        x="AMOUNT",
        y="NAME",
        orientation="h",
        labels={"AMOUNT": "₹ Approved Cost (in crores)", "NAME": "Project"},
        title="Top 6 Pilgrimage Projects by Approved Cost"
    )
    fig_prashad.update_layout(
        yaxis=dict(autorange="reversed"),
        margin=dict(t=40, b=40),
        height=400,
    )
    st.plotly_chart(fig_prashad, use_container_width=True)
        
    
    #TRAVEL PROVIDERS
    
    st.subheader("🧭 Travel Providers Overview")

    # Query with or without WHERE clause based on selected_state
    where_clause = f"WHERE STATE = '{selected_state}'" if selected_state != "All" else ""
    
    query_tree = f"""
        SELECT STATE, CATEGORY, ORGANISATION
        FROM TRAVELPROVIDERS
        WHERE STATE <> 'State'
        {f"AND STATE = '{selected_state}'" if selected_state != "All" else ""}
    """
    
    query_treemap = f"""
        SELECT STATE, CATEGORY, COUNT(ORGANISATION) AS NUMBER_OF_ORGANISATIONS
        FROM TRAVELPROVIDERS
        WHERE STATE <> 'State'
        {f"AND STATE = '{selected_state}'" if selected_state != "All" else ""}
        GROUP BY STATE, CATEGORY
        ORDER BY NUMBER_OF_ORGANISATIONS DESC
    """

    # Load data
    df_tree = session.sql(query_tree).to_pandas()
    df_tree["STATE"] = df_tree["STATE"].replace("Uttrakhand", "Uttarakhand")
    df_treemap = session.sql(query_treemap).to_pandas()

    # --- Treemap ---
    st.subheader("🗺️ Travel Providers by State and Category")
    fig_treemap = px.treemap(
        df_treemap,
        path=["STATE", "CATEGORY"],
        values="NUMBER_OF_ORGANISATIONS",
        color="STATE",
        title="Travel Providers Distribution"
    )
    st.plotly_chart(fig_treemap, use_container_width=True)

    # --- Tree View ---
    st.subheader("Details")

    for state in sorted(df_tree['STATE'].unique()):
        with st.expander(f"📍 {state}", expanded=True if selected_state != "All" else False):
            state_df = df_tree[df_tree['STATE'] == state]

            for category in sorted(state_df['CATEGORY'].unique()):
                st.markdown(f"**🔹 {category}**")
                cat_df = state_df[state_df['CATEGORY'] == category]

                for org in sorted(cat_df['ORGANISATION'].unique()):
                    st.markdown(f"- {org}")





with tab2:
    st.title("Newly Funded by GOI Experience & Peak Explorer")

    # Unified state list from both tables
    query_states = """
    SELECT DISTINCT STATE FROM SANCTIONEDPROJECTS23TO25 WHERE STATE<>'Total'
    UNION
    SELECT DISTINCT INITCAP(STATE) AS STATE FROM MOUNTAINSPORTS WHERE STATE<>'State'
    """
    state_list_df = session.sql(query_states).to_pandas()
    state_list = sorted(state_list_df["STATE"].dropna().unique().tolist())

    selected_state = st.selectbox("📍 Filter by State to see details", ["All"] + state_list)

    # --- Experience Chart ---
    exp_query = """
    SELECT STATE, DESTINATION, NAME_OF_EXPERIENCE
    FROM SANCTIONEDPROJECTS23TO25
    """
    df_exp = session.sql(exp_query).to_pandas()

    if selected_state != "All":
        df_exp = df_exp[df_exp['STATE'].str.title() == selected_state]

    if selected_state != "All":
        st.subheader(f"🎡 Experiences in {selected_state}")
        dest_counts = df_exp.groupby('DESTINATION').size().reset_index(name='Experience_Count')
        fig = px.bar(dest_counts, x='DESTINATION', y='Experience_Count',
                     title=f"Experience Counts by Destination in {selected_state}")
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("📝 List of Experiences")
        for name in sorted(df_exp["NAME_OF_EXPERIENCE"].dropna().unique()):
            st.markdown(f"- {name}")
    else:
        state_counts = df_exp.groupby('STATE').size().reset_index(name='Experience_Count')
        fig = px.bar(state_counts, x='STATE', y='Experience_Count',
                     title="Experience Counts by State")
        st.plotly_chart(fig, use_container_width=True)

          # --- Peak Chart ---
    st.title("⛰️ Mountain Peaks and Sports")
    
    df_peaks = session.sql("""
        SELECT INITCAP(STATE) AS STATE, PEAKNAME, HEIGHT, SPORTS 
        FROM MOUNTAINSPORTS 
        WHERE STATE <> 'State'
    """).to_pandas()
    
    if selected_state != "All":
        df_peaks = df_peaks[df_peaks["STATE"] == selected_state]
    
        peak_counts = df_peaks.groupby("STATE").size().reset_index(name="Peak Count")
        fig_peaks = px.bar(peak_counts.sort_values("Peak Count", ascending=True),
                           x="Peak Count", y="STATE", orientation="h",
                           title=f"Mountain Peaks in {selected_state}")
        st.plotly_chart(fig_peaks, use_container_width=True)
    
        # Treemap for peaks grouped by Sports
        st.subheader(f"🌲 Peak Activities in {selected_state}")
        fig_tree = px.treemap(
            df_peaks,
            path=["SPORTS", "PEAKNAME"],
            values=[1]*len(df_peaks),
            custom_data=["HEIGHT"],
            title=f"Treemap of Peaks by Sport in {selected_state}",
            color="HEIGHT",
            color_continuous_scale="Viridis"
        )
        fig_tree.update_traces(
            hovertemplate="<b>%{label}</b><br>Height: %{customdata[0]} m"
        )
        st.plotly_chart(fig_tree, use_container_width=True)
    
    else:
        peak_counts = df_peaks.groupby("STATE").size().reset_index(name="Peak Count")
        fig_peaks = px.bar(peak_counts.sort_values("Peak Count", ascending=True),
                           x="Peak Count", y="STATE", orientation="h",
                           title="Number of Mountain Peaks by State")
        st.plotly_chart(fig_peaks, use_container_width=True)


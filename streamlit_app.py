import streamlit as st
from snowflake.snowpark import Session
import pandas as pd
import plotly.express as px
from streamlit_plotly_events import plotly_events

st.set_page_config(layout="wide")

# Create Snowflake session
conn = st.secrets["connections"]["snowflake"]
session = Session.builder.configs(conn).create()

tab1, tab2, tab3 = st.tabs(["Festivals and Pilgrimage", "Experience & Adventure Sports", "Stats"])



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
        WHERE SANCTIONYEAR!='Total'
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
    st.subheader("🧾 Let's find out how much government has spent on festivals & pilgrimage")
    st.dataframe(df_summary, use_container_width=True)

    # Festival chart
    st.subheader(f"🎭 Most Funded Festivals in {state_label}")
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
    st.subheader(f"🛕 Top Pilgrimage Projects in {state_label}")
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
    
    st.subheader("🧭  Need help with your travel plans : Let's check out the travel providers trusted by our Government")

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
    WHERE STATE<>'Total'
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
    
        st.subheader(f"🌲 Peak Activities in {selected_state}")
        fig_tree = px.treemap(
            df_peaks,
            path=["SPORTS", "PEAKNAME"],
            values=None,
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

    st.subheader("🏺 Museums & Archeology")

    query_museum = """
    SELECT 
      STATE, 
      MUSEUM, 
      TYPE
    FROM MUSEUM
    WHERE STATE != 'Total'
    """
    
    df_museum = session.sql(query_museum).to_pandas()
    
    # Apply state filter
    df_filtered = df_museum if selected_state == "All" else df_museum[df_museum["STATE"] == selected_state]
    
    # Group by TYPE and count museums
    df_count = df_filtered.groupby("TYPE").size().reset_index(name="Museum_Count")
    
    # Apply state filter for grouped bar chart too
    df_filtered = df_museum if selected_state == "All" else df_museum[df_museum["STATE"] == selected_state]
    
    # Group by STATE and TYPE to count museums
    df_grouped = df_filtered.groupby(["STATE", "TYPE"]).size().reset_index(name="Museum_Count")
    
    fig3 = px.bar(
        df_grouped,
        x="STATE",
        y="Museum_Count",
        color="TYPE",
        barmode="group",
        title=f"Number of Museums by State and Type funded by GOI in recent years ({selected_state})" if selected_state != "All" else "Number of Museums by State and Type",
        labels={"Museum_Count": "Number of Museums", "STATE": "State", "TYPE": "Museum Type"},
    )
    
    st.plotly_chart(fig3, use_container_width=True)
    
    # Show detailed museum list if a specific state is selected
    if selected_state != "All":
        df_detail = df_filtered[["MUSEUM", "TYPE"]]
        st.markdown(f"### Museums Details in {selected_state}")
        st.dataframe(df_detail.reset_index(drop=True), use_container_width=True)










with tab3:
    st.title("Travel History")
    # Queries
    query_dtv = """
    SELECT
      v1.states,
      v1.DTV16,
      v1.DTV17,
      v1.DTV18,
      v2.DTV19,
      v2.DTV20,
      v2.DTV21
    FROM visitdata v1
    JOIN visitdata2 v2 ON v1.states = v2.state
    """
    df_dtv = session.sql(query_dtv).to_pandas()

    query_ftv = """
    SELECT
      v1.states,
      v1.FTV16,
      v1.FTV17,
      v1.FTV18,
      v2.FTV19,
      v2.FTV20,
      v2.FTV21
    FROM visitdata v1
    JOIN visitdata2 v2 ON v1.states = v2.state
    """
    df_ftv = session.sql(query_ftv).to_pandas()

    # State selector
    states = ["All"] + sorted(df_dtv["STATES"].unique())
    selected_state = st.selectbox("Select a State", states)

    # Domestic: melt & filter
    df_dtv_long = df_dtv.melt(id_vars=["STATES"], 
                              value_vars=["DTV16","DTV17","DTV18","DTV19","DTV20","DTV21"],
                              var_name="Year",
                              value_name="Visits")
    df_dtv_long["Year"] = df_dtv_long["Year"].str.replace("DTV", "20")

    if selected_state != "All":
        df_dtv_long = df_dtv_long[df_dtv_long["STATES"] == selected_state]

    fig_dtv = px.line(df_dtv_long, x="Year", y="Visits", color="STATES",
                      title=f"Domestic Tourist Visits ({selected_state})" if selected_state != "All" else "Domestic Tourist Visits (All States)")
    st.plotly_chart(fig_dtv, use_container_width=True)

    # Foreign: melt & filter
    df_ftv_long = df_ftv.melt(id_vars=["STATES"], 
                              value_vars=["FTV16","FTV17","FTV18","FTV19","FTV20","FTV21"],
                              var_name="Year",
                              value_name="Visits")
    df_ftv_long["Year"] = df_ftv_long["Year"].str.replace("FTV", "20")

    if selected_state != "All":
        df_ftv_long = df_ftv_long[df_ftv_long["STATES"] == selected_state]

    fig_ftv = px.line(df_ftv_long, x="Year", y="Visits", color="STATES",
                      title=f"Foreign Tourist Visits ({selected_state})" if selected_state != "All" else "Foreign Tourist Visits (All States)")
    st.plotly_chart(fig_ftv, use_container_width=True)


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
    # State selector
    states_df = session.sql("""
        SELECT STATE FROM (
            SELECT DISTINCT STATE FROM PRASHAD WHERE State <> 'Total'
            UNION
            SELECT DISTINCT STATE FROM FAIRSANDCARNIVALSBYSTATE
            UNION
            SELECT DISTINCT CASE 
                WHEN STATE = 'Uttrakhand' THEN 'Uttarakhand' 
                ELSE STATE 
            END AS STATE 
            FROM TRAVELPROVIDERS
            WHERE State <> 'State'
        ) ORDER BY STATE
    """).to_pandas()

    state_list = states_df["STATE"].tolist()
    selected_state = st.selectbox("Select a State", ["All"] + state_list)
    where_clause = f"WHERE STATE = '{selected_state}'" if selected_state != "All" else ""
    state_label = selected_state if selected_state != "All" else "All States"

    # Queries
    fairs_summary_query = f"""
        SELECT SANCTIONYEAR,
               SUM(AMOUNTRELEASED) AS AMOUNT_RELEASED_BY_GOV,
               COUNT(*) AS PROJECT_OR_FESTIVAL_COUNT,
               'Festival' AS CATEGORY
        FROM FAIRSANDCARNIVALSBYSTATE
        {where_clause}
        GROUP BY SANCTIONYEAR
    """
    prashad_where_clause = where_clause + (" AND " if where_clause else "WHERE ") + "PROJECTNAME NOT ILIKE '%total%' AND SANCTIONYEAR != 'Total'"
    
    prashad_summary_query = f"""
        SELECT SANCTIONYEAR,
               SUM(APPROVEDCOST) AS AMOUNT_RELEASED_BY_GOV,
               COUNT(*) AS PROJECT_OR_FESTIVAL_COUNT,
               'Pilgrimage' AS CATEGORY
        FROM PRASHAD
        {prashad_where_clause}
        GROUP BY SANCTIONYEAR
    """



    fairs_top_query = f"""
        SELECT STATE, NAMEOFFAIRS AS NAME, SUM(AMOUNTRELEASED) AS AMOUNT
        FROM FAIRSANDCARNIVALSBYSTATE
        {where_clause}
        GROUP BY STATE, NAMEOFFAIRS
        ORDER BY AMOUNT DESC
        LIMIT 6
    """

    prashad_top_query = f"""
        SELECT PROJECTNAME AS NAME, SUM(APPROVEDCOST) AS AMOUNT
        FROM PRASHAD
        {where_clause + (' AND' if where_clause else 'WHERE')} lower(PROJECTNAME) NOT LIKE '%total%'
        GROUP BY PROJECTNAME
        ORDER BY AMOUNT DESC
        LIMIT 6
    """

    # Load data
    df_fairs_summary = session.sql(fairs_summary_query).to_pandas()
    df_prashad_summary = session.sql(prashad_summary_query).to_pandas()
    df_summary = pd.concat([df_fairs_summary, df_prashad_summary], ignore_index=True)

    df_summary['AMOUNT_RELEASED_LAKH'] = df_summary.apply(
        lambda row: row['AMOUNT_RELEASED_BY_GOV'] * 100 if row['CATEGORY'] == 'Pilgrimage' else row['AMOUNT_RELEASED_BY_GOV'],
        axis=1
    )
    df_summary['SANCTIONYEAR'] = df_summary['SANCTIONYEAR'].astype(str)

    st.markdown("""
    <h3 style="
        color: #800000;
        text-align: center;
        font-family: Georgia, serif;
        padding: 10px 0;
        margin: 0;">
    🧾 Curious where the Government Funding goes? Let’s follow the trail!
    </h3>
    """, unsafe_allow_html=True)

    fig_summary = px.bar(
        df_summary,
        x='SANCTIONYEAR',
        y='AMOUNT_RELEASED_LAKH',
        color='CATEGORY',
        labels={
            'SANCTIONYEAR': 'Sanction Year',
            'AMOUNT_RELEASED_LAKH': 'Amount Released (in Lakhs)'
        },
        color_discrete_map={
            'Pilgrimage': '#800000',
            'Festival': '#F4A460'
        }
    )
    fig_summary.update_layout(barmode='group', xaxis={'type': 'category'}, height=450)
    st.plotly_chart(fig_summary, use_container_width=True)

    # Load top projects/fairs
    df_fairs_top = session.sql(fairs_top_query).to_pandas()
    df_prashad_top = session.sql(prashad_top_query).to_pandas()
    df_prashad_top["AMOUNT_LAKH"] = df_prashad_top["AMOUNT"] * 100

    st.markdown(f"""
    <h2 style="color:#800000; font-family: 'Georgia', serif; font-weight: bold; text-shadow: 1px 1px 2px #ccc; font-size: 24px">
    🎝️ Most Funded Festivals in {state_label}
    </h2>
    """, unsafe_allow_html=True)

    fig_fairs = px.bar(
        df_fairs_top.sort_values("AMOUNT", ascending=False),
        x="AMOUNT", y="NAME", orientation="h",
        labels={"AMOUNT": "₹ Funding (in lakh)", "NAME": "Festival"},
        color_discrete_sequence=['#F4C430']
    )
    fig_fairs.update_layout(yaxis=dict(autorange="reversed"), margin=dict(t=40, b=40), height=400)
    st.plotly_chart(fig_fairs, use_container_width=True)

    st.markdown(f"""
    <div style="color:#800000; font-family: Georgia, serif; font-weight: bold; font-size: 24px;">
    🗕️ Top Pilgrimage Projects in {state_label}
    </div>
    """, unsafe_allow_html=True)

    fig_prashad = px.bar(
        df_prashad_top.sort_values("AMOUNT_LAKH", ascending=False),
        x="AMOUNT_LAKH", y="NAME", orientation="h",
        labels={"AMOUNT_LAKH": "₹ Approved Cost (in lakh)", "NAME": "Project"},
        color_discrete_sequence=['#FF9933']
    )
    fig_prashad.update_layout(yaxis=dict(autorange="reversed"), margin=dict(t=40, b=40), height=400)
    st.plotly_chart(fig_prashad, use_container_width=True)

    # Travel providers
    st.markdown("""
    <div style="color:#800000; font-family: Georgia, serif; font-size: 20px; font-weight: bold;">
    🧽 Need help with your travel plans? Let's check out the travel providers trusted by our Government
    </div>
    """, unsafe_allow_html=True)


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

    df_tree = session.sql(query_tree).to_pandas()
    df_tree["STATE"] = df_tree["STATE"].replace("Uttrakhand", "Uttarakhand")
    df_treemap = session.sql(query_treemap).to_pandas()

    fig_treemap = px.treemap(
        df_treemap,
        path=["STATE", "CATEGORY"],
        values="NUMBER_OF_ORGANISATIONS",
        color="STATE"
    )
    st.plotly_chart(fig_treemap, use_container_width=True)

    if selected_state != "All":
        st.subheader("Details")
        state_df = df_tree[df_tree['STATE'] == selected_state]
        for category in sorted(state_df['CATEGORY'].unique()):
            st.markdown(f"**🔹 {category}**")
            cat_df = state_df[state_df['CATEGORY'] == category]
            for org in sorted(cat_df['ORGANISATION'].unique()):
                st.markdown(f"- {org}")






st.title("Newly Funded by GOI Experiences")

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

        st.markdown(f"""
        <h3 style='color: #808000; font-family: Georgia, serif; font-size: 24px;'>
            🎡 Experiences in {selected_state}
        </h3>
        """, unsafe_allow_html=True)

        dest_counts = df_exp.groupby('DESTINATION').size().reset_index(name='Number of Experiences')
        if not dest_counts.empty and dest_counts["Number of Experiences"].sum() > 0:
            fig = px.bar(dest_counts, x='DESTINATION', y='Number of Experiences',
                         title=f"Experience Counts by Destination in {selected_state}", color_discrete_sequence=['#808000'])
            st.plotly_chart(fig, use_container_width=True)

        for name in sorted(df_exp["NAME_OF_EXPERIENCE"].dropna().unique()):
            st.markdown(f"""
            <div style="
                background-color: #f0f8e0;
                border: 2px solid #808000;
                border-radius: 10px;
                padding: 12px;
                margin-bottom: 10px;
                font-family: 'Georgia', serif;
                color: #4b2e2e;">
                <div style="font-size: 18px;">
                    🎯 <b>{name}</b>
                </div>
            </div>
            """, unsafe_allow_html=True)
    else:
        state_counts = df_exp.groupby('STATE').size().reset_index(name='Number of Experiences')
        fig = px.bar(state_counts, x='STATE', y='Number of Experiences',color_discrete_sequence=['#808000'],
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

        peak_counts = df_peaks.groupby("STATE").size().reset_index(name="Number of Peaks")
        if not peak_counts.empty and peak_counts["Number of Peaks"].sum() > 0:
            fig_peaks = px.bar(peak_counts.sort_values("Number of Peaks", ascending=True),
                               x="Number of Peaks", y="STATE", orientation="h",color_discrete_sequence=['#808000'],
                               title=f"Mountain Peaks in {selected_state}")
            st.plotly_chart(fig_peaks, use_container_width=True)

        if not df_peaks.empty:
            st.markdown(f"""
            <h3 style='color: #808000; font-family: Georgia, serif; font-size: 24px;'>
                🌲 Peak Activities in {selected_state}
            </h3>
            """, unsafe_allow_html=True)

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
        peak_counts = df_peaks.groupby("STATE").size().reset_index(name="Number of Peaks")
        fig_peaks = px.bar(peak_counts.sort_values("Number of Peaks", ascending=True),
                           x="Number of Peaks", y="STATE", orientation="h",color_discrete_sequence=['#808000'],
                           title="Number of Mountain Peaks by State")
        st.plotly_chart(fig_peaks, use_container_width=True)

    st.title("🏺 Museums & Archeology")

    query_museum = """
    SELECT 
        STATE, 
        MUSEUM, 
        CASE 
            WHEN TYPE ILIKE 'Exiting Museum' THEN 'Existing Museum'
            WHEN TYPE ILIKE 'Existing museum' THEN 'Existing Museum'
            WHEN TYPE = 'VEM' THEN 'Visitor Experience Management'
            ELSE TYPE
        END AS TYPE
    FROM MUSEUM
    WHERE STATE != 'Total'"""

    df_museum = session.sql(query_museum).to_pandas()
    df_filtered = df_museum if selected_state == "All" else df_museum[df_museum["STATE"] == selected_state]

    df_grouped = df_filtered.groupby(["STATE", "TYPE"]).size().reset_index(name="Museum_Count")

    color_map = {
        "Existing Museum": "#808000",
        "New Museum": "#A0522D",
        "Modernization of Museum": "#FFC0CB",
        "Visitor Experience Management": "#CC7722"
    }
    
    if not df_grouped.empty and df_grouped["Museum_Count"].sum() > 0:
        fig3 = px.bar(
            df_grouped,
            x="STATE",
            y="Museum_Count",
            color="TYPE",
            barmode="group",
            title=f"Number of Museums by State and Type funded by GOI in recent years ({selected_state})" if selected_state != "All" else "Number of Museums by State and Type",
            labels={"Museum_Count": "Number of Museums", "STATE": "State", "TYPE": "Museum Type"},
            color_discrete_map=color_map
        )
        st.plotly_chart(fig3, use_container_width=True)

    if selected_state != "All" and not df_filtered.empty:
        df_detail = df_filtered[["MUSEUM", "TYPE"]]
        st.markdown(f"""
        <h3 style='color: #808000; font-family: Georgia, serif; font-size: 24px;'>
            🏺 Museums in {selected_state}
        </h3>
        """, unsafe_allow_html=True)

        for _, row in df_detail.iterrows():
            museum = row['MUSEUM']
            mtype = row['TYPE']
            st.markdown(f"""
            <div style="
                background-color: #f0f8e0;
                border: 2px solid #808000;
                border-radius: 10px;
                padding: 12px;
                margin-bottom: 10px;
                font-family: 'Georgia', serif;
                color: #4b2e2e;">
                <div style="font-size: 18px;">
                    🖼️ <b>{museum}</b>
                </div>
                <div style="font-size: 14px; color: #666666; margin-top: 4px;">
                    {mtype}
                </div>
            </div>
            """, unsafe_allow_html=True)

    # UNESCO Sites
    unesco_query = "SELECT * FROM UNESCO WHERE STATE <> 'State'"
    df_unesco = session.sql(unesco_query).to_pandas()

    if selected_state != "All":
        unesco_state_df = df_unesco[df_unesco['STATE'] == selected_state]

        if not unesco_state_df.empty:
            st.markdown("""
            <h3 style='color: #808000; font-family: Georgia, serif; font-size: 26px; text-align: center;'>
                BONUS: UNESCO IDENTIFIED SITE - APR 2025
            </h3>
            """, unsafe_allow_html=True)

            for _, row in unesco_state_df.iterrows():
                site = row['HERITAGESITE']
                site_type = row['TYPE']
                st.markdown(f"""
                <div style="
                    background-color: #F5F5DC;
                    border: 2px solid #808000;
                    border-radius: 10px;
                    padding: 12px;
                    margin-bottom: 10px;
                    font-family: 'Georgia', serif;
                    color: #4b2e2e;">
                    <div style="font-size: 18px;">
                        🌏 <b>{site}</b>
                    </div>
                    <div style="font-size: 14px; color: #666666; margin-top: 4px;">
                        {site_type}
                    </div>
                </div>
                """, unsafe_allow_html=True)

    # RSM Data
    query_rsm = 'SELECT * FROM "TOURISM"."PUBLIC"."RSM"'
    df_rsm = session.sql(query_rsm).to_pandas()

    st.markdown("### 🎨 Rashtriya Sanskriti Mahotsav (RSM)")
    st.markdown("*Rashtriya Sanskriti Mahotsav (RSM) revolves around functions like preservation and conservation of our cultural heritage and promotion of all forms of art and culture, both tangible and intangible.*")
    
    df_filtered_rsm = df_rsm if selected_state == "All" else df_rsm[df_rsm["STATE"] == selected_state]
    st.dataframe(df_filtered_rsm, use_container_width=True)


    # Untraceable Monuments Card
    query_untraceable = "SELECT * FROM UNTRACEABLEMONUMENTS WHERE STATE <> 'State'"
    df_untraceable = session.sql(query_untraceable).to_pandas()
    
    if selected_state != "All":
        df_untraceable_state = df_untraceable[df_untraceable['STATE'] == selected_state]
        
        if not df_untraceable_state.empty:
            random_monument = df_untraceable_state.sample(1).iloc[0]['MONUMENTS']
        
        st.markdown(f"""
        <div style="
            background-color: #fff0e6;
            border-left: 6px solid #ff6600;
            padding: 16px;
            border-radius: 10px;
            margin-top: 20px;
            font-family: Georgia, serif;">
            <h4 style="color: #cc5200;">🧭 Wanna explore more?</h4>
            <p style="margin: 0; color: #333;">
                <b>{random_monument}</b> has been listed as <i>untraceable</i> by the ASI.
            </p>
            <p style="font-size: 13px; color: #666;">
                A piece of our heritage, possibly lost — but maybe not forever.
            </p>
        </div>
        """, unsafe_allow_html=True)










with tab3:
    st.title("Travel History & Funding Statistics")
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

    query_art = """
    SELECT
      STATE,
      ORG2018 AS "Org 2018",
      AMT2018 AS "Amt 2018",
      ORG2019 AS "Org 2019",
      AMT2019 AS "Amt 2019",
      ORG2020 AS "Org 2020",
      AMT2020 AS "Amt 2020"
    FROM "TOURISM"."PUBLIC"."ARTCULTURE1"
    WHERE STATE <> 'Total'
    """
    df_art = session.sql(query_art).to_pandas()

    if selected_state != "All":
        df_art = df_art[df_art["STATE"] == selected_state]

    
    import plotly.graph_objects as go
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(x=df_art["STATE"], y=df_art["Org 2018"], name="Orgs 2018", marker_color="#808000"))
    fig.add_trace(go.Bar(x=df_art["STATE"], y=df_art["Org 2019"], name="Orgs 2019", marker_color="#A0522D"))
    fig.add_trace(go.Bar(x=df_art["STATE"], y=df_art["Org 2020"], name="Orgs 2020", marker_color="#D2691E"))
    
    fig.add_trace(go.Bar(x=df_art["STATE"], y=df_art["Amt 2018"], name="Amt 2018", marker_color="#D2B48C"))
    fig.add_trace(go.Bar(x=df_art["STATE"], y=df_art["Amt 2019"], name="Amt 2019", marker_color="#F5DEB3"))
    fig.add_trace(go.Bar(x=df_art["STATE"], y=df_art["Amt 2020"], name="Amt 2020", marker_color="#FFDAB9"))
    
    fig.update_layout(
        barmode='group',
        title="🎭 Govt Spending in Art & Culture Organizations (2018-2020) across States",
        xaxis_title="State",
        yaxis_title="Count / ₹ (in lakh)",
        legend_title="Metric",
        height=600
    )
    
    st.plotly_chart(fig, use_container_width=True)

    query_scheme = """
    SELECT *
    FROM "TOURISM"."PUBLIC"."ART_SCHEME_FUNDING"
    WHERE SCHEME <> 'Total'
    """
    df_scheme = session.sql(query_scheme).to_pandas()
    
    # Melt for easier plotting
    df_long = df_scheme.melt(id_vars="SCHEME", 
                             value_vars=["Y2019", "Y2020", "Y2021", "Y2022", "Y2023"],
                             var_name="Year", 
                             value_name="Funding")
    
    # Create grouped bar chart
    fig_scheme = px.bar(
        df_long,
        x="SCHEME",
        y="Funding",
        color="Year",
        barmode="group",
        title="🎨 GOI Funding by Scheme (2019–2023) in art & culture",
        labels={"Funding": "Funding (₹ in lakh)"},
        color_discrete_sequence=px.colors.sequential.Aggrnyl
    )
    
    st.plotly_chart(fig_scheme, use_container_width=True)

    query_asi = """
    SELECT *
    FROM "TOURISM"."PUBLIC"."ASI_FUNDING"
    """
    df_asi = session.sql(query_asi).to_pandas()
    
    # Melt and filter only expenditure
    df_asi_long = df_asi.melt(id_vars="YEAR", 
                              value_vars=["EXPENDITURE"],
                              var_name="Type", 
                              value_name="Amount")
    
    # Plot only expenditure
    fig_asi = px.line(
        df_asi_long,
        x="YEAR",
        y="Amount",
        markers=True,
        title="🏛️ ASI Expenditure on Preservation of Monuments (2019–2024)",
        labels={"Amount": "₹ in lakh", "YEAR": "Year"},
        color_discrete_sequence=["#CD5C5C"]
    )
    
    st.plotly_chart(fig_asi, use_container_width=True)


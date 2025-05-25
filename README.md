# IndianTourism

                +------------------------+
                |  data.gov.in           |
                +------------------------+
                           |
                           v
                +------------------------+
                |  Data Cleaning & ETL   |
                |  (Python / Pandas)     |
                +------------------------+
                            |
                            v
                +------------------------+
                |    Snowflake DB        |
                | (Tourism Dataset)      |
                +------------------------+
                           |
             +-------------+--------------+
             |                            |
             v                            v
 +---------------------+      +-------------------------+
 |  Snowpark Connector | ---> |   Streamlit App         |
 |  (Snowflake Python  |      | - Plotly / Altair etc.  |
 |   SDK for querying) |      +-------------------------+
 +---------------------+                    |
                                            v

                                 +--------------------------+
                                 |  Streamlit Community Cloud|
                                 |  (Deployed & Shared App) |
                                 +--------------------------+

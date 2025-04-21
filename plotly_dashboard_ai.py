from dash import dcc, html, Dash, dash_table, Output, Input, State, ctx, no_update, clientside_callback
import duckdb
import plotly.express as px
import os
import re
import math
import pandas as pd
import requests

proxy_url = os.getenv('LLM_API_URL') 
api_key = os.getenv('LLM_API_KEY')
op_url = '/v1/chat/completions'
llm_headers = {
      "Authorization": f"Bearer {api_key}",
      "Content-Type": "application/json",
  }
##prompt to ask AI

user_input = ""
prompt_p1 = """You are helping me write SQL to determine which records in a Medicare database table are linked to a certain condition based on user's input." 
            The claims records are in a table called claims with column ICD9_DGNS_CD_1 containing ICD-9 codes. Here is the DDL of table INPATIENT_CLAIMS_ICD10: 
            CREATE TABLE MAIN.INPATIENT_CLAIMS_ICD10(
            DESYNPUF_ID VARCHAR,   -- Beneficiary ID
            CLM_ID VARCHAR,  -- Claim ID
            CLM_FROM_DT DATE,  -- Claims start date
            CLM_THRU_DT DATE,  -- Claims end date
            CLM_PMT_AMT DECIMAL(10,2),  -- Claim Payment Amount
            CLM_ADMSN_DT DATE,  -- Inpatient admission date
            CLM_UTLZTN_DAY_CNT INTEGER,  -- Claim Utilization Day Count
            NCH_BENE_DSCHRG_DT DATE, -- Inpatient discharged date 
            CLM_DRG_CD VARCHAR,     -- Claim diagnosis related group (DRG) code
            ICD10_DGNS_CODE VARCHAR  -- Claim ICD-10 diagnosis Code
            ); 
            Note that all values in column ICD10_DGNS_CODE already have period marks removed. 

            And table ICD10_DIAG_DESC contains each ICD-10 code and its description. Here is the DDL of table ICD10_DIAG_DESC:
            CREATE TABLE MAIN.ICD10_DIAG_DESC(
              ICD10_CM_CODE VARCHAR, -- ICD-10-CM Code
              DESCRIPTION VARCHAR  -- Description of ICD-10-CM Code
            ); 
            in which ICD10_CM_CODE column contains ICD-10-CM code and DESCRIPTION column has the description of the code. Feel free to use this table as refernece only BUT DO NOT include this table in the returned SQL query.
            Note that values ICD10_CM_CODE fields have period marks removed. 

            The user's question is: 
            """
prompt_p2 = """Please generate a DuckDB-compatible SQL query that select all claims based on the user's question above. Here are the requirements:
            1) Respond only SQL statements and do not include comments. 
            2) Respond only in the format like so:
            $$$
            SELECT * FROM INPATIENT_CLAIMS_ICD10 as c WHERE ...
            $$$
            """
conn = duckdb.connect(database="claims.duckdb", read_only=True)
light_bg_color = '#f8f9fa'
light_text_color = '#333'
init_launch = 0
icd_top_level_grp_cnt = 0
init_min = 0
init_max = 0.1 # Adjusted for fractional rate (0-1)
            
def fetch_data(top_n=None, sqlstmt=None):
        query = """
        SELECT
            s.state_abbr as State,
            SUM(rr.readmissions) as "Total Readmissions",
            SUM(rr.total_admissions) as "Total Admissions"
        FROM
            (
            WITH valid_claims AS (
                SELECT
                    ic.CLM_ID,
                    ic.DESYNPUF_ID,
                    ic.CLM_ADMSN_DT,
                    LAG(ic.NCH_BENE_DSCHRG_DT) OVER (PARTITION BY ic.DESYNPUF_ID ORDER BY ic.CLM_ADMSN_DT) AS prev_discharge_date
                FROM """ + " ( " + f"{sqlstmt}" + " ) as ic " + """
                WHERE EXTRACT(YEAR FROM ic.CLM_ADMSN_DT) IN (2008, 2009, 2010)
            )
            SELECT
                bs.SP_STATE_CODE,
                COUNT(DISTINCT acr.CLM_ID) AS readmissions,
                COUNT(DISTINCT vc.CLM_ID) AS total_admissions
            FROM
                valid_claims vc
            JOIN
                beneficiary_summary bs ON vc.DESYNPUF_ID = bs.DESYNPUF_ID
            LEFT JOIN
                all_cause_readmission acr ON vc.CLM_ID = acr.CLM_ID
            WHERE
                (vc.CLM_ADMSN_DT - vc.prev_discharge_date > 30 OR vc.prev_discharge_date IS NULL)
            GROUP BY
                bs.SP_STATE_CODE
            ) rr
        JOIN
            state s ON rr.SP_STATE_CODE = s.sp_state_code
        GROUP BY
            s.state_abbr
        """

        result = conn.execute(query).fetchdf()

        # Calculate Readmission Rate (Removed redundant Pandas groupby)
        # Calculate Readmission Rate as a fraction (0 to 1) for consistency
        # Calculate Readmission Rate as a fraction (0 to 1) - removed rounding
        result['Readmission Rate'] = result['Total Readmissions'] / result['Total Admissions']

        # Sort and limit if needed
        result = result.sort_values('Readmission Rate', ascending=False)
        if top_n:
            result = result.head(top_n)

        return result

min_value = init_min
max_value = init_max
app = Dash(__name__, assets_folder='assets')

def create_column_chart(data):
    fig = px.bar(data, x='State', y='Readmission Rate', 
          title='Top 10 States by Readmission Rate'
     )
    fig.update_layout(
        title_x=0.5,
        xaxis_title='State',
        yaxis_title='Readmission Rate', # Removed (%) as rate is now fractional
        xaxis=dict(
          showgrid=True,  
          gridcolor='#c8c8c8 ', 
          title='State',
          title_font=dict(size=14, color='#787878'), 
          tickfont=dict(size=12, color='#646464'),
        ),
        margin=dict(l=10, r=10, t=60, b=10),
        template="plotly_white", 
        font=dict(color="#333"), 
        plot_bgcolor="#eee", 
        paper_bgcolor="#eee",
        font_color=light_text_color
    )
    return fig

def create_choropleth_map(data):
    fig = px.choropleth(
        data,
        locations='State',
        locationmode='USA-states',
        color='Readmission Rate',
        scope="usa",
        color_continuous_scale="Cividis",
        range_color=[min_value, max_value],
        title='US Readmission Rates by State',
        labels={'Readmission Rate': 'Readmission Rate'} # Removed (%) as rate is now fractional
    )
    fig.update_layout(
        geo_scope='usa',
        title_x=0.5,
        margin=dict(l=10, r=10, t=60, b=10),
        geo=dict(bgcolor="#eee"),
        template="plotly_white", 
        font=dict(color="#333"), 
        plot_bgcolor="white", 
        paper_bgcolor="#eee",
        font_color=light_text_color
    )
    return fig

noneicdcodequery = "SELECT icd10_cm_code, description FROM main.icd10_diag_desc where icd10_cm_code like 'NONE%' order by icd10_cm_code"
noneicdcodedf = conn.execute(noneicdcodequery).fetchdf() 
errornoicdcodedf = pd.DataFrame({
    'icd10_cm_code': ['**MESSAGE**'],
    'description': ['You have not specified conditions or the conditions are too broad. Please try again with more specific search criteria.']
})
icdcodedf = noneicdcodedf

app.layout = html.Div([
    html.Div([
        html.H1('Accelerated Analysis - Readmission Rates by State', className="header-style")
    ], className="header-container"),
    html.Div(html.Button('Print Dashboard', id='print-dashboard', n_clicks=0, className="button-style print-button-style"),
       className="print-button-area"),
    html.Div([
      dcc.Textarea(
          id="user-input", 
          placeholder="Enter your search here...",
          value="",
          style={'width':'400px','height':'80px','fontSize':'18px', 
            'whiteSpace': 'pre-wrap','backgroundColor':'#e7f1ff',"borderRadius":"5px"},
            className="textarea-style",),
      html.Button("Update Data", id="update-btn", n_clicks=0, className="button-style"),
      html.Button("Show/Hide ICD Codes", id="icd-code-btn", n_clicks=0, className="button-style"),             
      html.Button("Show/Hide Generated SQL", id="sql-btn", n_clicks=0, className="button-style")    
      ],className="top-area"),
    html.Div([ 
          html.Div(id="output-text", 
          style={"display": "none",},
          className="text-box-style"
          ),
          ],style={'display':'flex','justifyContent':'center','marginBottom':'20px'}),
##    Display ICD code list
    html.Div(id="table-container", children=[
      html.H3("ICD-10-CM Codes Table",className="table-title"),
      dash_table.DataTable(
          id='icd10-table',
          columns=[
              {"name": "ICD-10-CM Code", "id": "icd10_cm_code"},
              {"name": "Description", "id": "description"}
          ],
          data=errornoicdcodedf.to_dict("records"),
          page_size=20,
          style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": "#f2f2f2"},
            {"if": {"row_index": "even"}, "backgroundColor": "#ffffff"}, 
          ],
          style_header={
            "backgroundColor": "#e9ecef",
            "color":'#333',
            "fontWeight": "bold",
            "font-size": "16px",
          },
          style_data={
              "backgroundColor": "white",
              "color": "#333",
              "border": "1px solid #ddd",
          },
          style_table={'height': '400px', 'overflowY': 'auto'},
          style_cell={'textAlign': 'left', 'whiteSpace': 'normal', 'height': 'auto'},
          css=[{"selector": "table", "rule": "class: dash-table-container"}],
      )
    ],style={'display':'none'}),
    html.Div([
        html.Div([
            dcc.Graph(id='choropleth-map')
        ], className="graph-container"),
        html.Div([
            dcc.Graph(id='column-chart')
        ], className="graph-container"),
    ], style={'display': 'flex', 'flexDirection': 'row'}),
    html.Div([
        dash_table.DataTable(
            id='table',
            editable=False,
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
            page_action="native",
            page_current=0,
            page_size=10,
            style_table={'overflowX': 'auto'},
            style_data={
                "backgroundColor": "white",
                "color": "#333",
                "border": "1px solid #ddd"
            },
            style_data_conditional=[
              {"if": {"row_index": "odd"}, "backgroundColor": "#f2f2f2"},
              {"if": {"row_index": "even"}, "backgroundColor": "#ffffff"}, 
            ],
            style_cell={
                'height': 'auto',
                'minWidth': '100px', 'width': '150px', 'maxWidth': '180px',
                'whiteSpace': 'normal',
            },            
            style_header={
              "backgroundColor": "#e9ecef",
              "color": "#333",
              "fontWeight": "bold",
              "font-size": "16px",
            },
            css=[{"selector": "table", "rule": "class: dash-table-container"}],
        )
    ], style={'width': '100%', 'marginTop': '20px'}),
##Pop-up if search topic returns too many codes    
    dcc.ConfirmDialog(
        id = "popup-message",
        message = "It appears the topic you chose to research may be too broad. \nPlease use more specific search criteria and try again.",
        displayed = False 
    )
  ], style={'backgroundColor':light_bg_color,'padding':'20px'})

@app.callback(
   [
    Output('choropleth-map', 'figure'),
    Output('column-chart', 'figure'),
    Output('table', 'columns'),
    Output('table', 'data'),
    Output("popup-message", "displayed"),
    Output("output-text", "children")
   ],
    Input("update-btn", "n_clicks"),
    Input("user-input", "value")
)
def update_data(n_clicks,user_input):
    global init_launch 
    global icd_top_level_grp_cnt
    global icdcodedf
    global min_value
    global max_value
    inpatient_claims_source = """SELECT * FROM MAIN.INPATIENT_CLAIMS_ICD10"""
    if n_clicks == 0 or init_launch == 0 or (
                    ctx.triggered_id == "update-btn" and 
                    (user_input is None or user_input.strip() == "")):
        # Fetch data once for consistency
        all_states_data = fetch_data(sqlstmt=inpatient_claims_source)

        # Derive filtered_data and top_10_data from the single fetch
        filtered_data = all_states_data.copy()
        top_10_data = all_states_data.sort_values('Readmission Rate', ascending=False).head(10).copy()

        # Debugging prints for CT and TN data
        if init_launch == 0:
           init_launch = -1
        icdcodedf = errornoicdcodedf
        min_value = init_min
        max_value = init_max
        return (
         create_choropleth_map(filtered_data),
         create_column_chart(top_10_data),
         [{"name": i, "id": i} for i in filtered_data.columns],
         filtered_data.to_dict('records'),
         False,
         f"You have not entered anything yet."
        )
    elif ctx.triggered_id == "update-btn":
         prompt = prompt_p1 + ' ' + user_input + ' ' + prompt_p2
         data = {
             "model": "franklin",
             "messages": [{"role": "user", "content": prompt}]
         }
         try:
             print(f"Making request to: {proxy_url + op_url}")
             print(f"Headers: {llm_headers}")
             print(f"Data: {data}")
             response = requests.post(
                 proxy_url + op_url,
                 headers=llm_headers,
                 json=data)
             print(f"Response status code: {response.status_code}")
             print(f"Response content: {response.text}")
             response.raise_for_status()
             result = response.json()
         except Exception as e:
             print(f"Error making API request: {e}")
             print(f"Response details: {response.text if 'response' in locals() else 'No response'}")
             # Return default data on error
             # Fetch data once for consistency on error as well
             all_states_data = fetch_data(sqlstmt=inpatient_claims_source)
             filtered_data = all_states_data.copy()
             top_10_data = all_states_data.sort_values('Readmission Rate', ascending=False).head(10).copy()
             return (
               create_choropleth_map(filtered_data),
               create_column_chart(top_10_data),
               [{"name": i, "id": i} for i in filtered_data.columns],
               filtered_data.to_dict('records'),
               False,
               f"Error making API request: {e}"
             )
         msg = result["choices"][0]["message"]["content"]
         match_found = re.search(r"\${3}(.*)\${3}",msg,re.DOTALL)
         franklin_sql = re.sub(r"[ \t\n;]+"," ",re.sub(r'--.*',"", match_found.group(1)))
         where_clause_match = re.search(r"where\s+(.+)", franklin_sql, re.IGNORECASE)
         where_clause = where_clause_match.group(0) if where_clause_match else "WHERE clause not found"
         icd_codes = re.findall(r"ICD10_DGNS_CODE LIKE '([A-Z]\d{1,3})%'", where_clause)
         icd_top_level_grp_cnt = len(set(icd_codes))
         icdcodequery = "SELECT icd10_cm_code, description FROM main.icd10_diag_desc as c " + where_clause.replace("ICD10_DGNS_CODE","icd10_cm_code")
         icdcodedf = conn.execute(icdcodequery).fetchdf()
         if icd_top_level_grp_cnt < 10:
           inpatient_claims_source = franklin_sql
           show_pop = 'False'
         else:
           show_pop = 'True'
         # Fetch data once with the AI filter applied
         all_states_data = fetch_data(sqlstmt=inpatient_claims_source)
         filtered_data = all_states_data.copy()
         top_10_data = all_states_data.sort_values('Readmission Rate', ascending=False).head(10).copy()

         # Update min/max based on actual fractional data range after filtering
         min_value = filtered_data['Readmission Rate'].min()
         max_value = filtered_data['Readmission Rate'].max()
         return (
           create_choropleth_map(filtered_data),
           create_column_chart(top_10_data),
           [{"name": i, "id": i} for i in filtered_data.columns],
           filtered_data.to_dict('records'),
           show_pop == 'True',
           f" {franklin_sql}"
         )
    return no_update  
    
##Update SQL box
@app.callback(
    Output("output-text", "style"),
    Input("sql-btn", "n_clicks"),
    Input("update-btn", "n_clicks"),
    State("output-text", "style"),
    prevent_initial_call=True
)
def toggle_text(n_clicks,update_clicks,current_style):
    trigger_id = ctx.triggered_id
    if trigger_id == "update-btn":
       current_style["display"] = "none"
       return current_style
    elif trigger_id == "sql-btn":
       current_style["display"] = "block" if current_style["display"] == "none" else "none"
       return current_style
       
##Update ICD Code Table
@app.callback(
    Output("table-container", "style"),
    Output("icd10-table", "data"), 
    Output("icd10-table", "style_table"),   
    Input("icd-code-btn", "n_clicks"),
    Input("update-btn", "n_clicks"),
    State("table-container", "style"),
    State("icd10-table", "style_table"),
    prevent_initial_call=True
)
def show_icd_tbl(toggle_clicks,update_clicks,current_style_cont,current_style_tbl):
   trigger_id = ctx.triggered_id
   if trigger_id == "update-btn":
      current_style_cont["display"] = "none" 
      current_style_tbl["height"] = "100px"
      return [current_style_cont,noneicdcodedf.to_dict("records"),current_style_tbl]
   elif trigger_id == "icd-code-btn":
       current_style_cont["display"] = "block" if current_style_cont["display"] == "none" else "none" 
       if icd_top_level_grp_cnt < 10:
           current_style_tbl["height"] = "400px"
           return [current_style_cont,icdcodedf.to_dict("records"),current_style_tbl]
       else:
           current_style_tbl["height"] = "100px"
           return [current_style_cont,errornoicdcodedf.to_dict("records"),current_style_tbl]
    
# Add clientside callback for print functionality
app.clientside_callback(
    """
    function(n_clicks) {
        if (n_clicks > 0) {
            console.log('Print Dashboard button clicked');
            window.print();
        }
        return window.dash_clientside.no_update;
    }
    """,
    Output('print-dashboard', 'n_clicks'),
    Input('print-dashboard', 'n_clicks')
)

if __name__ == '__main__':
    app.run_server(debug=True)

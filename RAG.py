import streamlit as st
from snowflake.snowpark import Session
from snowflake.core import Root
import pandas as pd
import json
import plotly.express as px

pd.set_option("max_colwidth", None)

### Default Values
NUM_CHUNKS = 3  # Num-chunks provided as context. Play with this to check how it affects your accuracy

# Service parameters
CORTEX_SEARCH_DATABASE = "ANANYA_SNOWFLAKE"
CORTEX_SEARCH_SCHEMA = "PUBLIC"
CORTEX_SEARCH_SERVICE = "CC_SEARCH_SERVICE_CS_HOUSING"

# Columns to query in the service
COLUMNS = ["chunk", "relative_path", "category"]

connection_parameters = {
    "account": "wob36269",
    "user": "ANANYAHN",
    "password": "Qwerty1!"
}

session = Session.builder.configs(connection_parameters).create()
root = Root(session)

svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]

### Functions

def config_options():
    # Fetching available categories (e.g., country/state)
    # categories = session.sql("SELECT DISTINCT(TEST_OUTPUT) FROM DOCS_CATEGORIES_FINAL").collect()
    state_names = ["Alaska", "Alabama", "Arkansas", "American Samoa", "Arizona", "California", "Colorado", "Connecticut", "District ", "of Columbia", "Delaware", "Florida", "Georgia", "Guam", "Hawaii", "Iowa", "Idaho", "Illinois", "Indiana", "Kansas", "Kentucky", "Louisiana", "Massachusetts", "Maryland", "Maine", "Michigan", "Minnesota", "Missouri", "Mississippi", "Montana", "North Carolina", "North Dakota", "Nebraska", "New Hampshire", "New Jersey", "New Mexico", "Nevada", "New York", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Virginia", "Virgin Islands", "Vermont", "Washington", "Wisconsin", "West Virginia", "Wyoming"]

    cat_list = ["ALL"] + state_names

    # Sidebar filters
    st.sidebar.selectbox("Select a country", cat_list, key="country_value")
    st.sidebar.selectbox("Select a state", cat_list, key="state_value")

def get_similar_chunks_search_service(query):
    # Filtering based on selected country/state
    filters = []
    if st.session_state.country_value != "ALL":
        filters.append({"@eq": {"category": st.session_state.country_value}})
    if st.session_state.state_value != "ALL":
        filters.append({"@eq": {"category": st.session_state.state_value}})

    filter_obj = {"@and": filters} if filters else None
    response = svc.search(query, COLUMNS, filter=filter_obj, limit=NUM_CHUNKS) if filter_obj else svc.search(query, COLUMNS, limit=NUM_CHUNKS)

    st.sidebar.json(response.json())
    return response.json()

def create_prompt(myquestion):
    # Generating a prompt for the question with contextual chunks
    prompt_context = get_similar_chunks_search_service(myquestion)
    prompt = f"""
You are an expert real estate analyst with comprehensive knowledge of US housing markets, investment analysis, and economic trends. Your expertise includes:

Core Competencies:
- Housing market valuation and appraisal analysis
- Geographic and demographic market intelligence
- Investment opportunity assessment
- Economic and migration pattern analysis
- Real estate operations and compliance

Response Guidelines:

1. Geographic Specificity
- Always ground analysis in specific geographic contexts (CBSA, ZIP code, state level)
- Reference relevant local market indicators
- Consider regional economic factors and migration patterns

2. Data-Driven Analysis
- Support recommendations with specific metrics from the provided context
- Compare current values against historical trends when available
- Cross-reference multiple data points (e.g., housing prices, income levels, migration)

3. Actionable Recommendations
- Provide concrete, implementable suggestions
- Consider multiple stakeholder perspectives (investors, businesses, residents)
- Include both short-term and long-term implications

4. Context Integration
- Connect housing data with broader economic indicators
- Consider relationships between different metrics (e.g., mortgage rates and housing values)
- Factor in demographic and migration trends

Response Structure:

For each query, structure your response to include:

1. Market Overview
- Relevant geographic context
- Key market indicators
- Current trends and patterns

2. Analysis
- Data-supported insights
- Comparative analysis
- Risk factors and opportunities

3. Recommendations
- Specific, actionable steps
- Timeline considerations
- Risk mitigation strategies

4. Supporting Data
- Relevant metrics and statistics
- Trend comparisons
- Geographic comparisons

Remember to:
- Be specific about geographic areas
- Support claims with data from the context
- Provide actionable insights
- Consider multiple relevant factors
- Acknowledge data limitations when appropriate
- Use the CONTEXT provided below

           CONTEXT:
           {prompt_context}

           QUESTION:
           {myquestion}

           Answer:
        """
    
    return prompt

def country_identify(myquestion):
    # Generating a prompt for the question with contextual chunks
    if st.session_state.rag:
        prompt_context = get_similar_chunks_search_service(myquestion)
        prompt = f"""
           You are a climate and health expert. Use the CONTEXT provided below to answer questions.
           Provide actionable and geography-specific recommendations based on the CONTEXT. 
           Be as detailed as possible, using your understanding of the specific area.

           CONTEXT:
           {prompt_context}

           QUESTION:
           {myquestion}

           Answer:
        """
    else:
        prompt = f"Question: {myquestion}\nAnswer:"
    return prompt

def complete(myquestion):
    # Running LLM completion with Snowflake Cortex
    prompt = create_prompt(myquestion)
    cmd = """
        SELECT snowflake.cortex.complete(?, ?) AS response
    """
    df_response = session.sql(cmd, params=["mistral-large", prompt]).collect()
    return df_response[0].RESPONSE

def which_country(myquestion):
    # Running LLM completion with Snowflake Cortex
    prompt = create_prompt(myquestion)
    cmd = """
        SELECT snowflake.cortex.complete(?, ?) AS response
    """
    df_response = session.sql(cmd, params=["mistral-large", prompt]).collect()
    return df_response[0].RESPONSE

def visualize_data(state_or_country):
    # Example function for visualizing climate data
    data = session.sql(f"SELECT * FROM climate_data WHERE region='{state_or_country}'").to_pandas()
    fig = px.line(data, x="year", y="metric", title=f"Climate Trends for {state_or_country}")
    st.plotly_chart(fig)

def main():
    st.title(":earth_americas: Climate and Health Recommendations Dashboard")
    st.markdown("Use this dashboard to explore detailed recommendations for improving climate and health outcomes.")

    # Sidebar Filters
    config_options()
    st.session_state.rag = st.sidebar.checkbox("Use your own documents as context?")

    # User Question
    question = st.text_input("Enter your question", placeholder="What can be done in California to reduce emissions?")

    if question:
        response = complete(question)
        st.markdown(response)

if __name__ == "__main__":
    main()

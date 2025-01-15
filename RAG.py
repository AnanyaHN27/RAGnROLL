import streamlit as st
from snowflake.snowpark import Session
from snowflake.core import Root
import pandas as pd
import json
import plotly.express as px

# Configure pandas display
pd.set_option("max_colwidth", None)

### Constants and Configuration
NUM_CHUNKS = 3

# Snowflake Configuration
SNOWFLAKE_CONFIG = {
    "CORTEX_SEARCH_DATABASE": "ANANYA_SNOWFLAKE",
    "CORTEX_SEARCH_SCHEMA": "PUBLIC",
    "CORTEX_SEARCH_SERVICE": "CC_SEARCH_SERVICE_CS_AN",
    "COLUMNS": ["chunk", "relative_path", "test_output"]
}

st.set_page_config(
    page_title="Environmental Analysis",
    page_icon="🌍"
)

# Initialize session state
if 'country_value' not in st.session_state:
    st.session_state.country_value = "ALL"
if 'state_value' not in st.session_state:
    st.session_state.state_value = "ALL"
if 'rag' not in st.session_state:
    st.session_state.rag = False

# Initialize Snowflake connection
connection_parameters = {
    "account": "wob36269",
    "user": "ANANYAHN",
    "password": "Qwerty1!"
}

COLUMNS = ["chunk", "relative_path", "test_output"]

# session = get_active_session()
session = Session.builder.configs(connection_parameters).create()
root = Root(session)

svc = root.databases[SNOWFLAKE_CONFIG["CORTEX_SEARCH_DATABASE"]].schemas[
    SNOWFLAKE_CONFIG["CORTEX_SEARCH_SCHEMA"]].cortex_search_services[
    SNOWFLAKE_CONFIG["CORTEX_SEARCH_SERVICE"]]

def get_location_options():
    """Return list of available locations."""
    return ["ALL"] + [
        "Alaska", "Alabama", "Arkansas", "American Samoa", "Arizona", 
        "California", "Colorado", "Connecticut", "District of Columbia", 
        "Delaware", "Florida", "Georgia", "Guam", "Hawaii", "Iowa", 
        "Idaho", "Illinois", "Indiana", "Kansas", "Kentucky", "Louisiana", 
        "Massachusetts", "Maryland", "Maine", "Michigan", "Minnesota", 
        "Missouri", "Mississippi", "Montana", "North Carolina", 
        "North Dakota", "Nebraska", "New Hampshire", "New Jersey", 
        "New Mexico", "Nevada", "New York", "Ohio", "Oklahoma", "Oregon", 
        "Pennsylvania", "Puerto Rico", "Rhode Island", "South Carolina", 
        "South Dakota", "Tennessee", "Texas", "Utah", "Virginia", 
        "Virgin Islands", "Vermont", "Washington", "Wisconsin", 
        "West Virginia", "Wyoming"
    ]

def config_options():
    """Configure sidebar options."""
    state_names = ["Alaska", "Alabama", "Arkansas", "American Samoa", "Arizona", "California", "Colorado", "Connecticut", "District ", "of Columbia", "Delaware", "Florida", "Georgia", "Guam", "Hawaii", "Iowa", "Idaho", "Illinois", "Indiana", "Kansas", "Kentucky", "Louisiana", "Massachusetts", "Maryland", "Maine", "Michigan", "Minnesota", "Missouri", "Mississippi", "Montana", "North Carolina", "North Dakota", "Nebraska", "New Hampshire", "New Jersey", "New Mexico", "Nevada", "New York", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Virginia", "Virgin Islands", "Vermont", "Washington", "Wisconsin", "West Virginia", "Wyoming"]

    cat_list = ["ALL"] + state_names
    st.selectbox("Select US State", cat_list, key="state_value")
    st.session_state.rag = True #st.sidebar.checkbox("Use document context", value=True)

def get_similar_chunks_search_service(query):
    """Get relevant chunks from the search service."""
    filters = []
    if st.session_state.state_value != "ALL":
        filters.append({"@eq": {"test_output": st.session_state.state_value}})

    filter_obj = {"@and": filters} if filters else None
    response = svc.search(query, COLUMNS, filter=filter_obj, limit=NUM_CHUNKS) if filter_obj else svc.search(query, COLUMNS, limit=NUM_CHUNKS)
    # st.sidebar.json(response.json())
    return response.json()

def create_environmental_prompt(query_type):
    """Create specialized prompts based on query type."""
    prompts = {
        "emissions": """
    Focus on emissions analysis and reduction:
    - Current emission levels and sources
    - Reduction strategies and initiatives
    - Policy recommendations
    - Implementation challenges
    """,
        "climate_impact": """
    Analyze climate change impacts:
    - Current and projected effects
    - Vulnerable areas and populations
    - Adaptation strategies
    - Mitigation measures
    """,
        "biodiversity": """
    Evaluate biodiversity considerations:
    - Species diversity and distribution
    - Habitat preservation
    - Conservation strategies
    - Environmental pressures
    """,
        "default": """
    Provide comprehensive environmental analysis:
    - Current environmental conditions
    - Key challenges and opportunities
    - Action recommendations
    - Supporting data
    """
    }
    return prompts.get(query_type, prompts["default"])

def create_prompt(myquestion):
    """Generate enhanced prompt with contextual analysis."""
    if not st.session_state.rag:
        return f"Question: {myquestion}\nAnswer:"
    
    prompt_context = get_similar_chunks_search_service(myquestion)
    
    # Determine query type based on keywords
    query_type = "default"
    if any(word in myquestion.lower() for word in ["emission", "carbon", "greenhouse"]):
        query_type = "emissions"
    elif any(word in myquestion.lower() for word in ["climate", "warming", "temperature"]):
        query_type = "climate_impact"
    elif any(word in myquestion.lower() for word in ["biodiversity", "species", "habitat"]):
        query_type = "biodiversity"

    specialized_prompt = create_environmental_prompt(query_type)
    
    base_prompt = f"""
You are an environmental and climate science expert with comprehensive knowledge of climate change, environmental policy, and sustainability solutions.

{specialized_prompt}

Core Focus Areas:
- Climate change impact assessment
- Emissions reduction strategies
- Environmental policy analysis
- Biodiversity conservation
- Sustainability implementation

Analysis Framework:

1. Current Status
- Environmental conditions
- Key indicators and metrics
- Regional specific factors
- Recent trends

2. Impact Assessment
- Environmental effects
- Social implications
- Economic considerations
- Health impacts

3. Strategic Solutions
- Policy recommendations
- Technical solutions
- Implementation strategies
- Stakeholder engagement

4. Action Plan
- Immediate steps
- Long-term strategy
- Resource requirements
- Monitoring metrics

Guidelines:
- Provide region-specific insights
- Use evidence-based analysis
- Include actionable recommendations
- Consider multiple stakeholder perspectives
- Acknowledge data limitations

Current Region Focus: {st.session_state.state_value}

CONTEXT:
{prompt_context}

QUESTION:
{myquestion}

Provide a structured analysis following the framework above:
"""
    return base_prompt

def complete(myquestion):
    """Execute LLM completion with enhanced prompting."""
    prompt = create_prompt(myquestion)
    cmd = """
        SELECT snowflake.cortex.complete(?, ?) AS response
    """
    df_response = session.sql(cmd, params=["mistral-large", prompt]).collect()
    return df_response[0].RESPONSE

def main():
    """Main application function."""
    
    st.markdown("""
    <style>
    /* Global Styles */
    [data-testid="stAppViewContainer"] {
        background-color: #f3f7f0;
    }
    
    /* Typography */
    h1, h2, h3, h4, h5, h6 {
        font-family: 'Inter', -apple-system, system-ui, BlinkMacSystemFont, sans-serif;
        color: #2c4a3e;
        font-weight: 600;
        letter-spacing: -0.025em;
    }
    
    /* Title Styling */
    .title-container {
        background: #2c4a3e;
        padding: 2rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 15px rgba(44, 74, 62, 0.1);
    }
    
    .title-container h1 {
        color: white;
        font-size: 2.5rem;
        margin: 0;
        padding: 0;
    }
    
    .title-container p {
        color: rgba(255, 255, 255, 0.9);
        font-size: 1.1rem;
        margin-top: 1rem;
        line-height: 1.6;
    }
    
    /* Input Field Styling */
    .stTextInput > div > div > input {
        background-color: white;
        padding: 1rem;
        font-size: 1rem;
        border: 1px solid #c5d5c5;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(44, 74, 62, 0.05);
    }
    
    /* Footer Styling */
    .footer {
        position: fixed;
        bottom: 0;
        left: 0;
        right: 0;
        padding: 1rem;
        background-color: rgba(255, 255, 255, 0.9);
        border-top: 1px solid #c5d5c5;
        text-align: center;
        font-size: 0.875rem;
        color: #2c4a3e;
        z-index: 1000;
        backdrop-filter: blur(5px);
    }
    </style>
    """, unsafe_allow_html=True)

    # Title Section
    st.markdown("""
    <div class="title-container">
        <h1>US Environmental Analysis & Recommendations</h1>
        <p>Get detailed environmental analysis, climate impact assessments, and sustainability recommendations.
        Select a state and enter your query below.</p>
    </div>
    """, unsafe_allow_html=True)

    config_options()

    question = st.text_input(
        "Enter your question",
        placeholder="What are the main environmental challenges and potential solutions in this region?"
    )

    # Create a placeholder
    placeholder = st.empty()

    # Only populate the placeholder if there's a response
    if question:
        response = complete(question)
        with placeholder.container():
            st.header("🌿 Analysis & Recommendations")
            st.markdown(response)
    
    st.markdown("""
    <div class="footer">
        Made with 🌱 by Ananya Hari Narain for RAG 'n' ROLL Amp up Search with Snowflake & Mistral
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()

import streamlit as st
from pathlib import Path

def apply_compact_styling():
    """Apply compact styling to make UI elements smaller and more refined"""
    css_file = Path(__file__).parent / "styles" / "custom.css"
    
    #     # Debug information
    # st.write(f"**CSS Debug:**")
    # st.write(f"- shared_utils.py location: {Path(__file__)}")
    # st.write(f"- Looking for CSS at: {css_file}")
    # st.write(f"- CSS file exists: {css_file.exists()}")

    if css_file.exists():
        try:
            with open(css_file) as f:
                css_content = f.read()
            # st.write(f"- CSS file size: {len(css_content)} characters")
            st.markdown(f'<style>{css_content}</style>', unsafe_allow_html=True)
            # st.success("✅ External CSS file loaded successfully")
        except Exception as e:
            st.error(f"❌ Error reading CSS file: {e}")
    else:
        # Fallback compact styling
        st.markdown("""
        <style>
            /* Main container */
            .main .block-container {
                padding-top: 1rem !important;
                padding-bottom: 1rem !important;
                padding-left: 1rem !important;
                padding-right: 1rem !important;
                max-width: 95% !important;
            }
            
            /* Headers */
            h1 { font-size: 1.6rem !important; margin-bottom: 0.6rem !important; }
            h2 { font-size: 1.3rem !important; margin-bottom: 0.5rem !important; }
            h3 { font-size: 1.1rem !important; margin-bottom: 0.4rem !important; }
            
            /* Compact elements */
            .element-container { margin-bottom: 0.3rem !important; }
            
            /* Buttons */
            .stButton > button {
                padding: 0.25rem 0.6rem !important;
                font-size: 0.85rem !important;
                height: 2rem !important;
                border-radius: 0.25rem !important;
            }
            
            /* Form elements */
            .stSelectbox > div > div { min-height: 2rem !important; }
            .stTextInput > div > div > input { height: 2rem !important; }
            .stNumberInput > div > div > input { height: 2rem !important; }
            .stTextArea textarea { min-height: 100px !important; font-size: 0.85rem !important; }
            
            /* Metrics */
            [data-testid="metric-container"] {
                background-color: #f8f9fa;
                border: 1px solid #dee2e6;
                padding: 0.3rem 0.5rem;
                border-radius: 0.25rem;
                margin: 0.1rem 0;
            }
            [data-testid="metric-container"] label { font-size: 0.8rem !important; }
            [data-testid="metric-container"] [data-testid="metric-value"] { font-size: 1.1rem !important; }
            
            /* Expanders */
            .streamlit-expanderHeader {
                font-size: 0.9rem !important;
                padding: 0.25rem 0.5rem !important;
                background-color: #f8f9fa !important;
            }
            
            /* Alerts */
            .stAlert { padding: 0.4rem 0.6rem !important; margin: 0.2rem 0 !important; }
            
            /* Tables */
            .dataframe { font-size: 0.8rem !important; }
            
            /* Sidebar */
            .css-1d391kg { padding-top: 1rem !important; }
        </style>
        """, unsafe_allow_html=True)
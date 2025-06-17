import streamlit as st
from pathlib import Path
from datetime import datetime

# Global page config - set once for entire app
st.set_page_config(
    page_title="DataNavigator",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="auto"
)

# Global CSS loading function
def load_global_css():
    css_file = Path(__file__).parent / "styles" / "custom.css"
    if css_file.exists():
        with open(css_file) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    else:
        # Fallback: create basic styling if CSS file doesn't exist
        st.markdown("""
        <style>
            .main .block-container {
                padding-top: 1.5rem;
                padding-bottom: 1.5rem;
                max-width: 95%;
            }
            h1 { font-size: 1.8rem !important; margin-bottom: 0.8rem !important; }
            h2 { font-size: 1.4rem !important; margin-bottom: 0.6rem !important; }
            h3 { font-size: 1.2rem !important; margin-bottom: 0.4rem !important; }
            .element-container { margin-bottom: 0.4rem !important; }
            .stButton > button { padding: 0.3rem 0.8rem !important; font-size: 0.9rem !important; }
        </style>
        """, unsafe_allow_html=True)

# Show different icons based on error message
def get_status_icon(status, error_message):
    if status == "completed":
        return "✅"
    elif status == "running":
        return "🔄"
    elif status == "failed":
        # Check if it was manually stopped
        if error_message and "Manually stopped" in error_message:
            return "🛑"  # Manual stop
        else:
            return "❌"  # Actual failure
    else:
        return "❓"

# Apply global styling
load_global_css()

# Main page content
st.title("🏠 DataNavigator")
st.markdown("""
**Insight into data** — what exists and what is needed — from both the source data and business perspectives.  
From this insight, the actual data model can be created and managed, supporting ETL automation.
""")

st.divider()

# Quick stats
st.subheader("📈 System Overview")

try:
    import os
    import sqlalchemy as sa
    from dotenv import load_dotenv
    
    # Load environment variables
    dotenv_path = Path(__file__).parent.parent / ".env"
    load_dotenv(dotenv_path)
    
    # Connect to database
    db_url = sa.engine.URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("NAV_DB_USER"),
        password=os.getenv("NAV_DB_PASSWORD"),
        host=os.getenv("NAV_DB_HOST"),
        port=os.getenv("NAV_DB_PORT"),
        database=os.getenv("NAV_DB_NAME")
    )
    engine = sa.create_engine(db_url)
    
    with engine.connect() as conn:
        # Corrected stats query based on actual schema
        stats_result = conn.execute(sa.text("""
            SELECT 
                COUNT(DISTINCT c.id) as connections,
                COUNT(DISTINCT d.database_name) as databases,
                COUNT(DISTINCT CASE WHEN t.is_current = true THEN t.id END) as current_tables,
                COUNT(DISTINCT CASE WHEN col.is_current = true THEN col.id END) as current_columns,
                COUNT(DISTINCT cr.id) as total_runs,
                COUNT(DISTINCT CASE WHEN cr.run_status = 'completed' THEN cr.id END) as successful_runs
            FROM config.connections c
            LEFT JOIN catalog.catalog_runs cr ON c.id = cr.connection_id
            LEFT JOIN catalog.catalog_databases d ON cr.id = d.catalog_run_id AND d.is_current = true
            LEFT JOIN catalog.catalog_schemas s ON d.id = s.database_id AND s.is_current = true
            LEFT JOIN catalog.catalog_tables t ON s.id = t.schema_id AND t.is_current = true
            LEFT JOIN catalog.catalog_columns col ON t.id = col.table_id AND col.is_current = true
        """))
        
        stats = stats_result.fetchone()
        if stats:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("🔌 Connections", stats[0] or 0)
                st.metric("🗄️ Databases", stats[1] or 0)
            with col2:
                st.metric("📋 Tables", stats[2] or 0)
                st.metric("📄 Columns", stats[3] or 0)
            with col3:
                st.metric("🏃‍♂️ Total Runs", stats[4] or 0)
                st.metric("✅ Successful Runs", stats[5] or 0)
        
        # Recent activity (with management options)
        st.subheader("📅 Recent Activity")
        recent_result = conn.execute(sa.text("""
            SELECT 
                cr.id,
                cr.run_started_at,
                c.name as connection_name,
                cr.run_status,
                cr.tables_processed,
                cr.columns_processed,
                cr.run_completed_at,
                cr.error_message  -- ← Add this
            FROM catalog.catalog_runs cr
            JOIN config.connections c ON cr.connection_id = c.id
            ORDER BY cr.run_started_at DESC
            LIMIT 10
        """))

        recent_runs = recent_result.fetchall()
        if recent_runs:
            # Show running processes with kill option
            running_runs = [run for run in recent_runs if run[3] == 'running']
            if running_runs:
                st.warning(f"⚠️ {len(running_runs)} catalog run(s) currently running")
                
                # Quick kill options
                col1, col2 = st.columns([3, 1])
                with col1:
                    running_ids = [str(run[0]) for run in running_runs]
                    selected_runs = st.multiselect(
                        "Select runs to stop:",
                        options=running_ids,
                        format_func=lambda x: f"Run {x} - {next(run[2] for run in running_runs if str(run[0]) == x)}"
                    )
                with col2:
                    if selected_runs and st.button("🛑 Stop Selected", type="secondary"):
                        killed_count = 0
                        for run_id in selected_runs:
                            try:
                                conn.execute(sa.text("""
                                    UPDATE catalog.catalog_runs 
                                    SET run_status = 'failed',
                                        run_completed_at = CURRENT_TIMESTAMP,
                                        error_message = 'Manually stopped from Home dashboard'
                                    WHERE id = :run_id AND run_status = 'running'
                                """), {"run_id": int(run_id)})
                                killed_count += 1
                            except Exception as e:
                                st.error(f"Failed to stop run {run_id}: {e}")
                        
                        if killed_count > 0:
                            conn.commit()
                            st.success(f"🛑 Stopped {killed_count} catalog run(s)")
                            st.rerun()
            
            # Display all recent runs
            for run in recent_runs:
                run_id, started, conn_name, status, tables, columns, completed, error_message = run
                
                status_icon = get_status_icon(status, error_message)  
                
                # Calculate duration
                if completed and started:
                    duration = completed - started
                    duration_str = f" ({duration.total_seconds():.1f}s)"
                elif status == "running":
                    duration = datetime.now() - started.replace(tzinfo=None) if started else None
                    duration_str = f" ({int(duration.total_seconds()//60)}m {int(duration.total_seconds()%60)}s)" if duration else ""
                else:
                    duration_str = ""
                
                with st.expander(f"{status_icon} Run {run_id} - {conn_name} - {started.strftime('%Y-%m-%d %H:%M')}{duration_str}", expanded=False):
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.write(f"**Status:** {status}")
                    with col2:
                        st.write(f"**Tables:** {tables or 0}")
                    with col3:
                        st.write(f"**Columns:** {columns or 0}")
                    with col4:
                        if status == "running":
                            if st.button(f"🛑 Stop Run {run_id}", key=f"stop_{run_id}"):
                                try:
                                    conn.execute(sa.text("""
                                        UPDATE catalog.catalog_runs 
                                        SET run_status = 'failed',
                                            run_completed_at = CURRENT_TIMESTAMP,
                                            error_message = 'Manually stopped from Home dashboard'
                                        WHERE id = :run_id
                                    """), {"run_id": run_id})
                                    conn.commit()
                                    st.success(f"🛑 Stopped run {run_id}")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Failed to stop run: {e}")
        else:
            st.info("No catalog runs found yet. Start by configuring connections and running cataloging.")

except Exception as e:
    st.info("💡 Database statistics will be available once you configure connections and run cataloging.")
    st.error(f"Debug: {e}")

st.divider()

st.markdown("""
### 🚀 Getting Started

1. **Configure Connections** - Set up your database connections in the Connection Manager
2. **Run Cataloging** - Execute catalog discovery on your databases  
3. **Browse Results** - Explore your data catalog and discover insights

Use the navigation in the sidebar to access each module.
""")

# DataNavigator v1

Simple data catalog tool.

## Features (v1)
1. Connect to PostgreSQL database
2. Extract tables and columns
3. Display catalog in Streamlit
4. Add descriptions to tables/columns
5. Save descriptions

## Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Configure .env with your database credentials
cp .env.example .env
# Edit .env with real values

# Run the app
streamlit run app.py
```

## V1 Status
- [ ] Database connection working
- [ ] Table extraction working
- [ ] Column extraction working
- [ ] Streamlit display working
- [ ] Description editing working
# 🎭 Mockdata voor tabellen, views en edge cases

# Normale facttabel
mock_fact_sales = {
    "table_name": "fact_sales",
    "table_type": "BASE TABLE",
    "columns": [
        {"column_name": "id", "data_type": "integer"},
        {"column_name": "amount", "data_type": "numeric"},
        {"column_name": "order_date", "data_type": "date"},
    ]
}

# View met SQL-definitie
mock_view_revenue = {
    "table_name": "vw_revenue",
    "table_type": "VIEW",
    "definition": "SELECT * FROM sales"
}

# Lege tabel (geen kolommen)
mock_empty_table = {
    "table_name": "dim_empty",
    "table_type": "BASE TABLE",
    "columns": []
}

# Sampledata bij gewone tabellen
mock_sample_data = [
    {"id": 1, "amount": 100, "order_date": "2024-01-01"},
    {"id": 2, "amount": 200, "order_date": "2024-01-02"},
]

def build_prompt(table, metadata, sample_rows):
    column_info = "\n".join([f"- {col['name']} ({col['type']})" for col in metadata])
    samples = "\n".join([str(row) for row in sample_rows])

    prompt = f"""
Je bent een data-analist. Hieronder staat een tabel uit een softwarepakket genaamd ENK.

Tabel: {table['table_name']}

Kolommen:
{column_info}

Voorbeelddata:
{samples}

Beantwoord de volgende vragen:
1. Wat is waarschijnlijk het doel van deze tabel?
2. Welke kolommen zijn sleutelvelden of relaties met andere tabellen?
3. Wat voor soort gegevens bevat deze tabel (dimensies, feiten, referenties)?
4. Wat stel je voor als transformaties voor een silver layer?
"""
    return prompt.strip()
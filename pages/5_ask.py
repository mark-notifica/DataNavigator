"""
Ask - Chat with your data catalog using AI.
Ask questions about your data and get answers based on catalog descriptions.
"""

import os
from dotenv import load_dotenv
load_dotenv()

import streamlit as st
from vector_store import search, get_stats

st.set_page_config(
    page_title="Ask - DataNavigator",
    page_icon="💬",
    layout="wide"
)

st.title("💬 Ask")
st.markdown("Ask questions about your data catalog")

st.divider()

# Check if index has data
stats = get_stats()
if stats['total_vectors'] == 0:
    st.warning("No items in vector index. Go to **Index** page and sync your catalog first.")
    st.stop()

# LLM Configuration in sidebar
with st.sidebar:
    st.header("AI Model")

    # Check for API keys/hosts
    anthropic_key = os.environ.get('ANTHROPIC_API_KEY', '')
    ollama_host = os.environ.get('OLLAMA_HOST', '')

    available_models = []

    if anthropic_key:
        available_models.append("Claude (Anthropic)")
    if ollama_host:
        available_models.append("Mistral (Ollama)")

    if not available_models:
        st.warning("No AI models configured")
        st.markdown("""
        Add to your `.env` file:

        **For Claude:**
        ```
        ANTHROPIC_API_KEY=sk-ant-...
        ```

        **For Mistral (Ollama):**
        ```
        OLLAMA_HOST=http://10.3.152.8:11434
        OLLAMA_MODEL=mistral:instruct
        ```
        """)
        model_choice = None
    else:
        model_choice = st.selectbox("Select model", available_models)

    st.divider()

    st.subheader("Search Settings")
    num_context = st.slider("Context items", 3, 20, 10,
                           help="Number of catalog items to include as context")

    st.divider()

    st.subheader("Prompt Settings")

    # Default system prompts
    default_prompts = {
        "English": """You are a helpful data catalog assistant. You help users understand their data assets.

Use the following catalog context to answer questions. If the context doesn't contain enough information,
say so and suggest what additional information might help.

Be concise but thorough. Reference specific tables, columns, or other catalog items when relevant.""",

        "Nederlands": """Je bent een behulpzame data catalog assistent. Je helpt gebruikers hun data assets te begrijpen.

Gebruik de catalogus context om vragen te beantwoorden. Als de context onvoldoende informatie bevat,
geef dit aan en suggereer welke aanvullende informatie zou kunnen helpen.

Wees beknopt maar grondig. Verwijs naar specifieke tabellen, kolommen of andere catalog items waar relevant."""
    }

    # Initialize session state for prompts
    if 'prompt_language' not in st.session_state:
        st.session_state.prompt_language = "English"
    if 'system_prompt' not in st.session_state:
        st.session_state.system_prompt = default_prompts["English"]
    if 'business_rules' not in st.session_state:
        st.session_state.business_rules = ""

    # Language selector
    language = st.selectbox(
        "Language",
        options=list(default_prompts.keys()),
        index=list(default_prompts.keys()).index(st.session_state.prompt_language),
        help="Select language for the default system prompt"
    )

    # If language changed, update prompt
    if language != st.session_state.prompt_language:
        st.session_state.prompt_language = language
        st.session_state.system_prompt = default_prompts[language]
        st.rerun()

    # System prompt editor
    system_prompt_input = st.text_area(
        "System prompt",
        value=st.session_state.system_prompt,
        height=150,
        help="Instructions for the AI. Edit freely or switch language above."
    )
    st.session_state.system_prompt = system_prompt_input

    # Business rules placeholders per language
    business_rules_placeholders = {
        "English": """Example:
- Structure answers as: A) Direct answer, B) Relevant tables/columns, C) Data types, D) Notes
- Only answer based on catalog context - never guess or invent information
- If no relevant information found, state: "No matching information found in the catalog"
- Always include full qualified path (server/database/schema/table/column)
- Mention if an object is a view rather than a table""",

        "Nederlands": """Voorbeeld:
- Structureer antwoorden als: A) Direct antwoord, B) Relevante tabellen/kolommen, C) Datatypes, D) Opmerkingen
- Baseer antwoorden alleen op catalog context - raad nooit en verzin geen informatie
- Als geen informatie gevonden: "Geen overeenkomende informatie gevonden in de catalog"
- Vermeld altijd het volledige pad (server/database/schema/tabel/kolom)
- Vermeld als een object een view is in plaats van een tabel"""
    }

    # Business rules editor
    business_rules_input = st.text_area(
        "Business rules & guidelines",
        value=st.session_state.business_rules,
        height=150,
        placeholder=business_rules_placeholders.get(st.session_state.prompt_language, business_rules_placeholders["English"]),
        help="Additional context appended to the system prompt"
    )
    st.session_state.business_rules = business_rules_input

    # Reset button
    if st.button("Reset to defaults", use_container_width=True):
        st.session_state.system_prompt = default_prompts[st.session_state.prompt_language]
        st.session_state.business_rules = ""
        st.rerun()


# Helper functions defined before use
def call_llm(model: str, question: str, context: str) -> str:
    """Call the selected LLM with the question and context."""

    # Build system prompt from session state
    system_prompt = st.session_state.system_prompt

    # Append business rules if provided
    if st.session_state.business_rules.strip():
        system_prompt += f"\n\nBusiness rules and guidelines:\n{st.session_state.business_rules}"

    user_prompt = f"""Context from data catalog:
{context}

Question: {question}"""

    if "Claude" in model:
        return call_claude(system_prompt, user_prompt)
    elif "Mistral" in model:
        return call_ollama(system_prompt, user_prompt)
    else:
        return f"Unknown model: {model}"


def call_claude(system: str, user: str) -> str:
    """Call Claude API."""
    import anthropic

    client = anthropic.Anthropic()  # Uses ANTHROPIC_API_KEY env var

    message = client.messages.create(
        model="claude-3-5-haiku-20241022",
        max_tokens=1024,
        system=system,
        messages=[
            {"role": "user", "content": user}
        ]
    )

    return message.content[0].text


def call_ollama(system: str, user: str) -> str:
    """Call Ollama server with Mistral model."""
    from ollama import Client

    host = os.environ.get('OLLAMA_HOST', 'http://localhost:11434')
    model = os.environ.get('OLLAMA_MODEL', 'mistral:instruct')

    client = Client(host=host)

    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": user}
    ]

    response = client.chat(model=model, messages=messages)

    return response.get("message", {}).get("content", "")


# Chat interface
if 'messages' not in st.session_state:
    st.session_state.messages = []

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("Ask about your data catalog..."):
    # Add user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Get relevant context from vector search
    with st.spinner("Searching catalog..."):
        context_results = search(prompt, n_results=num_context)

    # Build context string
    context_parts = []
    for r in context_results:
        obj_type = r['object_type'].replace('DB_', '').lower()
        item = f"- {r['qualified_name']} ({obj_type})"
        if r.get('data_type'):
            item += f" [{r['data_type']}]"
        if r.get('description'):
            item += f": {r['description']}"
        context_parts.append(item)

    context = "\n".join(context_parts)

    # Generate response
    with st.chat_message("assistant"):
        if not model_choice:
            response = f"""I found {len(context_results)} relevant catalog items, but no AI model is configured.

**Relevant items:**
{context}

To enable AI responses, add an API key to your `.env` file."""
            st.markdown(response)
        else:
            # Call the appropriate LLM
            with st.spinner("Thinking..."):
                try:
                    response = call_llm(model_choice, prompt, context)
                    st.markdown(response)
                except Exception as e:
                    response = f"Error calling AI: {e}\n\n**Context found:**\n{context}"
                    st.error(response)

    st.session_state.messages.append({"role": "assistant", "content": response})

# Show context in expander
if st.session_state.messages:
    with st.expander("View search context"):
        st.markdown("These catalog items were used as context for the last response:")
        if 'context_results' in dir():
            for r in context_results:
                st.markdown(f"- **{r['qualified_name']}**: {r.get('description', 'No description')[:100]}")


# Footer
st.divider()
st.caption("Tip: The AI uses your catalog descriptions to answer questions. Better descriptions = better answers.")

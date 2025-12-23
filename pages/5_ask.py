"""
Ask - Chat with your data catalog using AI.
Ask questions about your data and get answers based on catalog descriptions.
"""

import os
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

    # Check for API keys
    anthropic_key = os.environ.get('ANTHROPIC_API_KEY', '')
    mistral_url = os.environ.get('MISTRAL_API_URL', '')

    available_models = []

    if anthropic_key:
        available_models.append("Claude (Anthropic)")
    if mistral_url:
        available_models.append("Mistral (Local)")

    if not available_models:
        st.warning("No AI models configured")
        st.markdown("""
        Add to your `.env` file:

        **For Claude:**
        ```
        ANTHROPIC_API_KEY=sk-ant-...
        ```

        **For Mistral (local):**
        ```
        MISTRAL_API_URL=http://your-server:port
        ```
        """)
        model_choice = None
    else:
        model_choice = st.selectbox("Select model", available_models)

    st.divider()

    st.subheader("Search Settings")
    num_context = st.slider("Context items", 3, 20, 10,
                           help="Number of catalog items to include as context")

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


def call_llm(model: str, question: str, context: str) -> str:
    """Call the selected LLM with the question and context."""

    system_prompt = """You are a helpful data catalog assistant. You help users understand their data assets.

Use the following catalog context to answer questions. If the context doesn't contain enough information,
say so and suggest what additional information might help.

Be concise but thorough. Reference specific tables, columns, or other catalog items when relevant."""

    user_prompt = f"""Context from data catalog:
{context}

Question: {question}"""

    if "Claude" in model:
        return call_claude(system_prompt, user_prompt)
    elif "Mistral" in model:
        return call_mistral(system_prompt, user_prompt)
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


def call_mistral(system: str, user: str) -> str:
    """Call local Mistral server (OpenAI-compatible API)."""
    import requests

    url = os.environ.get('MISTRAL_API_URL', '').rstrip('/')

    response = requests.post(
        f"{url}/v1/chat/completions",
        json={
            "model": "mistral",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user}
            ],
            "max_tokens": 1024,
            "temperature": 0.7
        },
        timeout=60
    )

    response.raise_for_status()
    data = response.json()

    return data['choices'][0]['message']['content']


# Footer
st.divider()
st.caption("Tip: The AI uses your catalog descriptions to answer questions. Better descriptions = better answers.")

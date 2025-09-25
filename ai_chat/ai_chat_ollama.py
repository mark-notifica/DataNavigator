#!/usr/bin/env python3
import json
from pathlib import Path
from typing import List, Optional

import typer
from ollama import Client  # <-- gebruik de Client API
from rich.console import Console
from rich.panel import Panel
from rich.markdown import Markdown
from rich.prompt import Prompt

app = typer.Typer(add_completion=False)
console = Console()

# Defaults: jouw VPN-host en model
DEFAULT_HOST = "http://10.3.152.8:11434"
DEFAULT_MODEL = "mistral:instruct"
HISTORY_FILE = Path(".ollama_chat_history.json")


def load_history() -> List[dict]:
    if HISTORY_FILE.exists():
        try:
            return json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []


def save_history(messages: List[dict]) -> None:
    try:
        HISTORY_FILE.write_text(json.dumps(messages, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        console.print(f"[red]Kon geschiedenis niet opslaan:[/red] {e}")


def print_header(host: str, model: str, system: Optional[str]):
    console.print(Panel.fit(f"[b]Ollama Chat[/b]\nhost: [cyan]{host}[/cyan]\nmodel: [cyan]{model}[/cyan]", border_style="cyan"))
    if system:
        console.print(Panel(Markdown(f"**System prompt**:\n\n{system}"), border_style="grey50"))


@app.command()
def chat(
    host: str = typer.Option(DEFAULT_HOST, "--host", "-h", help="Ollama host, bv. http://10.3.152.8:11434"),
    model: str = typer.Option(DEFAULT_MODEL, "--model", "-m", help="Model tag, bv. mistral:instruct"),
    system: Optional[str] = typer.Option(None, "--system", "-s", help="Optionele system prompt"),
    stream: bool = typer.Option(True, "--stream/--no-stream", help="Streaming weergave"),
    reset: bool = typer.Option(False, "--reset", help="Begin zonder eerdere geschiedenis"),
):
    """
    Start een interactieve chat met jouw lokale Ollama Mistral.
    """
    # Maak een client naar jouw VPN-host
    client = Client(host=host)

    messages: List[dict] = [] if reset else load_history()
    if system:
        # vervang evt. bestaande system
        messages = [m for m in messages if m.get("role") != "system"]
        messages.insert(0, {"role": "system", "content": system})

    print_header(host, model, system)

    while True:
        try:
            user_msg = Prompt.ask("[bold magenta]Jij[/bold magenta]")
        except (KeyboardInterrupt, EOFError):
            console.print("\n[cyan]Afsluiten...[/cyan]")
            break

        if user_msg.strip().lower() in {"exit", "quit", ":q"}:
            console.print("[cyan]Tot later![/cyan]")
            break

        if user_msg.strip() == "/reset":
            messages = [{"role": "system", "content": system}] if system else []
            save_history(messages)
            console.print("[yellow]Geschiedenis geleegd.[/yellow]")
            continue

        messages.append({"role": "user", "content": user_msg})

        try:
            if stream:
                console.print("[bold blue]Mistral[/bold blue]: ", end="")
                assistant_text_parts: List[str] = []
                for chunk in client.chat(model=model, messages=messages, stream=True):
                    delta = (chunk.get("message", {}) or {}).get("content", "")
                    if delta:
                        assistant_text_parts.append(delta)
                        console.print(delta, end="")  # directe stream
                console.print()  # newline
                assistant_text = "".join(assistant_text_parts).strip()
            else:
                resp = client.chat(model=model, messages=messages)
                assistant_text = resp["message"]["content"]
                console.print(Panel(Markdown(assistant_text), border_style="blue"))

            messages.append({"role": "assistant", "content": assistant_text})
            save_history(messages)

        except Exception as e:
            console.print(f"[red]API-fout:[/red] {e}")


if __name__ == "__main__":
    app()


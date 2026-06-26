"""Model provider abstraction. Completion via Anthropic, embeddings via Voyage.
Both are swappable: implement LLMProvider / EmbeddingProvider and select via env."""
import os
from abc import ABC, abstractmethod

import anthropic
import voyageai


class EmbeddingProvider(ABC):
    dims: int

    @abstractmethod
    def embed(self, text: str) -> list[float]: ...


class CompletionProvider(ABC):
    @abstractmethod
    def complete(self, system: str | None, prompt: str, max_tokens: int) -> str: ...


# ---------- Embeddings: Voyage ----------
class VoyageEmbedding(EmbeddingProvider):
    def __init__(self):
        self.client = voyageai.Client(api_key=os.environ["VOYAGE_API_KEY"])
        self.model = os.environ.get("EMBED_MODEL", "voyage-3")
        # voyage-3 -> 1024 dims; keep in sync with the VECTOR(...) column.
        self.dims = int(os.environ.get("EMBED_DIMS", "1024"))

    def embed(self, text: str) -> list[float]:
        r = self.client.embed([text], model=self.model, input_type="document")
        return r.embeddings[0]


# ---------- Completion: Anthropic ----------
class AnthropicCompletion(CompletionProvider):
    def __init__(self):
        self.client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
        self.answer_model = os.environ.get("ANSWER_MODEL", "claude-sonnet-4-6")
        self.parse_model = os.environ.get("PARSE_MODEL", "claude-haiku-4-5-20251001")

    def complete(self, system, prompt, max_tokens=1024, model=None):
        msg = self.client.messages.create(
            model=model or self.answer_model,
            max_tokens=max_tokens,
            system=system or anthropic.NOT_GIVEN,
            messages=[{"role": "user", "content": prompt}],
        )
        return "".join(b.text for b in msg.content if b.type == "text")


# ---------- Singletons ----------
_embedding: EmbeddingProvider | None = None
_completion: AnthropicCompletion | None = None


def embedding_provider() -> EmbeddingProvider:
    global _embedding
    if _embedding is None:
        _embedding = VoyageEmbedding()
    return _embedding


def completion_provider() -> AnthropicCompletion:
    global _completion
    if _completion is None:
        _completion = AnthropicCompletion()
    return _completion

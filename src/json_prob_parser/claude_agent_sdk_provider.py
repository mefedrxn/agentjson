from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional


_DEFAULT_SYSTEM = (
    "You are a JSON repair assistant.\n"
    "Return ONLY valid JSON. No markdown. No explanations.\n"
    "Prefer minimal local edits.\n"
)


@dataclass
class ClaudeAgentSDKProvider:
    """
    Best-effort adapter for the Claude Agent SDK.

    This project keeps LLM integration optional and dependency-free. To use the Agent SDK:
      - Construct an Agent SDK agent in your app (with tools: memory, web search, skills, etc.)
      - Wrap it with this provider and pass as RepairOptions.llm_provider.

    The agent object only needs to support ONE of these call styles:
      - agent.run(prompt: str) -> str|dict
      - agent.invoke(prompt: str) -> str|dict
      - agent(prompt: str) -> str|dict
    """

    agent: Any
    system_prompt: str = _DEFAULT_SYSTEM
    render: Optional[Callable[[Mapping[str, Any]], str]] = None

    def __call__(self, payload: Mapping[str, Any]) -> Any:
        if self.render is not None:
            prompt = self.render(payload)
        else:
            prompt = (
                self.system_prompt
                + "\n"
                + "Given the following PAYLOAD, return a JSON object in one of these modes:\n"
                + " - {\"mode\":\"patch_suggest\",\"patches\":[{\"patch_id\":\"p1\",\"ops\":[...],\"confidence\":0.5}]}\n"
                + " - {\"mode\":\"token_suggest\",\"suggestions\":[{\"type\":\"insert_tokens\",\"tokens\":[\":\"],\"confidence\":0.5}]}\n"
                + "\n"
                + "PAYLOAD:\n"
                + json.dumps(payload, ensure_ascii=False)
            )

        agent = self.agent
        if hasattr(agent, "run") and callable(getattr(agent, "run")):
            return agent.run(prompt)
        if hasattr(agent, "invoke") and callable(getattr(agent, "invoke")):
            return agent.invoke(prompt)
        if callable(agent):
            return agent(prompt)
        raise TypeError("agent must be callable or provide .run/.invoke")

    @classmethod
    def from_env(cls) -> "ClaudeAgentSDKProvider":
        """
        Convenience helper that attempts to construct an Agent SDK agent using env vars.

        This is intentionally conservative because Agent SDK APIs can vary by version.
        If this fails, construct the agent in your application and pass it to ClaudeAgentSDKProvider(agent=...).
        """
        # Note: This code is best-effort (import paths may differ by SDK version).
        module_names = [
            "claude_agent_sdk",
            "claude_agent",
            "anthropic_agent_sdk",
        ]
        last_err: Optional[Exception] = None
        for name in module_names:
            try:
                mod = __import__(name, fromlist=["Agent"])
            except Exception as e:  # noqa: BLE001
                last_err = e
                continue

            Agent = getattr(mod, "Agent", None)
            if Agent is None:
                last_err = ImportError(f"{name}.Agent not found")
                continue

            model = os.environ.get("CLAUDE_MODEL") or os.environ.get("ANTHROPIC_MODEL") or ""
            api_key = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_API_KEY") or ""
            try:
                # Common constructor patterns:
                #   Agent(model=..., api_key=...)
                #   Agent(model=...)
                #   Agent(...)
                if api_key and model:
                    agent = Agent(model=model, api_key=api_key)
                elif model:
                    agent = Agent(model=model)
                else:
                    agent = Agent()
            except Exception as e:  # noqa: BLE001
                last_err = e
                continue

            return cls(agent=agent)

        raise RuntimeError(f"Failed to initialize Agent SDK provider: {last_err}")


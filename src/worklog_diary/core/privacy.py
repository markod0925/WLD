from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class PrivacyPolicyEngine:
    blocked_processes: set[str] = field(default_factory=set)

    @classmethod
    def with_defaults(cls) -> "PrivacyPolicyEngine":
        return cls(blocked_processes={"chrome.exe", "msedge.exe", "webex.exe"})

    def is_blocked_process(self, process_name: str) -> bool:
        return process_name.lower() in self.blocked_processes

    def is_blocked(self, process_name: str) -> bool:
        return self.is_blocked_process(process_name)

    def update_blocked_processes(self, blocked: list[str]) -> None:
        self.blocked_processes = {item.strip().lower() for item in blocked if item.strip()}

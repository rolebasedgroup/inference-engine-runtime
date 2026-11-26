"""Read hook signals from a downwardAPI label file.

RBG syncs RoleInstance ``signalLabel`` values onto the Pod labels. The pod
projects its labels into a file through the downwardAPI volume, and Patio
polls that file instead of the Kubernetes API:

    volumes:
    - name: podinfo
      downwardAPI:
        items:
        - path: pod-labels
          fieldRef:
            fieldPath: metadata.labels

Reading from the file removes the per-second API-server polling. Clearing a
signal still goes through the Kubernetes API, because the downwardAPI file is
read-only.
"""

from __future__ import annotations

import os
from pathlib import Path

from patio.lifecycle.signal_label import KubernetesSignalLabelSource


class FileSignalSource:
    """Poll a downwardAPI label file and clear signals through the API."""

    def __init__(
        self,
        labels_path: str | Path | None = None,
        clear_source: KubernetesSignalLabelSource | None = None,
        expected_signals: set[str] | None = None,
    ):
        self.labels_path = Path(
            labels_path or os.getenv("LIFECYCLE_SIGNAL_FILE", "/etc/patio/pod-labels")
        )
        # The clear path needs the RoleInstance name and in-cluster API access.
        self._clear_source = clear_source or KubernetesSignalLabelSource()
        self._expected_signals = expected_signals
        self._last_signature: tuple[int, str] | None = None
        self._last_authoritative: set[str] | None = None

    async def signals(self) -> set[str]:
        """Return the labels currently projected into the file.

        A missing file is not an error: the volume may not be mounted yet or
        the kubelet may not have written the initial content.
        """

        try:
            content = self.labels_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return set()
        signals = parse_downward_labels(content)
        if self._expected_signals is not None:
            signals &= self._expected_signals
        if not signals:
            return set()

        signature = (self.labels_path.stat().st_mtime_ns, content)
        if self._last_signature == signature and self._last_authoritative is not None:
            return signals & self._last_authoritative

        # The downwardAPI file is a non-authoritative mirror of Pod labels.
        # Before any callback side effect runs, validate the candidate signals
        # against the RoleInstance labels.  This keeps file mode from turning a
        # forged Pod label into an offboard/onboard command.
        try:
            authoritative = await self._clear_source.signals()
        except Exception:
            self._last_signature = None
            self._last_authoritative = None
            raise

        self._last_signature = signature
        self._last_authoritative = authoritative
        return signals & authoritative

    async def validate(self) -> None:
        """Validate Kubernetes credentials and RBAC at startup."""

        await self._clear_source.signals()

    async def clear(self, signal: str) -> None:
        """Clear the signal on the RoleInstance through the Kubernetes API."""

        await self._clear_source.clear(signal)


def parse_downward_labels(content: str) -> set[str]:
    """Parse the downwardAPI label file format: one ``key="value"`` per line.

    Values escape ``"`` and ``\\`` with a backslash, matching the kubelet
    projection format.
    """

    signals: set[str] = set()
    for line in content.splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        value = _unescape(raw_value.strip())
        if not key or not value:
            continue
        signals.add(f"{key}={value}")
    return signals


def _unescape(quoted: str) -> str:
    if len(quoted) >= 2 and quoted.startswith('"') and quoted.endswith('"'):
        quoted = quoted[1:-1]
    result: list[str] = []
    escaped = False
    for char in quoted:
        if escaped:
            result.append(char)
            escaped = False
        elif char == "\\":
            escaped = True
        else:
            result.append(char)
    return "".join(result)

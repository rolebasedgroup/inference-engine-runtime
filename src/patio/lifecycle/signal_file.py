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
    ):
        self.labels_path = Path(
            labels_path or os.getenv("LIFECYCLE_SIGNAL_FILE", "/etc/patio/pod-labels")
        )
        # The clear path needs the RoleInstance name and in-cluster API access.
        self._clear_source = clear_source or KubernetesSignalLabelSource()

    async def signals(self) -> set[str]:
        """Return the labels currently projected into the file.

        A missing file is not an error: the volume may not be mounted yet or
        the kubelet may not have written the initial content.
        """

        try:
            content = self.labels_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return set()
        return parse_downward_labels(content)

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

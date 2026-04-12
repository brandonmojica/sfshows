from __future__ import annotations

import subprocess
from typing import Optional


class NotificationError(Exception):
    pass


def _escape_applescript(text: str) -> str:
    """Escape backslashes and double quotes for use in an AppleScript string literal."""
    return text.replace("\\", "\\\\").replace('"', '\\"')


def send_imessage_to_recipient(recipient: str, message: str) -> None:
    """
    Send a message to a single iMessage recipient (phone number or Apple ID email).

    recipient: e.g. "+15551234567" or "user@example.com"
    """
    escaped_recipient = _escape_applescript(recipient)
    escaped_msg = _escape_applescript(message)

    script = f"""
tell application "Messages"
    set targetService to 1st account whose service type = iMessage
    set targetBuddy to buddy "{escaped_recipient}" of targetService
    send "{escaped_msg}" to targetBuddy
end tell
"""
    _run_osascript(script, context=f"recipient '{recipient}'")


def send_imessage_to_group(group_name: str, message: str) -> None:
    """
    Send a message to a named iMessage group chat.

    group_name must match exactly (case-sensitive) what appears in Messages.app.
    """
    escaped_name = _escape_applescript(group_name)
    escaped_msg = _escape_applescript(message)

    script = f"""
tell application "Messages"
    set targetService to 1st account whose service type = iMessage
    set targetChat to 1st chat whose name = "{escaped_name}"
    send "{escaped_msg}" to targetChat
end tell
"""
    try:
        _run_osascript(script, context=f"group '{group_name}'")
    except NotificationError as e:
        if "Can't get chat" in str(e) or "can't get chat" in str(e).lower():
            available = _list_chat_names()
            hint = ""
            if available:
                hint = "\n\nAvailable chats:\n" + "\n".join(f"  - {n}" for n in available)
            raise NotificationError(
                f"iMessage group '{group_name}' not found.{hint}\n\n"
                "Check that the name in config.yaml matches exactly."
            ) from None
        raise


def send_imessage(
    message: str,
    recipients: tuple[str, ...] = (),
    group_name: Optional[str] = None,
) -> None:
    """
    Send message to all configured recipients and/or a group chat.
    Raises NotificationError on any failure.
    """
    if not recipients and not group_name:
        raise NotificationError("No recipients or group_name configured in config.yaml")

    errors: list[str] = []

    for recipient in recipients:
        try:
            send_imessage_to_recipient(recipient, message)
        except NotificationError as e:
            errors.append(str(e))

    if group_name:
        try:
            send_imessage_to_group(group_name, message)
        except NotificationError as e:
            errors.append(str(e))

    if errors:
        raise NotificationError("\n".join(errors))


def _run_osascript(script: str, context: str = "") -> None:
    """Execute an AppleScript snippet via osascript, raising NotificationError on failure."""
    result = subprocess.run(
        ["osascript", "-e", script],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        label = f" ({context})" if context else ""
        raise NotificationError(f"osascript failed{label}: {stderr}")


def _list_chat_names() -> list[str]:
    """Return the names of all chats in Messages.app (for debugging)."""
    script = """
tell application "Messages"
    set chatNames to {}
    repeat with c in chats
        try
            set end of chatNames to name of c
        end try
    end repeat
    return chatNames
end tell
"""
    result = subprocess.run(
        ["osascript", "-e", script],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode == 0 and result.stdout.strip():
        return [n.strip() for n in result.stdout.strip().split(",") if n.strip()]
    return []

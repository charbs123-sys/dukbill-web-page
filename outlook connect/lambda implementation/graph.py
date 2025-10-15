# graph.py
# Minimal Microsoft Graph client (REST) that uses an ALREADY-ISSUED access token.

from __future__ import annotations
import base64
import requests
from typing import Dict, List, Any, Optional

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

class GraphClient:
    def __init__(self, access_token: str, timeout: int = 20):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        })
        self.timeout = timeout

    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        r = self.session.get(url, params=params, timeout=self.timeout)
        if r.status_code >= 400:
            raise RuntimeError(f"Graph GET {url} failed: {r.status_code} {r.text[:500]}")
        return r.json()

    def _get_bytes(self, url: str) -> bytes:
        r = self.session.get(url, timeout=self.timeout)
        if r.status_code >= 400:
            raise RuntimeError(f"Graph GET {url} failed: {r.status_code} {r.text[:500]}")
        return r.content

    # -------- Messages (Inbox) --------
    def list_inbox_messages_since_with_attachments(
        self,
        since_iso_utc: str,
        page_size: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Returns ALL Inbox messages with attachments received since since_iso_utc (UTC ISO 8601),
        paging through @odata.nextLink.
        """
        url = f"{GRAPH_BASE}/me/mailFolders/inbox/messages"
        params = {
            "$filter": f"receivedDateTime ge {since_iso_utc} and hasAttachments eq true",
            "$select": "id,subject,receivedDateTime,from,bodyPreview,hasAttachments,conversationId,body",
            "$orderby": "receivedDateTime desc",
            "$top": page_size,
        }

        items: List[Dict[str, Any]] = []
        data = self._get(url, params=params)
        items.extend(data.get("value", []))

        next_link = data.get("@odata.nextLink")
        while next_link:
            data = self._get(next_link)
            items.extend(data.get("value", []))
            next_link = data.get("@odata.nextLink")

        return items

    # -------- Attachments --------
    def list_attachments_for_message(self, message_id: str) -> List[Dict[str, Any]]:
        """
        Return lightweight attachment metadata for a message.
        DO NOT $select '@odata.type' or 'contentBytes' here.
        """
        url = f"{GRAPH_BASE}/me/messages/{message_id}/attachments"
        params = {
            # safe fields to select
            "$select": "id,name,contentType,size,isInline"
        }

        items: List[Dict[str, Any]] = []
        data = self._get(url, params=params)
        items.extend(data.get("value", []))

        next_link = data.get("@odata.nextLink")
        while next_link:
            data = self._get(next_link)
            items.extend(data.get("value", []))
            next_link = data.get("@odata.nextLink")
        return items

    def get_attachment_by_id(self, message_id: str, attachment_id: str) -> Dict[str, Any]:
        """
        Fetch a single attachment object by id.
        For fileAttachment, this typically includes 'contentBytes' (<= ~3–4 MB).
        """
        url = f"{GRAPH_BASE}/me/messages/{message_id}/attachments/{attachment_id}"
        return self._get(url)

    def get_attachment_content_b64(self, message_id: str, attachment_id: str) -> str:
        """
        Returns base64 of the attachment content.
        1) Try 'contentBytes' from the JSON.
        2) If absent, fall back to GET .../attachments/{id}/$value (raw bytes) and base64-encode.
        """
        att = self.get_attachment_by_id(message_id, attachment_id)
        b64 = att.get("contentBytes")
        if isinstance(b64, str) and b64:
            return b64

        # Fallback for large fileAttachment: $value returns the raw bytes
        raw_url = f"{GRAPH_BASE}/me/messages/{message_id}/attachments/{attachment_id}/$value"
        raw = self._get_bytes(raw_url)
        return base64.b64encode(raw).decode("utf-8")

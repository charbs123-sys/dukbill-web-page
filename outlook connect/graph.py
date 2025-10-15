from configparser import SectionProxy
from azure.identity import DeviceCodeCredential
from msgraph import GraphServiceClient
from msgraph.generated.users.item.user_item_request_builder import UserItemRequestBuilder
from msgraph.generated.users.item.mail_folders.item.messages.messages_request_builder import (
    MessagesRequestBuilder)
from msgraph.generated.users.item.send_mail.send_mail_post_request_body import (
    SendMailPostRequestBody)
from msgraph.generated.models.message import Message
from msgraph.generated.models.item_body import ItemBody
from msgraph.generated.models.body_type import BodyType
from msgraph.generated.models.recipient import Recipient
from msgraph.generated.models.email_address import EmailAddress
import binascii

from datetime import datetime, timedelta, timezone
from msgraph.generated.users.item.messages.messages_request_builder import (
    MessagesRequestBuilder
)
from msgraph.generated.models.attachment import Attachment
from msgraph.generated.models.file_attachment import FileAttachment
from msgraph.generated.models.o_data_errors.o_data_error import ODataError

# graph.py
from configparser import SectionProxy
from datetime import datetime, timedelta, timezone
import base64

from azure.identity import DeviceCodeCredential
from msgraph import GraphServiceClient

from msgraph.generated.users.item.user_item_request_builder import UserItemRequestBuilder
from msgraph.generated.users.item.mail_folders.item.messages.messages_request_builder import (
    MessagesRequestBuilder
)
from msgraph.generated.models.file_attachment import FileAttachment

class Graph:
    settings: SectionProxy
    device_code_credential: DeviceCodeCredential
    user_client: GraphServiceClient

    def __init__(self, config: SectionProxy):
        self.settings = config
        client_id = self.settings['clientId']
        tenant_id = self.settings['tenantId']
        graph_scopes = self.settings['graphUserScopes'].split()

        self.device_code_credential = DeviceCodeCredential(client_id=client_id, tenant_id=tenant_id)
        self.user_client = GraphServiceClient(self.device_code_credential, graph_scopes)

    async def get_user_token(self):
        scopes = self.settings['graphUserScopes'].split()
        token = self.device_code_credential.get_token(*scopes)
        return token.token

    async def get_user(self):
        params = UserItemRequestBuilder.UserItemRequestBuilderGetQueryParameters(
            select=['displayName', 'mail', 'userPrincipalName']
        )
        cfg = UserItemRequestBuilder.UserItemRequestBuilderGetRequestConfiguration(
            query_parameters=params
        )
        return await self.user_client.me.get(request_configuration=cfg)

    async def _list_messages_since_with_attachments(self, since_utc: datetime, page_size: int = 50):
        """
        Page through messages since 'since_utc' from Inbox.
        We filter by date on the server, and filter attachments client-side to avoid 'InefficientFilter'.
        Returns a list of SDK message objects.
        """
        if since_utc.tzinfo is None:
            since_utc = since_utc.replace(tzinfo=timezone.utc)
        since_iso = since_utc.isoformat().replace("+00:00", "Z")

        def _build_params(orderby: bool):
            return MessagesRequestBuilder.MessagesRequestBuilderGetQueryParameters(
                # keep payload small; we'll fetch full body/attachments later per message
                select=['id', 'subject', 'receivedDateTime', 'from', 'hasAttachments', 'conversationId', 'bodyPreview'],
                filter=f"receivedDateTime ge {since_iso}",
                orderby=(['receivedDateTime DESC'] if orderby else None),
                top=page_size,
            )

        all_msgs = []

        async def _run(params):
            cfg = MessagesRequestBuilder.MessagesRequestBuilderGetRequestConfiguration(query_parameters=params)
            # Query a *folder* (Inbox) to avoid expensive “all mail” scans
            page = await self.user_client.me.mail_folders.by_mail_folder_id('inbox').messages.get(request_configuration=cfg)
            while True:
                # client-side filter: attachments only
                for m in (page.value or []):
                    if getattr(m, 'has_attachments', False):
                        all_msgs.append(m)

                next_link = page.odata_next_link
                if not next_link:
                    break
                page = await self.user_client.me.mail_folders.by_mail_folder_id('inbox').messages.with_url(next_link).get()

        try:
            # Preferred: date filter + orderby (fast on Inbox)
            await _run(_build_params(orderby=True))
        except ODataError as e:
            # Fallback for tenants that still complain: drop orderby, keep filter
            if "InefficientFilter" in str(e):
                await _run(_build_params(orderby=False))
            else:
                raise

        return all_msgs

    async def _get_message_attachments(self, message_id: str):
        """
        Fetch full attachment listing for a message (ensures we can access contentBytes).
        """
        atts_page = await self.user_client.me.messages.by_message_id(message_id).attachments.get()
        return atts_page.value or []

    async def get_pdf_threads_since(self, since_utc: datetime, limit_per_thread: int | None = None):
        """
        Returns a dict keyed by conversationId (thread id). Each value is a list of dicts:
        {
          "body": <str>,
          "date": <ISO8601 str>,
          "from_": <str>,
          "subject": <str>,
          "pdfs": [<name>, ...],
          "pdfencoded": [<base64 str>, ...],
          "processed": 0
        }
        Only messages that contain >=1 PDF are included.
        """
        msgs = await self._list_messages_since_with_attachments(since_utc)

        threads: dict[str, list[dict]] = {}

        for msg in msgs:
            # Gather attachments (use expanded set if present; otherwise fetch)
            atts = getattr(msg, "attachments", None)
            if atts is None:
                atts = await self._get_message_attachments(msg.id)

            pdf_names: list[str] = []
            pdf_b64s: list[str] = []

            for att in atts or []:
                # Only file attachments can be PDFs
                is_file = isinstance(att, FileAttachment) or getattr(att, "@odata.type", "").endswith("fileAttachment")
                if not is_file:
                    continue

                name = (att.name or "")
                ctype = (getattr(att, "content_type", None) or "")
                if name.lower().endswith(".pdf") or ctype.lower() == "application/pdf":
                    # Ensure we have content as base64 string
                    content = getattr(att, "content_bytes", None)
                    # content_bytes is often bytes in SDK; convert to base64 str
                    if isinstance(content, (bytes, bytearray)):
                        b64_str = base64.b64encode(content).decode("utf-8")
                    elif isinstance(content, str):
                        # Some SDKs already expose base64 string
                        b64_str = content
                    else:
                        # If content not present via expand, refetch this attachment by id
                        att_full = await self.user_client.me.messages.by_message_id(msg.id)\
                            .attachments.by_attachment_id(att.id).get()
                        full_bytes = getattr(att_full, "content_bytes", None)
                        if isinstance(full_bytes, (bytes, bytearray)):
                            b64_str = base64.b64encode(full_bytes).decode("utf-8")
                        elif isinstance(full_bytes, str):
                            b64_str = full_bytes
                        else:
                            # Skip if we truly can't read content
                            continue

                    pdf_names.append(name)
                    pdf_b64s.append(b64_str)

            if not pdf_names:
                continue  # skip messages that don't actually have PDFs

            body_content = ""
            try:
                # Prefer full body; fall back to preview
                body_content = (getattr(getattr(msg, "body", None), "content", None)
                                or getattr(msg, "body_preview", None) or "")
            except Exception:
                pass

            from_addr = None
            try:
                if msg.from_ and msg.from_.email_address:
                    from_addr = msg.from_.email_address.address or msg.from_.email_address.name
            except Exception:
                pass

            entry = {
                "body": body_content,
                "date": (msg.received_date_time.isoformat() if msg.received_date_time else None),
                "from_": from_addr,
                "subject": msg.subject,
                "pdfs": pdf_names,
                "pdfencoded": pdf_b64s,
                "processed": 0
            }

            conv_id = msg.conversation_id or msg.id  # fall back to message id just in case
            bucket = threads.setdefault(conv_id, [])
            bucket.append(entry)

            # optional cap per thread to avoid runaway growth
            if limit_per_thread is not None and len(bucket) >= limit_per_thread:
                continue

        # Sort each thread chronologically (oldest→newest) for easier downstream use
        for conv_id, items in threads.items():
            items.sort(key=lambda x: x["date"] or "")
        return threads
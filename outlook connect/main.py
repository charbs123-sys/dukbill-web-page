import asyncio
import configparser
from msgraph.generated.models.o_data_errors.o_data_error import ODataError
from graph import Graph
from datetime import datetime, timedelta, timezone
import json
import os

async def main():
    print('Python Graph Tutorial\n')

    # Load settings
    config = configparser.ConfigParser()
    config.read(['config.cfg', 'config.dev.cfg'])
    azure_settings = config['azure']

    graph: Graph = Graph(azure_settings)

    await greet_user(graph)

    choice = -1

    while choice != 0:
        print('Please choose one of the following options:')
        print('0. Exit')
        print('1. Display access token')
        print('2. List my inbox')
        print('3. Send mail')
        print('4. Make a Graph call')
        print('5. collect user emails')

        try:
            choice = int(input())
        except ValueError:
            choice = -1

        try:
            if choice == 0:
                print('Goodbye...')
            elif choice == 1:
                await display_access_token(graph)
            elif choice == 2:
                await list_inbox(graph)
            elif choice == 3:
                await send_mail(graph)
            elif choice == 4:
                await make_graph_call(graph)
            elif choice == 5:
                await list_threads_with_pdfs_last_two_years(graph)
            else:
                print('Invalid choice!\n')
        except ODataError as odata_error:
            print('Error:')
            if odata_error.error:
                print(odata_error.error.code, odata_error.error.message)

async def greet_user(graph: Graph):
    user = await graph.get_user()
    if user:
        print('Hello,', user.display_name)
        # For Work/school accounts, email is in mail property
        # Personal accounts, email is in userPrincipalName
        print('Email:', user.mail or user.user_principal_name, '\n')

async def display_access_token(graph: Graph):
    token = await graph.get_user_token()
    print('User token:', token, '\n')

async def list_inbox(graph: Graph):
    message_page = await graph.get_inbox()
    if message_page and message_page.value:
        # Output each message's details
        for message in message_page.value:
            print('Message:', message.subject)
            if (
                message.from_ and
                message.from_.email_address
            ):
                print('  From:', message.from_.email_address.name or 'NONE')
            else:
                print('  From: NONE')
            print('  Status:', 'Read' if message.is_read else 'Unread')
            print('  Received:', message.received_date_time)

        # If @odata.nextLink is present
        more_available = message_page.odata_next_link is not None
        print('\nMore messages available?', more_available, '\n')

async def send_mail(graph: Graph):
    # TODO
    return

async def make_graph_call(graph: Graph):
    # TODO
    return

async def list_pdfs_last_two_years(graph: Graph):
    from datetime import datetime, timedelta, timezone
    since = datetime.now(timezone.utc) - timedelta(days=730)
    msgs = await graph.get_messages_with_pdf_attachments_since(since)
    print(f"\nFound {len(msgs)} message(s) with PDF attachments since {since.date()}.\n")
    # Show a concise view
    for m in msgs[:25]:  # cap console spam
        when = m["receivedDateTime"]
        print(f"- {when} | {m['subject']}")
        print(f"  From: {m['from'] or 'Unknown'}")
        for a in m["pdf_attachments"]:
            print(f"    • {a['name']} ({a['size']} bytes)")
    if len(msgs) > 25:
        print(f"\n…and {len(msgs)-25} more.\n")

async def list_threads_with_pdfs_last_two_years(graph: Graph):
    since = datetime.now(timezone.utc) - timedelta(days=730)
    threads = await graph.get_pdf_threads_since(since)

    base_dir = os.getcwd()
    json_path = os.path.join(base_dir, f"pdf_threads.json")
    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump(threads, jf, ensure_ascii=False, indent=2)

    print(f"\nThreads with PDFs since {since.date()}: {len(threads)}\n")
    # Show a tiny summary
    shown = 0
    for conv_id, messages in threads.items():
        print(f"Conversation {conv_id}: {len(messages)} message(s) with PDFs")
        for m in messages[:2]:  # peek at first 2
            print(f"  - {m['date']} | {m['subject']} | PDFs: {len(m['pdfs'])}")
        shown += 1
        if shown >= 10:
            print("... (truncated display)\n")
            break

    # Optionally write to disk:
    out_path = os.path.join(os.getcwd(), "pdf_threads_last_2y.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(threads, f, ensure_ascii=False, indent=2)
    print(f"\nSaved JSON to: {out_path}\n")

if __name__ == "__main__":
    asyncio.run(main())
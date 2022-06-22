from typing import List, Optional
import base64
import os

import requests


email_api_url = os.getenv("AZURE_GEORGIA_EMAIL_API_URL")


def send_email(
    to_address: str,
    subject: str,
    html_body: str,
    attachment_file_loc_list: List[str],
    cc: Optional[str] = "",
):
    email_attachment_list = []
    for file in attachment_file_loc_list:
        # We must process each attachment into a base64 encoded string
        # why? because it's the only way I could get the thing to work
        with open(file, "rb") as f:
            email_attachment_list.append(
                {
                    "Name": f.name.split("/")[-1],
                    "ContentBytes": base64.b64encode(f.read()).decode("utf-8"),
                }
            )
    email_request = {
        "to": to_address,
        "cc": cc,
        "subject": subject,
        "attachments": email_attachment_list,
        "html": html_body,
    }
    requests.post(email_api_url, json=email_request)

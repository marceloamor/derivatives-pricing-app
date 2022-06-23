from typing import List, Optional, Tuple, Union
import base64
import os

import requests


email_api_url = os.getenv("AZURE_GEORGIA_EMAIL_API_URL")


def send_email(
    to_address: str,
    subject: str,
    html_body: str,
    attachment_file_loc_list: List[Union[str, Tuple[str, str]]],
    cc: Optional[str] = "",
):
    """Sends an email over an Azure Logic App defined API URL
    via a POST request.

    :param to_address: Destination email address.
    :type to_address: str
    :param subject: Email subject.
    :type subject: str
    :param html_body: HTML body of the email, can be entirely within
    <body> and </body> tags.
    :type html_body: str
    :param attachment_file_loc_list: List of file locations to attach,
    can so be a list of tuples containing `(file location, attachment name)`.
    This allows for attachments to be named differently to the system
    file they're derived from.
    :type attachment_file_loc_list: List[Union[str, Tuple[str, str]]]
    :param cc: Copied in email address, defaults to ""
    :type cc: Optional[str], optional
    """
    email_attachment_list = []
    for file_loc in attachment_file_loc_list:
        # We must process each attachment into a base64 encoded string
        # why? because it's the only way I could get the thing to work
        if isinstance(file_loc, tuple):
            file_name = file_loc[1]
            file_loc = file_loc[0]
        elif isinstance(file_loc, str):
            file_name = file_loc.split("/")[-1]

        with open(file_loc, "rb") as f:
            file_content = base64.b64encode(f.read()).decode("utf-8")
            email_attachment_list.append(
                {
                    "Name": file_name,
                    "ContentBytes": file_content,
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

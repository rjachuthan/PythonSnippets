#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Code to send mail using win32com.
"""
from win32com.client import Dispatch

PATH_TO_ATTACHMENT = ""

OUTLOOK = Dispatch("Outlook.Application")
MAIL = OUTLOOK.CreateItem(0)
MAIL.To = "xyz@company.com"
MAIL.CC = "abc@company.com"
MAIL.Subject = "This is subject"
MAIL.HTMLBody = (
    "<html>"
    "<body>"
    "Hi Team,<br>This is eMAIL.body:eMAIL."
    "<h1>This is Heading 1.</h1>"
    "Something Something"
    "</body>"
    "</html>"
)
MAIL.BodyFormat = 2
MAIL.Attachments.Add(PATH_TO_ATTACHMENT)
MAIL.Send()

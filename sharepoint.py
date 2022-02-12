#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Functions to help in Sharepoint operations using Python. This script contains
three functions:
    - file_download() - to download single file from sharepoint
    - file_upload() - to upload single file to Sharepoint
    - get_folder_list() - to get list of all the files present in Sharepoint
    - TODO: create_folder()
"""

import sharepy


def file_download(website, splink, username, password, filepath):
    """
    Function to download a single file from Sharepoint location
        param website (str) : Website link. Eg, https://abc.sharepoint.com
        param splink (str) : Sharepoint file link to be downloaded
        param username (str) : Username to login to Sharepoint
        param password (str) : Password to login to Sharepoint
        param filepath (str) : Local path to download the file
    """

    sess = sharepy.connect(site=website, username=username, password=password)
    file = sess.getfile(splink, filename=filepath)

    if file.status_code == 200:
        print("File downloaded successfully.")
    else:
        print("Unable to download file from Sharepoint.")
        print(f"Error Code: {file.status_code}")
    sess.close()


def file_upload(website, site, relpath, filename, inputfile, username, password):
    """
    Function to upload single file from local machine to Sharepoint
        param website (str) : Sharepoint link. Eg, https://abc.sharepoint.com
        param site (str) : Sharepoint site/teams names, Eg site/myfolder
        param relpath (str) : Relative path to upload folder in Sharepoint
        param filename (str) : Filename of the upload
        param inputfile (str) : File path and extension of the file to uploaded
        param username (str) : Username to login to Sharepoint
        param password (str) : Password to login to Sharepoint
    """
    sess = sharepy.connect(site=website, username=username, password=password)

    headers = {
        "accept": "application/json;odata=verbose",
        "content-type": "application/x-www-urlencoded; charset=UTF-8",
    }

    with open(inputfile, "rb") as read_file:
        content = read_file.read()

    # Upload link
    uploadpath = (
        f"{website}/{site}/_api/web/GetFolderByServerRelativeUrl"
        f"('/{relpath}')/Files/add(url='{filename}',overwrite=true)"
    )

    sess.post(uploadpath, data=content, headers=headers)
    sess.close()


def get_folder_list(website, site, library, relpath, username, password):
    """
    Function to get list of all the files present in a folder in Sharepoint.
    The API call has a limit of only 5000 files. Therefore, this activity has
    to be done recursively. After first call, the next call is made after
    'fileid'.
        param website (str) : Sharepoint link. Eg, https://abc.sharepoint.com
        param site (str) : Sharepoint site/teams names, Eg site/myfolder
        param library (str) : Sharepoint Library. Refer SP sidebar on left.
        param relpath (str) : Relative path to upload folder in Sharepoint
        param username (str) : Username to login to Sharepoint
        param password (str) : Password to login to Sharepoint
    """
    sess = sharepy.connect(site=website, username=username, password=password)

    # Count of JSON returns
    list1 = []
    condt = True
    fileid = ""
    while condt:
        link = (
            f"{site}/_api/web/lists/getbytitle('{library}')/items"
            f"?$select=FileLeafRef,FileRef,Id&top={5000}&%24"
            f"skiptoken=Paged%3DTRUE%26p_ID%{fileid}"
        )
        files = sess.get(link).json()["d"]["results"]
        list1 = list1 + files
        # Get the ID of the last element in the list
        # The next loop will continue from this Id onwards
        fileid = files[-1]["Id"]

        if len(files) != 5000:
            condt = False

    output_list: list = []
    for file in list1:
        fullpath = file["FileRef"]
        if (fullpath.startswith(relpath)) & (fullpath != relpath):
            output_list.append(file["FileRef"])

    # DEPRECATED:
    # relpath = (f"{site}/Shared Documents/RPA_Templates/I&A Projects/NAMETRUB/RUB"
    #            f"/Volume Report/{previous_qtrnm}")
    # r = ses.get(f"{website}/{site}/{api_call}('/{relpath}')/Files")
    # data = r.json()
    # file_date = [(elm['Name'], elm["TimeCreated"])for elm in data["d"]["results"]]
    # df_files = pd.DataFrame(file_date, columns=["File_name", "Created_date"])

    return output_list

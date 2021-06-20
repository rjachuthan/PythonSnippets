# Instagram Dashboard in StreamLit

This app has been copied from below blog:
https://predictivehacks.com/how-to-create-an-instagram-profile-analyzer-app-using-python-and-streamlit/

1. This app will first download the files from Instagram using the instagram-scraper tool.
NOTE: In my experience this tool rarely works. I am not sure why. But fortunately I was able to download the metadata for one Instagram account. And I continued developing using that.
2. Then using `json` package in python, this app will read the content into a `pandas` dataframe.
3. Create a dashboard using StreamLit.

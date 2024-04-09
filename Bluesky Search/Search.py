import requests


def search_bluesky_posts(keywords):
    """
    Searches for posts on Bluesky using a list of keywords.

    :param keywords: A list of keywords to search for.
    :return: A list of posts that match the keywords.
    """
    api_url = "https://bsky.social"  # Hypothetical URL, replace with actual
    headers = {
        "Authorization": "Bearer YOUR_ACCESS_TOKEN",  # Replace with your actual token
        "Content-Type": "application/json"
    }
    payload = {
        "keywords": keywords,
        "options": {  # Example options, adjust according to actual API capabilities
            "include_retweets": False,
            "language": "en"
        }
    }

    try:
        response = requests.post(api_url, json=payload, headers=headers)
        response.raise_for_status()  # Raises a HTTPError if the status is 4xx, 5xx
        posts = response.json()
        return posts
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Error during requests to {api_url} : {err}")


if __name__ == '__main__':
    keywords = ["Ukraine", "Russia", "kyiv"]
    posts = search_bluesky_posts(keywords)
    print(posts)

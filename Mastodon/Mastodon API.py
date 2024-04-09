from mastodon import Mastodon, StreamListener

# Initialize Mastodon instance
m = Mastodon(access_token="3ZBbePUdiYEAC-O0Kk8SC9Z7j9XlC3tMb7D6G0zXj4Q",
             api_base_url="https://mastodon.social")


# Define a custom stream listener
class MyStreamListener(StreamListener):
    def __init__(self, hashtag):
        super().__init__()
        self.hashtag = hashtag.lower()  # Convert hashtag to lowercase for case-insensitive comparison

    def on_update(self, status):
        if self.hashtag in status["content"].lower():
            print(status["content"])


# Initialize the stream listener with the hashtag filter
stream_listener = MyStreamListener("Gaza")  # Adjust the hashtag as needed

# Stream public toots
m.stream_public(listener=stream_listener)
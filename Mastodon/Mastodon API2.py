import threading
import time

from mastodon import Mastodon, StreamListener

_STOP_AFTER_SECONDS = 3

# Custom stream listener class
class MyStreamListener(StreamListener):
    def on_update(self, status):
        if "Ukraine" in status["content"]:
            print(status["content"])

def _stop_after_n_sec() -> None:
    time.sleep(_STOP_AFTER_SECONDS)
    m.stream_hashtag.stop()

# Initialize Mastodon instance
m = Mastodon(access_token="3ZBbePUdiYEAC-O0Kk8SC9Z7j9XlC3tMb7D6G0zXj4Q",
             api_base_url="https://mastodon.social")

threading.Thread(target=_stop_after_n_sec).start()


# Initialize custom stream listener
listener = MyStreamListener()

# Stream public toots with hashtag filter
m.stream_hashtag("Gaza", listener)



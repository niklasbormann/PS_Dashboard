import json
import threading
import time
import queue
from atproto import CAR, AtUri, FirehoseSubscribeReposClient, firehose_models, models, parse_subscribe_repos_message, \
    IdResolver

client = FirehoseSubscribeReposClient()
data = {
    'Text': [],
    'Lang': [],
    'Creator': [],
    'Timestamp': [],
    'Location': [],
    'Theme': []
}

data_lock = threading.Lock()  # Lock for synchronizing access to `data`


def _get_ops_by_type(commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> dict:
    operation_by_type = {
        'posts': {'created': [], 'deleted': []},
        'reposts': {'created': [], 'deleted': []},
        'likes': {'created': [], 'deleted': []},
        'follows': {'created': [], 'deleted': []},
    }

    resolver = IdResolver()
    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        uri = AtUri.from_str(f'at://{commit.repo}/{op.path}')

        if op.action == 'update':
            # not supported yet
            continue

        if op.action == 'create':
            if not op.cid:
                continue

            create_info = {
                'uri': str(uri),
                'cid': str(op.cid),
                'author': commit.repo,  # getattr(resolver.did.resolve(commit.repo), "also_known_as", []),
                'timestamp': commit.time,
            }

            record_raw_data = car.blocks.get(op.cid)
            if not record_raw_data:
                continue

            record = models.get_or_create(record_raw_data, strict=False)

            if uri.collection == models.ids.AppBskyFeedLike and models.is_record_type(
                    record, models.ids.AppBskyFeedLike
            ):
                operation_by_type['likes']['created'].append({'record': record, **create_info})
            elif uri.collection == models.ids.AppBskyFeedPost and models.is_record_type(
                    record, models.ids.AppBskyFeedPost
            ):
                operation_by_type['posts']['created'].append({'record': record, **create_info})
            elif uri.collection == models.ids.AppBskyFeedRepost and models.is_record_type(
                    record, models.ids.AppBskyFeedRepost
            ):
                operation_by_type['reposts']['created'].append({'record': record, **create_info})
            elif uri.collection == models.ids.AppBskyGraphFollow and models.is_record_type(
                    record, models.ids.AppBskyGraphFollow
            ):
                operation_by_type['follows']['created'].append({'record': record, **create_info})

        if op.action == 'delete':
            if uri.collection == models.ids.AppBskyFeedLike:
                operation_by_type['likes']['deleted'].append({'uri': str(uri)})
            elif uri.collection == models.ids.AppBskyFeedPost:
                operation_by_type['posts']['deleted'].append({'uri': str(uri)})
            elif uri.collection == models.ids.AppBskyFeedRepost:
                operation_by_type['reposts']['deleted'].append({'uri': str(uri)})
            elif uri.collection == models.ids.AppBskyGraphFollow:
                operation_by_type['follows']['deleted'].append({'uri': str(uri)})

    return operation_by_type


def worker_main(queue, Filters):
    while True:
        message = queue.get()
        if message is None:
            break  # Use `None` as a signal to stop the worker

        try:
            commit = parse_subscribe_repos_message(message)
            if commit is None or not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
                # print("Commit is None or not the expected type.")
                continue

            if commit.blocks is None:
                # print("Commit blocks are None.")
                continue

            ops = _get_ops_by_type(commit)
            for post in ops.get('posts', {}).get('created', []):  # Safely access nested dictionaries
                post_msg = post['record'].text
                post_langs = post['record'].langs
                post_author = post['author']
                post_timestamp = post['timestamp']
                if any(filter_str in post_msg for filter_str in Filters):
                    print(
                        f'Langs: {post_langs}. Text: {post_msg}. Creator: {post_author}. Time: {post_timestamp}')
                    with data_lock:  # Synchronize access to `data`
                        data["Text"].append(post_msg)
                        data["Lang"].append(post_langs)
                        data["Creator"].append(post_author)
                        data["Timestamp"].append(post_timestamp)
        except Exception as e:
            print(f"Error processing message: {e}")


def get_firehose_params() -> models.ComAtprotoSyncSubscribeRepos.Params:
    return models.ComAtprotoSyncSubscribeRepos.Params(cursor=0)


def on_message_handler(message: firehose_models.MessageFrame, queue):
    try:
        queue.put(message)
    except Exception as e:
        print(f"Error in message handler: {e}")


def stop_client_after_n_seconds(client, stop_timer, queue):
    time.sleep(stop_timer)
    client.stop()
    for _ in range(threading.active_count() - 1):
        queue.put(None)  # Signal workers to exit


def run_data_collection(StopTimer, Filters, filename):
    StopTimer = StopTimer

    client = FirehoseSubscribeReposClient(get_firehose_params())

    workers_count = 4  # Adjust based on expected workload
    max_queue_size = 500

    work_queue = queue.Queue(maxsize=max_queue_size)

    # Start worker threads
    for _ in range(workers_count):
        threading.Thread(target=worker_main, args=(work_queue, Filters), daemon=True).start()

    # Start the client in a separate thread to ensure it's non-blocking
    client_thread = threading.Thread(
        target=lambda: client.start(lambda message: on_message_handler(message, work_queue)), daemon=True)
    client_thread.start()

    # Timer to stop the client after a specified interval
    stop_timer_thread = threading.Thread(target=stop_client_after_n_seconds, args=(client, StopTimer, work_queue),
                                         daemon=True)
    stop_timer_thread.start()

    stop_timer_thread.join()  # Wait for timer to finish

    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)
    print(f'Successfully stopped after {StopTimer} seconds!')
    print(f'Data saved to data.json')


if __name__ == '__main__':
    Filter = ["Ukraine", "Russia"]
    run_data_collection(1800 , Filter, "Dashboardtestdata.json")

from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message, CAR, AtUri

client = FirehoseSubscribeReposClient()


def on_message_handler(message) -> None:
    commit = parse_subscribe_repos_message(message)
    car = CAR.from_bytes(commit.blocks)

    for op in commit.ops:
        uri = AtUri.from_str(f'at://{commit.repo}/{op.path}')
        if i == "create":
            create_info = {'uri': str(uri), 'cid': str(op.cid), 'author': commit.repo}
            record_raw_data = car.blocks.get(op.cid)
    print(message, parse_subscribe_repos_message(message))


client.start(on_message_handler)
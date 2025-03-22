import crypto_client

client = crypto_client.get_crypto_client()

client.start_stream("BTC/USD", buffer_hours=4)
client.graph_stream("BTC/USD")

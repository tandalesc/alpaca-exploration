import crypto_client


client = crypto_client.get_crypto_client()

client.start_stream("BTC/USD")
client.show_stream("BTC/USD")

client.stream.run()
# client.show_stream("BTC/USD")

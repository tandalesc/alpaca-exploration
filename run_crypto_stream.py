from lib.crypto_client import CryptoStreamClient


# convinience function to create client from env vars
def get_crypto_client():
    import os
    from dotenv import load_dotenv

    load_dotenv()
    api_key = os.getenv("ALPACA_API_KEY")
    api_secret = os.getenv("ALPACA_API_SECRET")
    return CryptoStreamClient(api_key, api_secret)


client = get_crypto_client()
client.start_stream("BTC/USD", buffer_hours=4)
client.graph_stream("BTC/USD")

import pandas as pd
import requests


def get_black_scholes(symbol, irate, access_token):
    url = f'https://api.oplab.com.br/v3/market/options/bs?symbol={symbol}&irate={irate}'
    response = requests.get(url, headers={'Access-Token': access_token})
    if response.status_code != 200:
        return pd.DataFrame()
    df = pd.DataFrame(response.json(), index=[0])
    return df


def get_cotacao_historica(symbol, hist_resolution, hist_from, hist_to, access_token):
    url = f'https://api.oplab.com.br/v3/market/historical/{symbol}/{hist_resolution}?from={hist_from}&to={hist_to}&df=iso'
    response = requests.get(url, headers={'Access-Token': access_token})
    if response.status_code != 200:
        return pd.DataFrame()
    df = pd.DataFrame(response.json()['data'])
    df['date'] = pd.to_datetime(df.time).dt.date
    df['ticker'] = symbol
    df = df[['ticker', 'date', 'open', 'low', 'high', 'close', 'volume']]
    df = df.round(decimals=2)
    return df


def get_lista_cotacao(tickers, access_token):
    tickers = ','.join(tickers)
    url = f'https://api.oplab.com.br/v3/market/quote?tickers={tickers}'
    response = requests.get(url, headers={'Access-Token': access_token})
    if response.status_code != 200:
        return pd.DataFrame()
    df = pd.DataFrame(response.json())
    df['time'] = pd.to_datetime(df.time / 1000, unit='s').dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
    return df


def get_lista_opcoes(symbols, access_token):
    symbols = ','.join(symbols)
    url = f'https://api.oplab.com.br/v3/market/options/strategies/covered?underlying={symbols}'
    response = requests.get(url, headers={'Access-Token': access_token})
    if response.status_code != 200:
        return pd.DataFrame()
    df = pd.DataFrame(response.json())
    df.due_date = pd.to_datetime(df.due_date)
    return df


def get_opcoes(symbol, access_token):
    url = f'https://api.oplab.com.br/v3/market/options/{symbol}'
    response = requests.get(url, headers={'Access-Token': access_token})
    if response.status_code != 200:
        return pd.DataFrame()
    df = pd.DataFrame(response.json())
    df['parent_symbol'] = symbol
    df.due_date = pd.to_datetime(df.due_date)
    # df['last_trade_at'] = pd.to_datetime(df.last_trade_at / 1000, unit='s').dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
    return df


def get_options_data(symbol, access_token):
    url = f'https://api.oplab.com.br/v3/market/options/details/{symbol}'
    response = requests.get(url, headers={'Access-Token': access_token})
    if response.status_code != 200:
        return pd.DataFrame()
    df = pd.DataFrame(response.json(), index=[0])
    return df


def get_pozinhos(access_token):
    url = f'https://api.oplab.com.br/v3/market/options/powders'
    response = requests.get(url, headers={'Access-Token': access_token})
    if response.status_code != 200:
        return pd.DataFrame()
    df = pd.DataFrame(response.json())
    return df


def get_ranking_acoes(limit=50, sort='desc', financial_volume_start=1, rank_by='financial_volume', access_token='access_token'):
    url = f'https://api.oplab.com.br/v3/market/stocks?rank_by={rank_by}&sort={sort}&limit={limit}&financial_volume_start={financial_volume_start}'
    response = requests.get(url, headers={'Access-Token': access_token})
    if response.status_code != 200:
        return pd.DataFrame()
    df = pd.DataFrame(response.json())
    df['time'] = pd.to_datetime(df.time / 1000, unit='s').dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
    return df


def get_stock_data(symbol, access_token):
    url = f'https://api.oplab.com.br/v3/market/stocks/{symbol}'
    response = requests.get(url, headers={'Access-Token': access_token})
    if response.status_code != 200:
        return pd.DataFrame()
    df = pd.DataFrame(response.json(), index=[0])
    return df


def get_taxa_juros(access_token):
    url = f'https://api.oplab.com.br/v3/market/interest_rates'
    response = requests.get(url, headers={'Access-Token': access_token})
    if response.status_code != 200:
        return 0
    df = pd.DataFrame(response.json())
    selic = round(float(df[df.uid == 'SELIC']['value'].values[0] / 100), 6)
    return selic


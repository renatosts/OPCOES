from my_metadata import MY_ACOES
import datetime
import oplab
import pandas as pd
import sqlite3


conn = sqlite3.connect('oplab_db.sqlite3')

def get_controle_data(conn):
    df = pd.read_sql('SELECT * FROM controle', con=conn)
    controle_data = df.data_base.iloc[0]
    return controle_data


def get_cotacao_historica(conn, data_base, LISTA_ACOES, TOKEN):
    # Obtém cotação histórica das ações
    hist_from = data_base - datetime.timedelta(days=365)
    hist_to = data_base
    df = pd.DataFrame()
    for symbol in LISTA_ACOES:
        df = pd.concat([df, oplab.get_cotacao_historica(symbol, '1d', hist_from, hist_to, TOKEN)], ignore_index=True)
    # Salva em banco de dados cotação histórica das ações
    df.to_sql('cotacao_historica_acao', con=conn, if_exists='replace', index=False)


def get_lista_acoes(conn, TOKEN):
    # Importa as 20 opções mais negociadas
    df = oplab.get_ranking_acoes(limit=20, access_token=TOKEN)
    # Complementa lista com ações de MY_ACOES
    my_acoes = pd.DataFrame(MY_ACOES, columns=['symbol'])
    my_acoes = pd.concat([df['symbol'], my_acoes]).drop_duplicates()
    my_acoes.columns = ['ticker']
    # Salva em banco de dados tabela de tickers por ordem de liquidez
    my_acoes.to_sql('tickers', con=conn, if_exists='replace', index=False)
    # Cria lista ordenada das ações
    LISTA_ACOES = sorted(my_acoes.ticker.tolist())
    return LISTA_ACOES

def get_opcoes(conn, data_base, LISTA_ACOES, TOKEN):
    data_base = pd.to_datetime(data_base)
    # Obtém lista das opções
    df = pd.DataFrame()
    for symbol in LISTA_ACOES:
        df = pd.concat([df, oplab.get_opcoes(symbol, TOKEN)], ignore_index=True)
    # Elimina opções vencidas
    df = df[df.due_date >= data_base]
    # Ajusta coluna maturidade Americana/Europeia
    df.loc[df.maturity_type == 'AMERICAN', 'maturity_type'] = 'A'
    df.loc[df.maturity_type == 'EUROPEAN', 'maturity_type'] = 'E'
    # Ajusta coluna Formador de Mercado
    df['fm'] = ' '
    df.loc[df.market_maker == True, 'fm'] = 'FM'
    # Elimina colunas desnecessárias
    df = df[['parent_symbol', 'symbol', 'due_date', 'strike', 'maturity_type',
            'fm', 'close', 'volume',
            'financial_volume', 'trades', 'bid', 'ask',
            'type', 'last_trade_at']]
    # Salva em banco de dados tabela das opções
    df.to_sql('oplab_opcoes', con=conn, if_exists='replace', index=False)

    # Cria lista de datas de vencimento
    vencimentos = pd.DataFrame({'datven': df.due_date.unique()}).sort_values(by='datven')
    vencimentos['datven'] = vencimentos['datven'].dt.date
    vencimentos.to_sql('vencimentos', con=conn, if_exists='replace', index=False)

    # Obtém cotação atual das ações
    cot_acao = oplab.get_lista_cotacao(LISTA_ACOES, TOKEN)
    # Salva em banco de dados tabela das opções
    cot_acao.to_sql('cotacao_atual_acao', con=conn, if_exists='replace', index=False)
    # Elimina colunas desnecessárias para merge
    cot_acao = cot_acao[['symbol', 'close']]
    cot_acao.columns = ['parent_symbol', 'cotacao']


    # Faz cálculos das opções
    df = df.merge(cot_acao, on='parent_symbol', how='left')

    # Redefine nome das colunas
    df.columns = ['ticker', 'codneg', 'datven', 'strike', 'estilo', 'fm',
        'premio', 'quatot', 'valtot', 'totneg',
        'bid', 'ask', 'tipo', 'dataneg', 'cotacao']

    df['dias'] = (df['datven'] - data_base).dt.days

    # Cria lista de liquidez
    # liquidez = df[df.dataneg == df.dataneg.max()].groupby('ticker')[['totneg', 'valtot']].sum().reset_index()
    liquidez = df.groupby('ticker')[['totneg', 'valtot']].sum().reset_index()
    liquidez = liquidez.sort_values(by=['valtot', 'ticker'], ascending=[False, True])
    liquidez.columns = ['ticker', 'liq_totneg', 'liq_valtot']
    liquidez.to_sql('liquidez', con=conn, if_exists='replace', index=False)


    df['dist_strike'] = round(df['strike'] /  df['cotacao'] - 1, 4)

    # Moneyness (ATM, ITM, OTM)
    df['aiotm'] = ''
    df.loc[(df.dist_strike == 0), 'aiotm'] = 'ATM'
    df.loc[(df.tipo == 'CALL') & (df.dist_strike < 0), 'aiotm'] = 'ITM'
    df.loc[(df.tipo == 'CALL') & (df.dist_strike > 0), 'aiotm'] = 'OTM'
    df.loc[(df.tipo == 'PUT') & (df.dist_strike > 0), 'aiotm'] = 'ITM'
    df.loc[(df.tipo == 'PUT') & (df.dist_strike < 0), 'aiotm'] = 'OTM'

    # Identifica opções ATM (mais próximas do strike)
    base = df[(df.dist_strike.abs() <= 0.02)] # somente diferença de até 2%

    # CALL
    idx = base[(base.tipo == 'CALL') & (base.dist_strike < 0)].groupby(['ticker', 'datven'])['dist_strike'].idxmax()
    df.loc[idx, 'aiotm'] = 'ATM'

    idx = base[(base.tipo == 'CALL') & (base.dist_strike > 0)].groupby(['ticker', 'datven'])['dist_strike'].idxmin()
    df.loc[idx, 'aiotm'] = 'ATM'

    # PUT
    idx = base[(base.tipo == 'PUT') & (base.dist_strike < 0)].groupby(['ticker', 'datven'])['dist_strike'].idxmax()
    df.loc[idx, 'aiotm'] = 'ATM'

    idx = base[(base.tipo == 'PUT') & (base.dist_strike > 0)].groupby(['ticker', 'datven'])['dist_strike'].idxmin()
    df.loc[idx, 'aiotm'] = 'ATM'

    # Taxa
    df['taxa'] = round(df['premio'] / df['cotacao'], 4)

    # Valor Intrínseco
    df['vi'] = 0.0
    df.loc[(df.aiotm == 'ITM') & (df.tipo == 'CALL'), 'vi'] = df['cotacao'] - df['strike']  # CALL
    df.loc[(df.aiotm == 'ITM') & (df.tipo == 'PUT'), 'vi'] = df['strike'] - df['cotacao'] # PUT

    # Valor Extrínseco
    df['ve'] = df['premio'] - df['vi']
    df['ve_perc'] = round(df['ve'] / df['cotacao'], 4)

    # Breakeven
    df['breakeven'] = 0.0
    df.loc[df.tipo == 'PUT', 'breakeven'] = df['strike'] - df['premio']
    df.loc[df.tipo == 'CALL', 'breakeven'] = df['strike'] + df['premio']

    # Classifica opções
    df = df.sort_values(by=['ticker', 'datven', 'strike', 'tipo'])

    # Salva em banco de dados tabela de opções
    df.to_sql('opcoes', con=conn, if_exists='replace', index=False)


def get_pozinhos(conn, LISTA_ACOES, TOKEN):
    # Pozinhos
    df = oplab.get_pozinhos(TOKEN)
    df = df[df['spot-symbol'].isin(LISTA_ACOES)]
    # Elimina colunas desnecessárias
    # df = df[[]]
    # Salva em banco de dados pozinhos
    df.to_sql('pozinhos', con=conn, if_exists='replace', index=False)


def importa_oplab(conn, hoje):
    '''
    # Importa as 20 ações com as opções mais negociadas
    df = oplab.get_ranking_acoes(limit=20, access_token=TOKEN)
    symbols = df.symbol.tolist()
    # Complementa lista com ações de MY_ACOES
    my_acoes = pd.DataFrame(MY_ACOES, columns=['symbol'])
    my_acoes = pd.concat([df['symbol'], my_acoes]).drop_duplicates()
    my_acoes.columns = ['ticker']
    # Salva em banco de dados tabela de tickers por ordem de liquidez
    my_acoes.to_sql('tickers', con=conn, if_exists='replace', index=False)
    # Cria lista ordenada das ações
    LISTA_ACOES = sorted(my_acoes.ticker.tolist())
    '''


    # Obtém lista das opções
    df = pd.DataFrame()
    for symbol in LISTA_ACOES:
        df = pd.concat([df, oplab.get_opcoes(symbol, TOKEN)], ignore_index=True)
    # Elimina opções vencidas
    df = df[df.due_date >= hoje]
    # Ajusta coluna maturidade Americana/Europeia
    df.loc[df.maturity_type == 'AMERICAN', 'maturity_type'] = 'A'
    df.loc[df.maturity_type == 'EUROPEAN', 'maturity_type'] = 'E'
    # Ajusta coluna Formador de Mercado
    df['fm'] = ' '
    df.loc[df.market_maker == True, 'fm'] = 'FM'
    # Elimina colunas desnecessárias
    df = df[['parent_symbol', 'symbol', 'due_date', 'strike', 'maturity_type',
            'fm', 'days_to_maturity', 'close', 'volume',
            'financial_volume', 'trades', 'bid', 'ask',
            'type', 'last_trade_at',
        ]
        ]
    # Salva em banco de dados tabela das opções
    df.to_sql('oplab_opcoes', con=conn, if_exists='replace', index=False)

    # Cria lista de datas de vencimento
    vencimentos = pd.DataFrame({'datven': df.due_date.unique()}).sort_values(by='datven')
    vencimentos.to_sql('vencimentos', con=conn, if_exists='replace', index=False)


    '''
    # Obtém lista das opções da lista de ações
    df = oplab.get_lista_opcoes(LISTA_ACOES, TOKEN)
    # Elimina opções vencidas
    df = df[df.due_date >= hoje]
    '''

    # Obtém cotação atual das ações
    cot_acao = oplab.get_lista_cotacao(LISTA_ACOES, TOKEN)
    # Salva em banco de dados tabela das opções
    cot_acao.to_sql('cotacao_atual_acao', con=conn, if_exists='replace', index=False)
    # Elimina colunas desnecessárias para merge
    cot_acao = cot_acao[['symbol', 'close']]
    cot_acao.columns = ['parent_symbol', 'cotacao']


    # Faz cálculos das opções
    df = df.merge(cot_acao, on='parent_symbol', how='left')

    # Redefine nome das colunas
    df.columns = ['ticker', 'codneg', 'datven', 'strike', 'estilo', 'fm',
        'dias', 'premio', 'quatot', 'valtot', 'totneg',
        'bid', 'ask', 'tipo', 'dataneg', 'cotacao']

    # Cria lista de liquidez
    # liquidez = df[df.dataneg == df.dataneg.max()].groupby('ticker')[['totneg', 'valtot']].sum().reset_index()
    liquidez = df.groupby('ticker')[['totneg', 'valtot']].sum().reset_index()
    liquidez = liquidez.sort_values(by=['valtot', 'ticker'], ascending=[False, True])
    liquidez.columns = ['ticker', 'liq_totneg', 'liq_valtot']
    liquidez.to_sql('liquidez', con=conn, if_exists='replace', index=False)


    df['dist_strike'] = round(df['strike'] /  df['cotacao'] - 1, 4)

    # Moneyness (ATM, ITM, OTM)
    df['aiotm'] = ''
    df.loc[(df.dist_strike == 0), 'aiotm'] = 'ATM'
    df.loc[(df.tipo == 'CALL') & (df.dist_strike < 0), 'aiotm'] = 'ITM'
    df.loc[(df.tipo == 'CALL') & (df.dist_strike > 0), 'aiotm'] = 'OTM'
    df.loc[(df.tipo == 'PUT') & (df.dist_strike > 0), 'aiotm'] = 'ITM'
    df.loc[(df.tipo == 'PUT') & (df.dist_strike < 0), 'aiotm'] = 'OTM'

    # Identifica opções ATM (mais próximas do strike)
    base = df[(df.dist_strike.abs() <= 0.01)] # somente diferença de até 1%

    # CALL
    idx = base[(base.tipo == 'CALL') & (base.dist_strike < 0)].groupby(['ticker', 'datven'])['dist_strike'].idxmax()
    df.loc[idx, 'aiotm'] = 'ATM'

    idx = base[(base.tipo == 'CALL') & (base.dist_strike > 0)].groupby(['ticker', 'datven'])['dist_strike'].idxmin()
    df.loc[idx, 'aiotm'] = 'ATM'

    # PUT
    idx = base[(base.tipo == 'PUT') & (base.dist_strike < 0)].groupby(['ticker', 'datven'])['dist_strike'].idxmax()
    df.loc[idx, 'aiotm'] = 'ATM'

    idx = base[(base.tipo == 'PUT') & (base.dist_strike > 0)].groupby(['ticker', 'datven'])['dist_strike'].idxmin()
    df.loc[idx, 'aiotm'] = 'ATM'

    # Taxa
    df['taxa'] = round(df['premio'] / df['cotacao'], 4)

    # Valor Intrínseco
    df['vi'] = 0.0
    df.loc[(df.aiotm == 'ITM') & (df.tipo == 'CALL'), 'vi'] = df['cotacao'] - df['strike']  # CALL
    df.loc[(df.aiotm == 'ITM') & (df.tipo == 'PUT'), 'vi'] = df['strike'] - df['cotacao'] # PUT

    # Valor Extrínseco
    df['ve'] = df['premio'] - df['vi']
    df['ve_perc'] = round(df['ve'] / df['cotacao'], 4)

    # Breakeven
    df['breakeven'] = 0.0
    df.loc[df.tipo == 'PUT', 'breakeven'] = df['strike'] - df['premio']
    df.loc[df.tipo == 'CALL', 'breakeven'] = df['strike'] + df['premio']

    # Salva em banco de dados tabela de opções
    df.to_sql('opcoes', con=conn, if_exists='replace', index=False)

    '''
    # Obtém Black & Scholes das opções
    df = pd.DataFrame()
    for symbol in LISTA_OPCOES:
        df = pd.concat([df, oplab.get_black_scholes(symbol, SELIC, TOKEN)], ignore_index=True)
    '''

    # Obtém cotação histórica das ações
    hist_from = hoje - datetime.timedelta(days=365)
    hist_to = hoje
    df = pd.DataFrame()
    for symbol in LISTA_ACOES:
        df = pd.concat([df, oplab.get_cotacao_historica(symbol, '1d', hist_from, hist_to, TOKEN)], ignore_index=True)
    # Salva em banco de dados cotação histórica das ações
    df.to_sql('cotacao_historica_acao', con=conn, if_exists='replace', index=False)

    '''
    # Obtém cotação histórica das opções

    SELIC = oplab.get_taxa_juros(TOKEN)

    hist_from = hoje - datetime.timedelta(days=365)
    hist_to = hoje
    df = pd.DataFrame()
    for symbol in LISTA_ACOES:
        df = pd.concat([df, oplab.get_cotacao_historica(symbol, '1d', hist_from, hist_to, TOKEN)], ignore_index=True)
    '''


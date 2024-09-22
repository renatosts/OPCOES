import re
from black_scholes import black_scholes_greeks, calcular_volatilidade
import datetime
import pandas as pd
import plotly.graph_objects as go
import sqlite3
import streamlit as st
import yfinance as yf
import zipfile

conn = sqlite3.connect('db.sqlite3')

MY_TICKERS = ['VALE3', 'PETR4', 'WEGE3', 'RADL3', 'BBAS3', 'VIVT3',
              'RENT3', 'JBSS3', 'AZZA3', 'SLCE3', 'CSNA3', 'EGIE3',
              'UNIP6', 'BBDC4', 'ITSA4', 'ABEV3', 'HYPE3', 'FLRY3',
              'ALUP11', 'LREN3', 'CSMG3', 'RDOR3', 'SAPR11', 'KLBN4',
              'TAEE11', 'CSAN3', 'MGLU3', 'ODPV3', 'FESA4', 'BBSE3',
              'GOLL4', 'CVCB3', 'AZUL4', 'BOVA11']

MY_TICKERS.sort()

def importa_base_b3():

    df = pd.DataFrame()

    colspecs = [(0, 2), (2, 10), (10, 12), (12, 24), (27, 39),
                (39, 42), (56, 69), (69, 82), (82, 95), (95, 108),
                (108, 121), (121, 134), (134, 147), (147, 152), (152, 170),
                (170, 188), (188, 201), (202, 210), (210, 217), (31, 32),
                (22, 31)]

    names = ['tipreg', 'dataneg', 'codbdi', 'codneg', 'nomres',
             'tpacao', 'preabe', 'premax', 'premin', 'premed',
             'premio', 'preofc', 'preofv', 'totneg', 'quatot',
             'valtot', 'strike', 'datven', 'factot', 'estilo',
             'data_base_b3']

    ano = datetime.date.today().year
    mes = datetime.date.today().month

    ano_inicial = ano - 1 if mes <= 2 else ano

    # Processa arquivos zipados (ano anterior e ano atual)
    for i in range(ano_inicial, ano + 1):
        try:
            with st.spinner(f'Processando {i}'):
                f = f'COTAHIST_A{i}.ZIP'
                with zipfile.ZipFile(f, 'r') as z:
                    with z.open(f'COTAHIST_A{i}.TXT') as arq:
                        temp = pd.read_fwf(arq, colspecs=colspecs, names=names)
                df = pd.concat([df, temp])
        except:
            pass

    # Obtém data da base B3
    data_base_b3 = df.loc[df.tipreg == 0, 'data_base_b3'].max()
    data_base_b3 = pd.to_datetime(data_base_b3, format='%Y%m%d').date()

    controle = pd.DataFrame({'data_base_b3': [data_base_b3]})
    controle.to_sql('controle', conn, if_exists='replace', index=False)

    df['origem'] = 'B3'

    # Apenas registro tipo 1
    df = df[df.tipreg == 1]

    # Deleta FNAM, FNOR
    df = df[df.factot == 1]

    # Retirar as colunas que não serão mais utilizadas
    df = df.drop(columns=['tipreg', 'factot'])

    df.dataneg = pd.to_datetime(df.dataneg, format='%Y%m%d')
    df.datven = pd.to_datetime(df.datven, format='%Y%m%d', errors='coerce')

    # Ajusta campos de valores
    campos = ['preabe', 'premax', 'premin', 'premed', 'premio', 'preofc', 'preofv', 'valtot', 'strike']
    for campo in campos:
        df[campo] = df[campo] / 100

    df['ativo'] = df['codneg'].str[:4]

    df['tipo'] = ''
    df.loc[df.codbdi == 2, 'tipo'] = 'AÇÃO'
    df.loc[df.codbdi == 14, 'tipo'] = 'AÇÃO'
    df.loc[df.codbdi == 78, 'tipo'] = 'CALL'
    df.loc[df.codbdi == 82, 'tipo'] = 'PUT'

    df = df[df.tipo.isin(['AÇÃO', 'CALL', 'PUT'])]

    # Cria base de ações para obter ticker
    acoes = df[(df.tipo == 'AÇÃO') & (df.dataneg == df.dataneg.max())][['ativo', 'tpacao', 'codneg']]
    acoes.rename(columns={'codneg': 'ticker'}, inplace=True)

    # Atribui ticker para as opções
    df = df.merge(acoes, on=['ativo', 'tpacao'], how='left')

    # Elimina tickers vazios
    df = df[~df.ticker.isna()]

    # Permanecem apenas opções
    df = df[df.tipo.isin(['CALL', 'PUT'])]

    # Elimina as opções vencidas
    df = df[df.datven >= datetime.date.today().strftime("%Y-%m-%d")]

    # Cria lista de tickers por liquidez
    liquidez = df[df.dataneg == df.dataneg.max()].groupby('ticker')[['totneg', 'valtot']].sum().reset_index()
    liquidez = liquidez.sort_values(by=['totneg', 'ticker'], ascending=[False, True])
    liquidez.columns = ['ticker', 'liq_totneg', 'liq_valtot']
    liquidez.to_sql('liquidez', con=conn, if_exists='replace', index=False)

    # Elimina tickers sem liquidez
    df = df[df.ticker.isin(liquidez.ticker)]

    # Cria lista de tickers
    tickers = pd.DataFrame({'ticker': df.ticker.unique()}).sort_values(by='ticker')
    tickers.to_sql('tickers', con=conn, if_exists='replace', index=False)

    # Cria lista de datas de vencimento
    vencimentos = pd.DataFrame({'datven': df.datven.unique()}).sort_values(by='datven')
    vencimentos.to_sql('vencimentos', con=conn, if_exists='replace', index=False)


    df['estilo'] = df['estilo'].fillna('A')
    df['nomres'] = df['nomres'].fillna('')

    df['fm'] = ' '
    df.loc[df.nomres.str.contains('FM'), 'fm'] = 'FM'

    # Salva Cotações das Opções
    cotacoes_opcoes = df[['tipo', 'codneg', 'dataneg', 'preabe', 'premax', 'premin', 'premio', 'totneg', 'quatot', 'valtot', 'datven', 'strike', 'origem']
       ].sort_values(by=['tipo', 'codneg', 'dataneg'])
    cotacoes_opcoes.to_sql('cotacoes_opcoes', con=conn, if_exists='replace', index=False)

    # Deixa dados da última negociação
    indices_max_dataneg = df.groupby('codneg')['dataneg'].idxmax()
    df = df.loc[indices_max_dataneg]

    df = df.sort_values(by=['ativo', 'tpacao', 'datven', 'strike'])

    # Retirar as colunas que não serão mais utilizadas
    df = df.drop(columns=['codbdi', 'data_base_b3'])

    # Salva Opções
    df.to_sql('opcoes', con=conn, if_exists='replace', index=False)

    return df


def define_color(val):

    if val < 0:
        color = 'red'
    elif val > 0:
        color = 'green'
    else:
        color = 'gray'
    return 'color: %s' % color


def define_color_aio(val):

    if val == 'ATM':
        color = 'cyan'
    elif val == 'ITM':
        color = 'darkseagreen'
    else:
        color = 'coral'
    return 'color: %s' % color


def define_color_tipo(val):

    if val == 'CALL':
        color = 'orangered'
    else:
        color = 'slateblue'
    return 'color: %s' % color


def define_color_cotacao(val):

    color = 'yellow'
    return 'color: %s' % color


def define_color_strike(val):

    color = 'orange'
    return 'color: %s' % color


def estrutura_trava():


    # Read bases de opções e cotações
    cotacoes_acoes = read_cotacoes_acoes()

    df = read_estrutura_trava()

    max_dataneg = df.dataneg.max()

    opt_datven = pd.to_datetime(df.datven).sort_values().dt.strftime('%d/%m/%Y').unique()

    todos_ativos = read_lista_tickers()

    col1, col2, col3, col4, col5, col6, col7 = st.columns([.8, 1.2, 1.2, .8, 1.5, .8, .5])

    with col1:
        todos_ticker = st.selectbox(
            label = 'Ativos',

            options = [''] + todos_ativos
        )
    with col2:
        excl_ticker = st.multiselect(
            label = 'Excluir Ativo',
            options = [''] + df.ticker.unique().tolist()
        )
    with col3:
        aiotm = st.multiselect(
            label = 'ITM/ATM/OTM',
            options = ['', 'ITM', 'ATM', 'OTM']
        )
    with col4:
        tipo = st.selectbox(
            label = 'Call/Put',
            options = ['', 'CALL', 'PUT']
        )
    with col5:
        datven = st.multiselect(
            label = 'Vencimento',
            options = [''] + opt_datven
        )
    with col6:
        direc = st.selectbox(
            label = 'Direção',
            options = ['', 'Alta', 'Baixa']
        )
    with col7:
        deb_cred = st.selectbox(
            label = 'D/C',
            options = ['', 'D', 'C']
        )


    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        dist_strike = st.checkbox(
            label = 'Strike 5 a 15%',
            value = False
        )
    with col2:
        taxas_decr = st.checkbox(
            label = 'Diferenças Decrescentes',
            value = False
        )
    with col3:
        perc_decr = st.checkbox(
            label = '% Prêmio/Strike Decrescentes',
            value = False
        )
    with col4:
        liq_decr = st.checkbox(
            label = 'Liquidez Decrescente',
            value = False
        )
    with col5:
        fm = st.checkbox(
            label = 'Formador de Mercado',
            value = False
        )


    # weeks = False
    weeks = True
    if not weeks:
        df = df[df.codneg1.str.endswith('W1') == False]
        df = df[df.codneg1.str.endswith('W2') == False]
        df = df[df.codneg1.str.endswith('W3') == False]
        df = df[df.codneg1.str.endswith('W4') == False]
        df = df[df.codneg1.str.endswith('W5') == False]
        df = df[df.codneg2.str.endswith('W1') == False]
        df = df[df.codneg2.str.endswith('W2') == False]
        df = df[df.codneg2.str.endswith('W3') == False]
        df = df[df.codneg2.str.endswith('W4') == False]
        df = df[df.codneg2.str.endswith('W5') == False]


    ticker = ''
    if todos_ticker != '':
        ticker = todos_ticker
        df = df[df.ticker == ticker]


    if tipo != '':
        df = df[df.tipo == tipo]

    if direc != '':
        df = df[df.direc == direc]

    if deb_cred != '':
        df = df[df.deb_cred == deb_cred]

    if len(excl_ticker) > 0:
        df = df[~df.ticker.isin(excl_ticker)]

    if len(datven) > 0:
        df = df[df.datven.isin(datven)]

    if len(aiotm) > 0:
        df = df[df.aiotm.isin(aiotm)]


    if taxas_decr:
        df = df.sort_values(by=['dif_premio', 'dif_strike'], ascending=[False, True])

    if perc_decr:
        df = df.sort_values(by=['perc_premio_strike', 'dif_premio'], ascending=[False, False])

    if liq_decr:
        df = df.sort_values(by=['liquidez', 'dif_premio'], ascending=[False, False])

    if dist_strike:
        df = df[(df.dist_strike.abs() >= .05) & (df.dist_strike.abs() <= 0.15)]
        df = df[(df.dist_strike_2.abs() >= .05) & (df.dist_strike_2.abs() <= 0.15)]

    if fm:
        df = df[df.fm == 'FM']
        df = df[df.fm_2 == 'FM']


    df_aux = df.copy()

    df_aux = df_aux[['direc', 'codneg1', 'codneg2', 'tipo', 'strike', 'strike_2',
                     'estilo', 'estilo_2', 'datven', 'dias',
                    'cotacao', 'dist_strike', 'dist_strike_2',
                    'totneg', 'totneg_2', 'valtot', 'valtot_2',
                    'preofc', 'preofv_2', 'premio', 'premio_2', 'dif_premio',
                    'dif_strike', 'perc_premio_strike', 'aiotm',
                    'deb_cred', 'dataneg', 'dataneg_2']].reset_index(drop=True)

    df_aux.columns = [
        'Direc', 'Ticker1', 'Ticker2', 'Tipo', 'Strike1', 'Strike2', 'Est1', 'Est2',
         'Vencto', 'Dias', 'Cotação', 'Distância1', 'Distância2', 'Qtd1', 'Qtd2',
         'Tot1', 'Tot2', 'Bid1', 'Ask2', 'Prêmio1', 'Prêmio2', 'Dif Pr', 'Dif Strk',
         'Pr / Strike', 'AIO', 'D/C', 'Negoc1', 'Negoc2']


    # Aplicar formatação
    styled_df = df_aux.style.format(
        thousands=".",
        decimal=",",
        formatter={
            'Vencto': '{:%d/%m}',
            'Negoc1': '{:%d/%m}',
            'Negoc2': '{:%d/%m}',
            'Cotação': '{:,.2f}',
            'Distância1': '{:,.2%}',
            'Distância2': '{:,.2%}',
            'Qtd1': '{:,.0f}',
            'Qtd2': '{:,.0f}',
            'Tot1': '{:,.0f}',
            'Tot2': '{:,.0f}',
            'Strike1': '{:,.2f}',
            'Strike2': '{:,.2f}',
            'Prêmio1': '{:,.2f}',
            'Prêmio2': '{:,.2f}',
            'Bid1': '{:.2f}',
            'Ask2': '{:.2f}',
            'Dif Pr': '{:.2f}',
            'Dif Strk': '{:.2f}',
            'Pr / Strike': '{:,.2%}'
        }
    )

    # Data específica a ser usada
    specific_date = max_dataneg

    # Aplicar o estilo
    styled_df = styled_df.map(
        lambda val: highlight_dates(val, specific_date),
        subset=['Negoc1', 'Negoc2']
    )

    # Aplicar cor para tipo CALL ou PUT
    styled_df = styled_df.map(define_color_tipo, subset=['Tipo'])

    # Aplicar cor para cotação
    styled_df = styled_df.map(define_color_cotacao, subset=['Cotação'])

    # Aplicar cor para diferença de prêmios
    styled_df = styled_df.map(define_color, subset=['Dif Pr'])

    # Aplicar cor para strike
    styled_df = styled_df.map(define_color_strike, subset=['Strike1', 'Strike2', 'Dif Strk'])



    if ticker:

        cot = cotacoes_acoes[cotacoes_acoes.Ticker == ticker]

        var = cot.iloc[-1]['Close'] / cot.iloc[-2]['Close'] - 1
        ws_color = define_color(var)

        st.write(f'''<b>{ticker} R$ {cot.iloc[-1]['Close']:,.2f} <span style="{ws_color}"> {var:,.2%}</span></b>''', unsafe_allow_html=True)

    if ticker != '' or len(datven) > 0:
        st.dataframe(styled_df, use_container_width=True, hide_index=True)

    if ticker:

        fig = grafico_cotacao_acoes(cotacoes_acoes, ticker, bollinger=True)
        st.plotly_chart(fig, use_container_width=True)



def exporta_excel(df):
    df.to_excel('opcoes.xlsx')


def gera_base_opcoes(lista_acoes=None):

    with st.spinner('Gerando Base Opções'):

        df = read_opcoes()
        if lista_acoes:
            df = df[df.ticker.isin(lista_acoes)]

        # Read cotações das ações
        cotacoes_acoes = read_cotacoes_acoes()

        # Obtém última cotação
        data_maxima = cotacoes_acoes.Date.max()
        ult_cot = cotacoes_acoes[cotacoes_acoes.Date == data_maxima][['Ticker', 'Close']]
        ult_cot.columns = ['ticker', 'cotacao']

        # Determina ATM, ITM, OTM

        # Traz cotação atual, eliminando do df as opções sem cotação do ativo (não usando how='left')
        df = df.merge(ult_cot, on='ticker')

        df['dias'] = (pd.to_datetime(df['datven']) - df['dataneg']).dt.days

        df['dist_strike'] = round(df['strike'] /  df['cotacao'] - 1, 4)

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


        df['taxa'] = round(df['premio'] / df['cotacao'], 4)

        df['vi'] = 0.0
        df.loc[(df.aiotm == 'ITM') & (df.tipo == 'CALL'), 'vi'] = df['cotacao'] - df['strike']  # CALL / ITM ou ATM
        df.loc[(df.aiotm == 'ITM') & (df.tipo == 'PUT'), 'vi'] = df['strike'] - df['cotacao'] # CALL / ITM ou ATM
        df['vi'] = round(df['vi'], 2)

        df['ve'] = df['premio'] - df['vi']

        df['breakeven'] = 0.0
        df.loc[df.tipo == 'PUT', 'breakeven'] = df['strike'] - df['premio']
        df.loc[df.tipo == 'CALL', 'breakeven'] = df['strike'] + df['premio']

        df['ve_perc'] = round(df['ve'] / df['cotacao'], 4)


    # Apuração Black & Scholes

    with st.spinner('Volatilidade Implícita'):

        # Taxa de juros (r) = 10,75%
        r = 0.1075

        # Calcular a volatilidade implícita para cada linha do DataFrame
        df['volatilidade_implicita'] = df.apply(calcular_volatilidade, axis=1, args=(r,))


    with st.spinner('Black & Scholes'):

        # Aplicar a função a cada linha do DataFrame
        greeks = df.apply(lambda row: black_scholes_greeks(row['cotacao'], row['strike'], row['dias'], r, row['volatilidade_implicita']), axis=1)

        # Transformar o resultado em um DataFrame e concatenar com o original
        greeks_df = pd.DataFrame(list(greeks))

        # Concatenar os resultados no DataFrame original
        df = pd.concat([df, greeks_df], axis=1)


        df['delta'] = 0.0
        df.loc[(df.tipo == 'CALL'), 'delta'] = df['Delta Call']
        df.loc[(df.tipo == 'PUT'), 'delta'] = df['Delta Put']

        df['gamma'] = df['Gamma']

        df['theta'] = 0.0
        df.loc[(df.tipo == 'CALL'), 'theta'] = df['Theta Call']
        df.loc[(df.tipo == 'PUT'), 'theta'] = df['Theta Put']

        df['vega'] = df['Vega']

        df['rho'] = 0.0
        df.loc[(df.tipo == 'CALL'), 'rho'] = df['Rho Call']
        df.loc[(df.tipo == 'PUT'), 'rho'] = df['Rho Put']

        for i in ['volatilidade_implicita', 'delta', 'gamma', 'theta', 'vega', 'rho']:
            df[i] = round(df[i], 4)

        # Retirar as colunas que não serão mais utilizadas
        df = df.drop(columns=['Delta Call', 'Delta Put', 'Gamma', 'Theta Call', 'Theta Put', 'Vega', 'Rho Call', 'Rho Put'])

    # Join para trazer dados da liquidez
    liquidez = read_liquidez()
    df = df.merge(liquidez, on='ticker', how='left')

    if lista_acoes:
        if len(lista_acoes) > 0: 
            # Read Opções Final e elimina opções que foram processadas
            opcoes_final = read_opcoes_final()
            opcoes_final = opcoes_final[~opcoes_final.ticker.isin(lista_acoes)]
            # Concatena as opções processadas
            df = pd.concat([df, opcoes_final])
            df = df.sort_values(by=['ticker', 'datven', 'strike'])

    df.dataneg = pd.to_datetime(df.dataneg).dt.date

    df.to_sql('opcoes_final', con=conn, if_exists='replace', index=False)


def gera_estrutura_trava():

    with st.spinner('Gerando Base Travas'):


        lista_tickers = read_lista_tickers()

        sql_where = f" WHERE ticker IN ({','.join([f"'{x}'" for x in lista_tickers])})"

        # Read Opções
        opcoes = read_opcoes_final(sql_where=sql_where)

        temp_opcoes = opcoes[['codneg', 'ticker']]

        df = pd.DataFrame()
        for ticker in temp_opcoes['ticker'].unique():
            call_temp = temp_opcoes[temp_opcoes.ticker == ticker][['codneg']]
            temp = call_temp.merge(call_temp, how='cross')
            df = pd.concat([df, temp])
        
        df.columns = ['codneg1', 'codneg2']

        df = df[df.codneg1 != df.codneg2]

        df = df.join(opcoes.set_index('codneg'), on='codneg1', rsuffix='_1', how='left')
        df = df.join(opcoes.set_index('codneg'), on='codneg2', rsuffix='_2', how='left')

        df = df[df.datven == df.datven_2]
        df = df[df.tipo == df.tipo_2]
        df = df[df.aiotm == df.aiotm_2]

        df['dif_strike'] = abs(df['strike'] - df['strike_2'])
        indices_min_dif_strike = df.groupby('codneg1')['dif_strike'].idxmin()
        df = df.loc[indices_min_dif_strike]


        # Trava Alta no Crédito (PUT)
        df.loc[((df.tipo == 'PUT') & (df.strike > df.strike_2)), 'direc'] = 'Alta'
        df.loc[((df.tipo == 'PUT') & (df.strike > df.strike_2)), 'deb_cred'] = 'C'

        # Trava Baixa no Crédito (CALL)
        df.loc[((df.tipo == 'CALL') & (df.strike < df.strike_2)), 'direc'] = 'Baixa'
        df.loc[((df.tipo == 'CALL') & (df.strike < df.strike_2)), 'deb_cred'] = 'C'

        # Trava Alta no Débito (CALL)
        df.loc[((df.tipo == 'CALL') & (df.strike > df.strike_2)), 'direc'] = 'Alta'
        df.loc[((df.tipo == 'CALL') & (df.strike > df.strike_2)), 'deb_cred'] = 'D'

        # Trava Baixa no Débito (PUT)
        df.loc[((df.tipo == 'PUT') & (df.strike < df.strike_2)), 'direc'] = 'Baixa'
        df.loc[((df.tipo == 'PUT') & (df.strike < df.strike_2)), 'deb_cred'] = 'D'

        df = df.reset_index(drop=True)

        df = df[['codneg1', 'codneg2', 'dataneg', 'tipo', 'premio', 'preofc',
                'preofv', 'totneg', 'quatot', 'valtot', 'strike', 'datven',
                'estilo', 'ativo', 'ticker', 'fm', 'cotacao', 'dias',
                'dist_strike', 'taxa', 'dataneg_2', 'estilo_2', 'premio_2', 'preofc_2',
                'preofv_2', 'totneg_2', 'quatot_2', 'valtot_2', 'strike_2',
                'fm_2', 'dist_strike_2', 'taxa_2', 'aiotm', 'direc', 'deb_cred',
                'dif_strike']]

        df['dif_premio'] = df['premio'] - df['premio_2']
        df['perc_premio_strike'] = abs(df['dif_premio'] / df['dif_strike'])

        df['liquidez'] = df['totneg'] + df['totneg_2']

        df = df.sort_values(by=['ticker', 'datven', 'strike', 'strike_2'])

        df.to_sql('estrutura_trava', con=conn, if_exists='replace', index=False)


def grafico_cotacao_acoes(df, ticker, bollinger=True):
    '''
    Gera gráfico de cotações
    '''
    cot = df[df.Ticker == ticker].copy()

    # Bandas Bollinger
    WINDOW = 20
    cot['sma'] = cot['Close'].rolling(WINDOW).mean()
    cot['std'] = cot['Close'].rolling(WINDOW).std(ddof = 0)
    cot['bol_upp'] = cot['sma'] + (cot['std'] * 2)
    cot['bol_low'] = cot['sma'] - (cot['std'] * 2)
    
    # RSI
    n = 14
    cot['rsi'] = cot['Close'].diff(1).mask(cot['Close'].diff(1) < 0, 0).ewm(alpha=1/n, adjust=False).mean().div(df['Close'].diff(1).mask(cot['Close'].diff(1) > 0, -0.0).abs().ewm(alpha=1/n, adjust=False).mean()).add(1).rdiv(100).rsub(100)

    # Candlestick

    fig = go.Figure()

    fig.add_trace(
        go.Candlestick(
            x=cot.Date,
            open=cot.Open,
            high=cot.High,
            low=cot.Low,
            close=cot.Close,
            name='Candlestick'))
    fig.add_trace(
        go.Scatter(
            x=cot.Date,
            y=cot['rsi'],
            mode='markers+lines',
            name='RSI',
            yaxis='y2'))

    # Bandas de Bollinger

    if bollinger:

        fig.add_trace(
            go.Scatter(
                x=cot.Date,
                y=cot.sma,
                line_color='orange',
                name='Moving Average'))

        fig.add_trace(
            go.Scatter(
                x=cot.Date,
                y=cot.bol_upp,
                line_color='gray',
                line={'dash': 'dash'},
                name='Upper Band'))
        
        fig.add_trace(
            go.Scatter(
                x=cot.Date,
                y=cot.bol_low,
                line_color='gray',
                line={'dash': 'dash'},
                name='Lower Band'))
    
    fig.update_layout(
        xaxis_rangeslider_visible=False,
        showlegend=False,
        height=600,
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1,
            xanchor='right',
            x=1),
        yaxis_domain=[0.4, 1],
        yaxis2={"domain": [0, 0.3]})

    return fig    


def input_cotacao_excel(conn, data_base_b3):
    '''
    Importa arquivo Excel com cotação das opções
    Elimina cotações de datas anteriores a data_base_b3
    '''
    f = 'CotacaoAtualOpcao.csv'
    df = pd.read_csv(f, encoding='latin-1', sep=';', decimal=',')
    df.opcao_data = pd.to_datetime(df.opcao_data, dayfirst=True)
    df = df[df.opcao_data >= data_base_b3]
    df.to_sql('cotacao_excel', con=conn, if_exists='replace', index=False)
    return df


def update_cotacao_acao_excel(conn, excel):
    '''
    Atualiza cotação das ações importadas do Excel
    '''
    cursor = conn.cursor()
    df = excel[excel.opcao_tipo == 'Acao']
    # Deleta todas cotações anteriores do Excel
    sql = '''
        DELETE FROM cotacoes_acoes
        WHERE
            Origem = 'Excel'
    '''
    cursor.execute(sql)
    # Atualiza cotações Excel no banco de dados
    sql = '''
        UPDATE cotacoes_acoes
        SET
            Close = ?,
            Origem = 'Excel'
        WHERE
            Ticker = ?
            AND Date = ?
    '''
    for index, row in df.iterrows():
        t = [row['opcao_cotacao'],
             row['opcao_ticker'],
             row['opcao_data'].strftime('%Y-%m-%d')]
        cursor.execute(sql, t)


def update_cotacao_opcao_excel(conn, excel):
    '''
    Atualiza cotação (prêmio) das opções importadas do Excel
    '''
    df = excel[excel.opcao_tipo == 'Opcao']
    cursor = conn.cursor()
    # Deleta todas cotações anteriores do Excel
    sql = '''
        DELETE FROM cotacoes_opcoes
        WHERE
            origem = 'Excel'
    '''
    cursor.execute(sql)
    # # Obtém dados das opções a serem atualizadas
    # lista_ticker = df.opcao_ticker.unique().tolist()
    # lista_ticker = str(tuple(lista_ticker)).replace(',)', ')')
    # sql = f'''
    #     SELECT *
    #     FROM opcoes
    #     WHERE
    #         codneg IN {lista_ticker}
    # '''
    opcoes = read_opcoes()
    opcoes = opcoes[opcoes.codneg.isin(df.opcao_ticker)]
    # Junta cotação Excel
    df = df.merge(opcoes, left_on='opcao_ticker', right_on='codneg')
    # Insere cotações Excel no banco de dados
    sql = '''
        INSERT INTO cotacoes_opcoes
            (tipo, codneg, dataneg, preabe, premax, premin, premio, totneg, quatot, valtot, datven, strike, origem)
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    for index, row in df.iterrows():
        t = [row['tipo'],
             row['codneg'],
             row['opcao_data'].strftime('%Y-%m-%d %H:%M:%S'),
             row['opcao_cotacao'],
             row['opcao_cotacao'],
             row['opcao_cotacao'],
             row['opcao_cotacao'],
             1,
             1,
             1,
             row['datven'].strftime('%Y-%m-%d %H:%M:%S'),
             row['strike'],
             'Excel']
        cursor.execute(sql, t)

    # df = excel[excel.opcao_tipo == 'Acao']
    # # Atualiza base opção final com cotações Excel
    # sql = '''
    #     UPDATE opcoes
    #     SET
    #         premio = ?,
    #         dataneg = ?,
    #         origem = ?
    #     WHERE
    #         codneg = ?
    # '''
    # for index, row in df.iterrows():
    #     t = [row['opcao_cotacao'],
    #          row['opcao_data'].strftime('%Y-%m-%d %H:%M:%S'),
    #          'Excel',
    #          row['opcao_ticker']]
    #     cursor.execute(sql, t)
    # Atualiza base opção com cotações Excel
    sql = '''
        UPDATE opcoes
        SET
            premio = ?,
            dataneg = ?,
            origem = ?
        WHERE
            codneg = ?
    '''
    for index, row in df.iterrows():
        t = [row['opcao_cotacao'],
             row['opcao_data'].strftime('%Y-%m-%d %H:%M:%S'),
             'Excel',
             row['opcao_ticker']]
        cursor.execute(sql, t)
    conn.commit()


def highlight_dates(val, specific_date):
    '''Função para colorir células da coluna 'Data de Negociação' se diferente de uma data específica
    '''
    if pd.isna(val):  # Se o valor é NaN, não aplica a cor
        return ''
    w_color = ''
    if val.date() == specific_date.date():
        w_color = 'color: purple'
    elif val.date() > specific_date.date():
        w_color = 'color: yellow'
    return w_color


def main():

    # Read bases de opções
    df = read_opcoes_final()

    max_dataneg = df[df.origem == 'B3'].dataneg.max()

    opt_datven = read_lista_vencimentos()

    todos_ativos = read_lista_tickers()

    col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 2.5, 1])

    with col1:
        my_ticker = st.selectbox(
            label = 'Meus Ativos',
            options = [''] + MY_TICKERS
        )

    with col2:
        tipo = st.selectbox(
            label = 'Call/Put',
            options = ['', 'CALL', 'PUT']
        )

    with col3:
        aiotm = st.multiselect(
            label = 'ITM/ATM/OTM',
            options = ['', 'ITM', 'ATM', 'OTM']
        )

    with col4:
        datven = st.multiselect(
            label = 'Vencimento',
            options = [''] + opt_datven
        )

    with col5:
        dataneg = st.date_input(
            label = 'Pregão (a partir)',
            value = datetime.date.today()
        )

    col_1, col_2, col_3, col_4, col_5 = st.columns([1, 2, 1, 1, 1])

    with col_1:
        todos_ticker = st.selectbox(
            label = 'Todos Ativos',
            options = ['', 'All'] + todos_ativos
        )
    with col_2:
        excl_ticker = st.multiselect(
            label = 'Excluir Ativo',
            options = [''] + todos_ativos
        )
    with col_3:
        taxas_decr = st.checkbox(
            label = 'Taxas Decrescentes',
            value = False
        )
        dist_strike = st.checkbox(
            label = 'Strike superior a 5%',
            value = False
        )
    with col_4:
        ve_decr = st.checkbox(
            label = 'VE Descrescente',
            value = False
        )
        liq_ativo_decr = st.checkbox(
            label = 'Liquidez Ativo',
            value = False
        )
    with col_5:
        fm = st.checkbox(
            label = 'Formador de Mercado',
            value = False
        )


    # weeks = False
    weeks = True
    if not weeks:
        df = df[df.codneg.str.endswith('W1') == False]
        df = df[df.codneg.str.endswith('W2') == False]
        df = df[df.codneg.str.endswith('W3') == False]
        df = df[df.codneg.str.endswith('W4') == False]
        df = df[df.codneg.str.endswith('W5') == False]

    ticker = ''
    if my_ticker != '':
        ticker = my_ticker
        df = df[df.ticker == ticker]
    elif not todos_ticker in ('', 'All'):
        ticker = todos_ticker
        df = df[df.ticker == ticker]
    elif todos_ticker != 'All':
        df = df[df.ticker.isin(MY_TICKERS)]

    if tipo != '':
        df = df[df.tipo == tipo]

    if len(aiotm) > 0:
        df = df[df.aiotm.isin(aiotm)]

    if len(excl_ticker) > 0:
        df = df[~df.ticker.isin(excl_ticker)]

    if len(datven) > 0:
        datven = [datetime.datetime.strptime(x, '%d/%m/%Y').date().strftime('%Y-%m-%d') for x in datven]
        df = df[df.datven.dt.strftime('%Y-%m-%d').isin(datven)]

    if dataneg:
        if dataneg >= datetime.date.today():
            dataneg = max_dataneg
        df = df[df.dataneg >= pd.to_datetime(dataneg)]

    if taxas_decr:
        df = df.sort_values(by=['taxa', 'datven', 'strike'], ascending=[False, True, False])

    if dist_strike:
        df = df[(df.dist_strike.abs() >= .05)]

    if fm:
        df = df[df.fm == 'FM']

    if ve_decr:
        if tipo == 'PUT':
            # df = df.sort_values(by='ve_perc', ascending=True)
            df = df.sort_values(by='ve_perc', ascending=False)
        else:
            df = df.sort_values(by='ve_perc', ascending=False)

    if liq_ativo_decr:
        df = df.sort_values(by=['liq_totneg', 'totneg', 'dias', 'tipo', 'strike'], ascending=[False, False, True, True, True])

    df_aux = df.copy()

    df_aux = df_aux[['tipo', 'fm', 'estilo', 'codneg', 'datven', 'dias',
                    'strike', 'cotacao', 'dist_strike', 'aiotm', 'liq_totneg',
                    'dataneg', 'delta', 'volatilidade_implicita', 'preofc', 'preofv', 'totneg', 'valtot',
                    'vi', 've', 'breakeven', 've_perc',
                    'premio', 'taxa']].reset_index(drop=True)
    
    # Define lista de códigos das opções exibidas
    lista_codneg = df_aux.codneg.tolist()

    # Ajusta nome das colunas
    df_aux.columns = [
        'Tipo', 'FM', 'Est', 'Ticker', 'Vencto', 'Dias',
        'Strike', 'Cot', 'Distância', 'AIO', 'Neg Ação',
        'Preg', 'Delta', 'Volat', 'Bid', 'Ask', 'Neg', 'Total',
        'VI', 'VE', 'BkEv', 'VE %', 'Prêmio', 'Taxa'
    ]

    # Definir o índice para 'Código Negociável'
    df_aux = df_aux.set_index('Ticker')

    # Aplicar formatação
    styled_df = df_aux.style.format(
        thousands=".",
        decimal=",",
        formatter={
            'Vencto': '{:%d/%m/%y}',
            'Preg': '{:%d/%m}',
            'Neg Ação': '{:,.0f}',
            'Bid': '{:,.2f}',
            'Ask': '{:,.2f}',
            'Cot': '{:,.2f}',
            'Neg': '{:,.0f}',
            'Total': '{:,.0f}',
            'Strike': '{:,.2f}',
            'Prêmio': '{:,.2f}',
            'BkEv': '{:,.2f}',
            'VE %': '{:,.2%}',
            'VI': '{:,.2f}',
            'VE': '{:,.2f}',
            'Delta': '{:.2f}',
            'Volat': '{:.2%}',
            'Taxa': '{:.2%}',
            'Distância': '{:.2%}'
        }
    )

    # Data específica a ser usada
    specific_date = max_dataneg

    # Aplicar o estilo
    styled_df = styled_df.map(
        lambda val: highlight_dates(val, specific_date),
        subset=['Preg']
    )           

    # Aplicar cor para Distância Breakeven
    styled_df = styled_df.map(define_color, subset=['Distância', 'VE %'])

    # Aplicar cor para tipo CALL ou PUT
    styled_df = styled_df.map(define_color_tipo, subset=['Tipo'])

    # Aplicar cor para ATM
    styled_df = styled_df.map(define_color_aio, subset=['AIO'])

    # Aplicar cor para cotação
    styled_df = styled_df.map(define_color_cotacao, subset=['Cot'])


    if ticker:

        sql_where = f' WHERE Ticker = \'{ticker}\''
        cot_acoes = read_cotacoes_acoes(sql_where)

        # cot = cotacoes_acoes[cotacoes_acoes.Ticker == ticker].copy()
        # Bandas Bollinger
        WINDOW = 20
        cot_acoes['sma'] = cot_acoes['Close'].rolling(WINDOW).mean()
        cot_acoes['std'] = cot_acoes['Close'].rolling(WINDOW).std(ddof = 0)
        cot_acoes['bol_upp'] = cot_acoes['sma'] + (cot_acoes['std'] * 2)
        cot_acoes['bol_low'] = cot_acoes['sma'] - (cot_acoes['std'] * 2)

        var = cot_acoes.iloc[-1]['Close'] / cot_acoes.iloc[-2]['Close'] - 1
        ws_color = define_color(var)

        st.write(f'''<b>{ticker} R$ {cot_acoes.iloc[-1]['Close']:,.2f} <span style="{ws_color}"> {var:,.2%}</span></b>''', unsafe_allow_html=True)

    # Exibe dataframe
    if ticker != '' or tipo != '' or len(datven) > 0 or len(aiotm) > 0:
        st.dataframe(styled_df, use_container_width=True)


    col1, col2 = st.columns(2)
    with col1:
        # Adiciona botão para exportar para Excel
        st.button('Exporta Excel', on_click=exporta_excel, args=(styled_df,), type='secondary')
        if ticker:
            with col2:
                graf_opcoes = st.checkbox(
                    label = 'Gráfico Opções',
                    value = False
                )

    # Gráficos
    if ticker:

        # Gráfico de cotações ações
        fig = grafico_cotacao_acoes(cot_acoes, ticker, bollinger=True)
        st.plotly_chart(fig, use_container_width=True)

        if graf_opcoes:

            # Obtém cotações das opções exibidas
            sql_where = f" WHERE codneg IN ({','.join([f"'{x}'" for x in lista_codneg])})"
            cotacoes_opcoes = read_cotacoes_opcoes(sql_where)


            for i in lista_codneg:
                df_cot = cotacoes_opcoes[cotacoes_opcoes.codneg == i]

                try:
                    cot_atu = df_cot.iloc[-1]['premio']
                    cot_ant = df_cot.iloc[-2]['premio']
                    var = cot_atu / cot_ant - 1
                except:
                    var = 0
            
                ws_color = define_color(var)

                fig = go.Figure(data=[
                go.Scatter(x=df_cot.dataneg, y=df_cot.premio, marker=dict(color="darkgoldenrod"))])
            
                fig = go.Figure(data=[
                    go.Candlestick(x=df_cot.dataneg,
                                open=df_cot.preabe,
                                high=df_cot.premax,
                                low=df_cot.premin,
                                close=df_cot.premio)])

                fig.update_layout(xaxis_rangeslider_visible=False,
                                showlegend=False,
                                #hovermode='x unified',
                                legend=dict(orientation='h',
                                            yanchor='bottom',
                                            y=1,
                                            xanchor='right',
                                            x=1))

                fig.update_layout(title=f'<b>{i}   (Ref: {df_cot.dataneg.iloc[-1]:%d/%m/%Y})    R$ {df_cot.premio.iloc[-1]:,.2f} <span style="{ws_color}"> {var:,.2%}</span>  (Strike: {df_cot.strike.iloc[-1]:,.2f}  Vencimento: {df_cot.datven.iloc[-1]:%d/%m/%Y})</b>')

                st.plotly_chart(fig, use_container_width=True)


def obtem_data_base_b3():
    df =  pd.read_sql('SELECT * FROM controle', con=conn)
    df.data_base_b3 = pd.to_datetime(df.data_base_b3)
    data_base_b3 = df.data_base_b3.iloc[0]
    return data_base_b3


def read_cotacoes_acoes(sql_where=''):
    df = pd.read_sql(f'SELECT * FROM cotacoes_acoes {sql_where}', con=conn)
    df.Date = pd.to_datetime(df.Date)
    return df


def read_cotacoes_opcoes(sql_where=''):
    df = pd.read_sql(f'SELECT * FROM cotacoes_opcoes {sql_where}', con=conn)
    df.dataneg = pd.to_datetime(df.dataneg)
    df.datven = pd.to_datetime(df.datven)
    return df

def read_estrutura_trava():
    df = pd.read_sql('SELECT * FROM estrutura_trava', con=conn)
    df.dataneg = pd.to_datetime(df.dataneg)
    df.dataneg_2 = pd.to_datetime(df.dataneg_2)
    df.datven = pd.to_datetime(df.datven)
    return df


def read_liquidez():
    df = pd.read_sql('SELECT * FROM liquidez', con=conn)
    return df


def read_opcoes():
    df =  pd.read_sql('SELECT * FROM opcoes', con=conn)
    df.dataneg = pd.to_datetime(df.dataneg)
    df.datven = pd.to_datetime(df.datven)
    return df


def read_opcoes_final(sql_where=''):
    df = pd.read_sql(f'SELECT * FROM opcoes_final {sql_where}', con=conn)
    df.dataneg = pd.to_datetime(df.dataneg)
    df.datven = pd.to_datetime(df.datven)
    return df


def read_lista_tickers():
    df = pd.read_sql('SELECT * FROM liquidez', con=conn)
    return df.ticker.tolist()


def read_lista_vencimentos():
    df = pd.read_sql('SELECT * FROM vencimentos', con=conn)
    df.datven = pd.to_datetime(df.datven)
    return df.datven.dt.strftime('%d/%m/%Y').tolist()


def input_cotacoes_yfinance():
        lista_ticker = read_lista_tickers()
        with st.spinner('Cotações Yahoo Finance'):
            # Ajusta lista de tickers
            lista_ticker_yahoo = [x + '.SA' for x in lista_ticker]
            # Acessa YFinance e salva em banco de dados
            df = yf.download(tickers=lista_ticker_yahoo, period='1y', interval='1d')
            df = df.round(decimals=2)
            df = df.stack(level=1, future_stack=True).rename_axis(['Date', 'Ticker']).reset_index(level=1)
            df = df.reset_index()

            df['Ticker'] = df['Ticker'].str.replace('.SA', '')
            df['Origem'] = 'YFinance'
            df = df[['Ticker', 'Date', 'Open', 'Low', 'High', 'Close', 'Origem']]

            df = df.sort_values(by=['Ticker', 'Date'])

            df.Date = pd.to_datetime(df.Date).dt.date

            df.to_sql('cotacoes_acoes', con=conn, if_exists='replace', index=False)


st.set_page_config(
    layout='wide',
    initial_sidebar_state='collapsed',
    page_icon='app.ico',
    page_title='Opções')


with st.sidebar:

    menu = st.selectbox(
    label = 'OPÇÕES',
    options = [
        'Main',
        'Travas'])

    data_base_b3 = obtem_data_base_b3()
    st.write(f'Base B3: {data_base_b3.strftime('%d/%m/%Y')}')

    if st.button('Importa Base B3'):
        opcoes = importa_base_b3()
        gera_base_opcoes()
        gera_estrutura_trava()

    if st.button('Calcula Estrutura Trava'):
        gera_estrutura_trava()

    if st.button('Cotações YFinance'):
        input_cotacoes_yfinance()
        gera_base_opcoes()

    if st.button('Cotações Excel'):
        # Importa cotações Excel
        excel = input_cotacao_excel(conn, data_base_b3)
        # Define lista de opções a serem atualizadas
        lista_acoes = excel[excel.opcao_tipo == 'Acao'].opcao_ticker.unique().tolist()
        # # Atualiza cotações Yahoo Finance
        # lista_acoes = excel[excel.opcao_tipo == 'Acao'].opcao_ticker.unique().tolist()
        input_cotacoes_yfinance()
        # Atualiza cotações das ações no banco de dados
        update_cotacao_acao_excel(conn, excel)
        # Atualiza cotações das opções no banco de dados
        update_cotacao_opcao_excel(conn, excel)
        gera_base_opcoes(lista_acoes)
        # gera_estrutura_trava()


if menu == 'Main':
    main()

if menu == 'Travas':
    estrutura_trava()

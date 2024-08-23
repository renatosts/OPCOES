import datetime
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf
import zipfile


MY_TICKERS = ['VALE3', 'PETR4', 'WEGE3', 'RADL3', 'BBAS3', 'VIVT3',
              'RENT3', 'JBSS3', 'AZZA3', 'SLCE3', 'CSNA3', 'EGIE3',
              'UNIP6', 'BBDC4', 'ITSA4', 'ABEV3', 'HYPE3', 'FLRY3',
              'ALUP11', 'LREN3', 'CSMG3', 'RDOR3', 'SAPR11', 'KLBN4',
              'CIEL3', 'TAEE11', 'CSAN3', 'MGLU3', 'ODPV3', 'FESA4',
              'BBSE3', 'GOLL4', 'CVCB3', 'BHIA3', 'BOVA11']

MY_TICKERS.sort()

def processa_base_b3():

    df = pd.DataFrame()

    colspecs = [(0, 2), (2, 10), (10, 12), (12, 24), (27, 39),
                (39, 42), (56, 69), (69, 82), (82, 95), (95, 108),
                (108, 121), (121, 134), (134, 147), (147, 152), (152, 170),
                (170, 188), (188, 201), (202, 210), (210, 217), (31, 32)]

    names = ['tipreg', 'dataneg', 'codbdi', 'codneg', 'nomres',
             'tpacao', 'preabe', 'premax', 'premin', 'premed',
             'preult', 'preofc', 'preofv', 'totneg', 'quatot',
             'valtot', 'strike', 'datven', 'factot', 'estilo']

    ano = datetime.date.today().year

    # Processa arquivos zipados (ano anterior e ano atual)
    for i in range(ano - 1, ano + 1):
        try:
            with st.spinner(f'Processando {i}'):
                f = f'COTAHIST_A{i}.ZIP'
                with zipfile.ZipFile(f, 'r') as z:
                    with z.open(f'COTAHIST_A{i}.TXT') as arq:
                        temp = pd.read_fwf(arq, colspecs=colspecs, names=names)
                df = pd.concat([df, temp])
        except:
            pass

    # Apenas registro tipo 1
    df = df[df.tipreg == 1]

    # Deleta FNAM, FNOR
    df = df[df.factot == 1]

    # Retirar as colunas que não serão mais utilizadas
    df = df.drop(columns=['tipreg', 'factot'])

    df.dataneg = pd.to_datetime(df.dataneg, format='%Y%m%d')
    df.datven = pd.to_datetime(df.datven, format='%Y%m%d', errors='coerce')

    # Ajusta campos de valores
    campos = ['preabe', 'premax', 'premin', 'premed', 'preult', 'preofc', 'preofv', 'valtot', 'strike']
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

    # Atribui ticker para as opções e Prêmioação
    df = df.merge(acoes, on=['ativo', 'tpacao'], how='left')

    # Elimina tickers vazios
    df = df[~df.ticker.isna()]

    # Permanecem apenas opções
    df = df[df.tipo.isin(['CALL', 'PUT'])]

    # Elimina as opções vencidas
    df = df[df.datven >= datetime.date.today().strftime("%Y-%m-%d")]

    df['estilo'] = df['estilo'].fillna('A')
    df['nomres'] = df['nomres'].fillna('')

    df['fm'] = ' '
    df.loc[df.nomres.str.contains('FM'), 'fm'] = 'FM'

    # Salva Cotações das Opções
    cotacoes_opcoes = df[['tipo', 'codneg', 'dataneg', 'preabe', 'premax', 'premin', 'preult', 'totneg', 'quatot', 'valtot', 'datven', 'strike']
       ].sort_values(by=['tipo', 'codneg', 'dataneg'])
    cotacoes_opcoes.to_parquet('cotacoes_opcoes.parquet', index=False)

    # Deixa dados da última negociação
    indices_max_dataneg = df.groupby('codneg')['dataneg'].idxmax()
    df = df.loc[indices_max_dataneg]

    df = df.sort_values(by=['ativo', 'tpacao', 'datven', 'strike'])

    # Salva Opções
    df.to_parquet('opcoes.parquet', index=False)

    return df


def define_color(val):

    if val < 0:
        color = 'red'
    elif val > 0:
        color = 'green'
    else:
        color = 'gray'
    return 'color: %s' % color


def gera_base_opcoes():

    df = read_opcoes()

    # Read cotações das ações
    cotacoes_acoes = read_cotacoes_acoes()

    # Obtém Prêmioação
    data_maxima = cotacoes_acoes.index.max()
    ult_cot = cotacoes_acoes[cotacoes_acoes.index == data_maxima]['Close'].T
    ult_cot['ticker'] = ult_cot.index.str.replace('.SA', '')
    ult_cot.columns = ['ult_cotacao', 'ticker']

    # Determina ATM, ITM, OTM

    df = opcoes.merge(ult_cot, on='ticker', how='left')

    df['dias'] = (df['datven'] - pd.to_datetime(datetime.date.today())).dt.days

    df['dist_strike'] = df['strike'] /  df['ult_cotacao'] - 1

    df['aiotm'] = ''
    df.loc[(df.tipo == 'CALL') & (df.dist_strike < 0), 'aiotm'] = 'ITM'
    df.loc[(df.tipo == 'CALL') & (df.dist_strike > 0), 'aiotm'] = 'OTM'
    df.loc[(df.tipo == 'PUT') & (df.dist_strike > 0), 'aiotm'] = 'ITM'
    df.loc[(df.tipo == 'PUT') & (df.dist_strike < 0), 'aiotm'] = 'OTM'
    df.loc[(df.dist_strike >= -0.02) & (df.dist_strike <= 0.02), 'aiotm'] = 'ATM'

    df['taxa'] = df['preult'] /  df['ult_cotacao']

    df['vlr_intrinseco'] = abs(df['strike'] -  df['ult_cotacao'])

    df.to_parquet('opcoes_final.parquet', index=False)


# Função para colorir células da coluna 'Data de Negociação' se diferente de uma data específica
def highlight_dates(val, specific_date):
    if pd.isna(val):  # Se o valor é NaN, não aplica a cor
        return ''
    return 'color: green' if val.date() == specific_date.date() else ''


@st.cache_data
def read_cotacoes_acoes():
    return pd.read_parquet('cotacoes_acoes.parquet')


@st.cache_data
def read_cotacoes_opcoes():
    return pd.read_parquet('cotacoes_opcoes.parquet')


@st.cache_data
def read_opcoes():
    return pd.read_parquet('opcoes.parquet')


@st.cache_data
def read_opcoes_final():
    return pd.read_parquet('opcoes_final.parquet')


def update_cotacoes_acoes(opcoes):
    with st.spinner('Cotações Yahoo Finance'):
        lista_ticker = opcoes.ticker.unique().tolist()
        lista_ticker = [x + '.SA' for x in lista_ticker]
        cot = yf.download(tickers=lista_ticker, period='100d', interval='1d')
        cot.to_parquet('cotacoes_acoes.parquet')


st.set_page_config(
    layout='wide',
    initial_sidebar_state='collapsed',
    page_icon='app.ico',
    page_title='Opções')


with st.sidebar:

    if st.button('Processa Base B3'):
        opcoes = processa_base_b3()
        update_cotacoes_acoes(opcoes)
        gera_base_opcoes()

    if st.button('Atualiza Cotações'):
        opcoes = read_opcoes()
        update_cotacoes_acoes(opcoes)
        gera_base_opcoes()


# Read bases de opções e cotações
cotacoes_opcoes = read_cotacoes_opcoes()
cotacoes_acoes = read_cotacoes_acoes()
df = read_opcoes_final()

opt_dataneg = df.dataneg.sort_values(ascending=False).dt.strftime('%d/%m/%Y').unique()
opt_datven = df.datven.sort_values().dt.strftime('%d/%m/%Y').unique()


col1, col2, col3, col4, col5= st.columns([1, 1, 2, 2.5, 1])

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
        label = 'Negociação (a partir)',
        value = datetime.date.today())

col1, col2, col3, col4 = st.columns([1, 3.5, 1.5, 1.5])

todos_ativos = df.ticker.unique().tolist()

with col1:
    todos_ticker = st.selectbox(
        label = 'Todos Ativos',

        options = ['', 'All'] + todos_ativos
    )
with col2:
    excl_ticker = st.multiselect(
        label = 'Excluir Ativo',
        options = [''] + df.ticker.unique().tolist()
    )
with col3:
    taxas_decr = st.checkbox(
        label = 'Taxas Decrescentes',
        value = False
    )
    dist_strike = st.checkbox(
        label = 'Strike 5 a 15%',
        value = False
    )
with col4:
    fm = st.checkbox(
        label = 'Formador de Mercado',
        value = False
    )
    graf_opcoes = st.checkbox(
        label = 'Gráfico Opções',
        value = False
    )


weeks = False
if not weeks:
    df = df[df.codneg.str.endswith('W1') == False]
    df = df[df.codneg.str.endswith('W2') == False]
    df = df[df.codneg.str.endswith('W3') == False]
    df = df[df.codneg.str.endswith('W4') == False]


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
    df = df[df.datven.isin(datven)]

max_dataneg = df.dataneg.max()
if dataneg:
    if dataneg >= datetime.date.today():
        dataneg = max_dataneg - datetime.timedelta(days=5)
    df = df[df.dataneg >= pd.to_datetime(dataneg)]

if taxas_decr:
    df = df.sort_values(by='taxa', ascending=False)

if dist_strike:
    df = df[(df.dist_strike.abs() >= .05) & (df.dist_strike.abs() <= 0.15)]

if fm:
    df = df[df.fm == 'FM']


df_aux = df.copy()

ult_cot = df_aux.ult_cotacao.max()

df_aux = df_aux[['tipo', 'fm', 'estilo', 'codneg', 'datven', 'dias',
                 'strike', 'ult_cotacao', 'dist_strike', 'aiotm',
                 'dataneg', 'preofc', 'preofv', 'totneg', 'valtot',
                 'vlr_intrinseco', 'preult', 'taxa']].reset_index(drop=True)

lista_codneg = df_aux.codneg.tolist()


df_aux.columns = [
    'Tipo', 'FM', 'Estilo', 'Ticker', 'Vencimento', 'Dias',
    'Strike', 'Cotação', 'Distância', 'AIO',
    'Negociação', 'Bid', 'Ask', 'Negócios', 'Total',
    'Vlr. Intr', 'Prêmio', 'Taxa'
]

# Definir o índice para 'Código Negociável'
df_aux = df_aux.set_index('Ticker')

# Aplicar formatação
styled_df = df_aux.style.format(
    thousands=".",
    decimal=",",
    formatter={
        'Vencimento': '{:%d/%m/%Y}',
        'Negociação': '{:%d/%m/%Y}',
        'Bid': '{:,.2f}',
        'Ask': '{:,.2f}',
        'Cotação': '{:,.2f}',
        'Negócios': '{:,.0f}',
        'Total': '{:,.0f}',
        'Strike': '{:,.2f}',
        'Prêmio': '{:,.2f}',
        'Vlr. Intr': '{:,.2f}',
        'Taxa': '{:.2%}',
        'Distância': '{:.2%}'
    }
)

# Data específica a ser usada
specific_date = max_dataneg

# Aplicar o estilo
styled_df = styled_df.applymap(
    lambda val: highlight_dates(val, specific_date),
    subset=['Negociação']
)

# df_aux = df_aux.set_index('codneg')


# df_aux = df_aux.style.format(
#     thousands=".",
#     decimal = ",",
#     formatter={
#         'datven': '{:%d/%m/%Y}',
#         'dataneg': '{:%d/%m/%Y}',
#         'preabe': '{:,.2f}',
#         'premax': '{:,.2f}',
#         'premin': '{:,.2f}',
#         'preult': '{:,.2f}',
#         'preofc': '{:,.2f}',
#         'preofv': '{:,.2f}',
#         'totneg': '{:,.0f}',
#         'valtot': '{:,.0f}',
#         'strike': '{:,.2f}',
#         'ult_cotacao': '{:,.2f}',
#         'taxa': '{:.2%}',
#         'dist_strike': '{:.2%}'})

# label=list(df_aux.columns.values) 
# df_aux.relabel_index(['Tipo', 'FM', 'estilo', 'datven', 'dias',
#                 'strike', 'ult_cotacao', 'dist_strike', 'aiotm',
#                 'dataneg', 'preofc', 'preofv', 'totneg', 'valtot',
#                 'preult', 'taxa'])

df_cot = cotacoes_opcoes[cotacoes_opcoes.codneg == ticker].tail(200)

if ticker:

    cot_open = cotacoes_acoes[('Open', f'{ticker}.SA')]
    cot_close = cotacoes_acoes[('Adj Close', f'{ticker}.SA')]
    cot_high = cotacoes_acoes[('High', f'{ticker}.SA')]
    cot_low = cotacoes_acoes[('Low', f'{ticker}.SA')]

    cot = cotacoes_acoes[('Adj Close', f'{ticker}.SA')]
    var = cot.iloc[-1] / cot.iloc[-2] - 1
    ws_color = define_color(var)

    st.write(f'''<b>{ticker} R$ {cot.iloc[-1]:,.2f} <span style="{ws_color}"> {var:,.2%}</span></b>''', unsafe_allow_html=True)

if ticker != '' or len(datven) > 0:
    st.dataframe(styled_df, use_container_width=True)

if ticker:

    fig = go.Figure(data=[
        go.Candlestick(x=cot.index,
                      open=cot_open,
                      high=cot_high,
                      low=cot_low,
                      close=cot_close)])

    fig.update_layout(
        xaxis_rangeslider_visible=False,
        showlegend=False,
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1,
            xanchor='right',
            x=1))
    
    st.plotly_chart(fig, use_container_width=True)

    if graf_opcoes:
        for i in lista_codneg:
            df_cot = cotacoes_opcoes[cotacoes_opcoes.codneg == i]

            try:
                cot_atu = df_cot.iloc[-1]['preult']
                cot_ant = df_cot.iloc[-2]['preult']
                var = cot_atu / cot_ant - 1
            except:
                var = 0
        
            ws_color = define_color(var)

            fig = go.Figure(data=[
            go.Scatter(x=df_cot.dataneg, y=df_cot.preult, marker=dict(color="darkgoldenrod"))])
        
            fig = go.Figure(data=[
                go.Candlestick(x=df_cot.dataneg,
                            open=df_cot.preabe,
                            high=df_cot.premax,
                            low=df_cot.premin,
                            close=df_cot.preult)])

            fig.update_layout(xaxis_rangeslider_visible=False,
                            showlegend=False,
                            #hovermode='x unified',
                            legend=dict(orientation='h',
                                        yanchor='bottom',
                                        y=1,
                                        xanchor='right',
                                        x=1))

            fig.update_layout(title=f'<b>{i}   (Ref: {df_cot.dataneg.iloc[-1]:%d/%m/%Y})    R$ {df_cot.preult.iloc[-1]:,.2f} <span style="{ws_color}"> {var:,.2%}</span>  (Strike: {df_cot.strike.iloc[-1]:,.2f}  Vencimento: {df_cot.datven.iloc[-1]:%d/%m/%Y})</b>')

            st.plotly_chart(fig, use_container_width=True)


from datetime import date
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf
import zipfile


MY_TICKERS = ['VALE3', 'PETR4', 'WEGE3', 'RADL3', 'BBAS3', 'VIVT3',
              'RENT3', 'JBSS3', 'ARZZ3', 'SLCE3', 'CSNA3', 'EGIE3',
              'UNIP3', 'BBDC4', 'ITSA4', 'ABEV3', 'HYPE3', 'FLRY3',
              'ALUP11', 'LREN3', 'CSMG3', 'RDOR3', 'SAPR11', 'KLBN4',
              'CIEL3', 'TAEE11', 'CSAN3', 'MGLU3', 'ODPV3', 'FESA4',
              'BBSE3', 'GOLL4', 'CVCB3', 'BHIA3', 'BOVA11']

MY_TICKERS.sort()

def gera_base():

    # Cria opções

    df = pd.DataFrame()

    colspecs = [(0, 2), (2, 10), (10, 12), (12, 24), (27, 39),
                (39, 42), (56, 69), (69, 82), (82, 95), (95, 108),
                (108, 121), (121, 134), (134, 147), (147, 152), (152, 170),
                (170, 188), (188, 201), (202, 210), (210, 217), (31, 32)]

    names = ['tipreg', 'dataneg', 'codbdi', 'codneg', 'nomres',
             'tpacao', 'preabe', 'premax', 'premin', 'premed',
             'preult', 'preofc', 'preofv', 'totneg', 'quatot',
             'valtot', 'strike', 'datven', 'factot', 'estilo']

    ano = date.today().year

    # Processa arquivo zipado
    for i in range(ano - 1, ano + 1):
        try:
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

    # Cria base de ações com somente dados da última negociação e obtém ticker
    acoes = df[(df.tipo == 'AÇÃO') & (df.dataneg == df.dataneg.max())][['ativo', 'tpacao', 'codneg', 'preult']]
    acoes.rename(columns={'preult': 'ult_cotacao', 'codneg': 'ticker'}, inplace=True)

    # Atribui ticker para as opções e última cotação
    df = df.merge(acoes, on=['ativo', 'tpacao'], how='left')

    # Elimina tickers vazios
    df = df[~df.ticker.isna()]

    # df = df[df.ticker.isin(MY_TICKERS)]

    # Salva Cotações
    cotacoes = df[['tipo', 'codneg', 'dataneg', 'preabe', 'premax', 'premin', 'preult', 'totneg', 'quatot', 'valtot', 'strike', 'datven']
       ].sort_values(by=['tipo', 'codneg', 'dataneg'])
    cotacoes.to_parquet('cotacoes.parquet', index=False)


    # Tratamento das opções
    df = df[df.tipo.isin(['CALL', 'PUT'])]

    # Elimina as opções vencidas
    df = df[df.datven >= date.today().strftime("%Y-%m-%d")]


    df['estilo'] = df['estilo'].fillna('A')
    df['nomres'] = df['nomres'].fillna('')

    df['fm'] = ' '
    df.loc[df.nomres.str.contains('FM'), 'fm'] = 'FM'


    # Deixa dados da última negociação
    indices_max_dataneg = df.groupby('codneg')['dataneg'].idxmax()
    df = df.loc[indices_max_dataneg]

    df = df.sort_values(by=['ativo', 'tpacao', 'datven', 'codneg', 'dataneg'])


    df['dist_strike'] = df['strike'] /  df['ult_cotacao'] - 1

    df['aiotm'] = ''
    df.loc[(df.tipo == 'CALL') & (df.dist_strike < 0), 'aiotm'] = 'ITM'
    df.loc[(df.tipo == 'CALL') & (df.dist_strike > 0), 'aiotm'] = 'OTM'
    df.loc[(df.tipo == 'PUT') & (df.dist_strike > 0), 'aiotm'] = 'ITM'
    df.loc[(df.tipo == 'PUT') & (df.dist_strike < 0), 'aiotm'] = 'OTM'
    df.loc[(df.dist_strike >= -0.02) & (df.dist_strike <= 0.02), 'aiotm'] = 'ATM'

    df['taxa'] = df['preult'] /  df['ult_cotacao']

    df['dias'] = (df['datven'] - pd.to_datetime(date.today())).dt.days

    # Salva Opções
    df.to_parquet('opcoes.parquet', index=False)


def define_color(val):
    if val < 0:
        color = 'red'
    elif val > 0:
        color = 'green'
    else:
        color = 'gray'
    return 'color: %s' % color


# gera_base()

st.set_page_config(
    layout='wide',
    initial_sidebar_state='collapsed',
    page_icon='app.jpg',
    page_title='Opções')


@st.cache_data
def read_cotacoes():
    return pd.read_parquet('cotacoes.parquet')

@st.cache_data
def read_opcoes():
    return pd.read_parquet('opcoes.parquet')

df = read_opcoes()

cotacoes =read_cotacoes()


opt_dataneg = df.dataneg.sort_values(ascending=False).dt.strftime('%d/%m/%Y').unique()
opt_datven = df.datven.sort_values().dt.strftime('%d/%m/%Y').unique()


# Gera Base Opções  dataneg_hoje = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
# 
with st.sidebar:
    

    if st.button('Gera Base Opções'):
        gera_base()


col1, col2, col3, col4, col5, col6 = st.columns([1, 1, 1, 2.5, 1, 2])

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
    aiotm = st.selectbox(
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
        value = date.today())

with col6:
    taxas_decr = st.checkbox(
        label = 'Taxas Decrescentes',
        value = False
    )
    dist_strike = st.checkbox(
        label = 'Strike 5 a 15%',
        value = False
    )
    # weeks = st.checkbox(
    #     label = 'Inclui Semanais',
    #     value = False
    # )

col1, col2, col3 = st.columns([1, 7.5, 2])

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

if aiotm != '':
    df = df[df.aiotm == aiotm]

if len(excl_ticker) > 0:
    df = df[~df.ticker.isin(excl_ticker)]

if len(datven) > 0:
    df = df[df.datven.isin(datven)]

if dataneg:
    max_dataneg = df.dataneg.max()
    if pd.to_datetime(dataneg) > max_dataneg:
        dataneg = max_dataneg
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
                 'preult', 'taxa']].reset_index(drop=True)

lista_codneg = df_aux.codneg.tolist()

df_aux = df_aux.set_index('codneg')


df_aux = df_aux.style.format(
    thousands=".",
    decimal = ",",
    formatter={
        'datven': '{:%d/%m/%Y}',
        'dataneg': '{:%d/%m/%Y}',
        'preabe': '{:,.2f}',
        'premax': '{:,.2f}',
        'premin': '{:,.2f}',
        'preult': '{:,.2f}',
        'preofc': '{:,.2f}',
        'preofv': '{:,.2f}',
        'totneg': '{:,.0f}',
        'valtot': '{:,.0f}',
        'strike': '{:,.2f}',
        'ult_cotacao': '{:,.2f}',
        'taxa': '{:.2%}',
        'dist_strike': '{:.2%}'})
    

df_cot = cotacoes[cotacoes.codneg == ticker].tail(200)

if ticker:
    
    cot_yf = yf.download(f'{ticker}.SA', period='100d')
    var = cot_yf.iloc[-1]['Adj Close'] / cot_yf.iloc[-2]['Adj Close'] - 1
    ws_color = define_color(var)

    st.write(f'''<b>{ticker} R$ {cot_yf.iloc[-1]['Adj Close']:,.2f} <span style="{ws_color}"> {var:,.2%}</span></b>''', unsafe_allow_html=True)

if ticker != '' or len(datven) > 0:
    st.dataframe(df_aux, use_container_width=True)

if ticker:

    fig = go.Figure(data=[
        go.Candlestick(x=cot_yf.index,
                      open=cot_yf['Open'],
                      high=cot_yf['High'],
                      low=cot_yf['Low'],
                      close=cot_yf['Adj Close'])])

    fig.update_layout(xaxis_rangeslider_visible=False,
                          showlegend=False,
                          legend=dict(orientation='h',
                                      yanchor='bottom',
                                      y=1,
                                      xanchor='right',
                                      x=1))
    st.plotly_chart(fig, use_container_width=True)

    if graf_opcoes:
        for i in lista_codneg:
            df_cot = cotacoes[cotacoes.codneg == i]
            # st.table(df_cot)

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


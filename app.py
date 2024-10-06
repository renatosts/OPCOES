from my_metadata import MY_ACOES, TOKEN
from oplab import get_ranking_acoes
import datetime
import oplab_to_database
import pandas as pd
import plotly.graph_objects as go
import sqlite3
import streamlit as st


conn = sqlite3.connect('oplab_db.sqlite3')


def atualiza_cotacoes(conn, hoje, TOKEN):
    LISTA_ACOES = oplab_to_database.get_lista_acoes(conn, TOKEN)
    oplab_to_database.get_opcoes(conn, hoje, LISTA_ACOES, TOKEN)

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


def define_color_estilo(val):

    if val == 'A':
        color = 'red'
    else:
        color = 'white'
    return 'color: %s' % color


def define_color_negocios(val):

    if val > 200:
        color = 'green'
    elif val > 100:
        color = 'yellow'
    elif val > 10:
        color = 'orange'
    elif val > 1:
        color = 'white'
    else:
        color = 'grey'
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
        datven = [datetime.datetime.strptime(x, '%d/%m/%Y').date().strftime('%Y-%m-%d') for x in datven]
        df = df[df.datven.dt.strftime('%Y-%m-%d').isin(datven)]

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
                    'valtot', 'valtot_2',
                    'bid', 'ask_2', 'premio', 'premio_2', 'dif_premio',
                    'dif_strike', 'perc_premio_strike', 'aiotm',
                    'deb_cred', 'dataneg', 'dataneg_2']].reset_index(drop=True)

    df_aux.columns = [
        'Direc', 'Ticker1', 'Ticker2', 'Tipo', 'Strike1', 'Strike2', 'Est1', 'Est2',
         'Vencto', 'Dias', 'Cotação', 'Distância1', 'Distância2',
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

        cot = cotacoes_acoes[cotacoes_acoes.ticker == ticker]

        var = cot.iloc[-1]['close'] / cot.iloc[-2]['close'] - 1
        ws_color = define_color(var)

        st.write(f'''<b>{ticker} R$ {cot.iloc[-1]['close']:,.2f} <span style="{ws_color}"> {var:,.2%}</span></b>''', unsafe_allow_html=True)

    if ticker != '' or len(datven) > 0:
        st.dataframe(styled_df, use_container_width=True, hide_index=True)

    if ticker:

        fig = grafico_cotacao_acoes(cotacoes_acoes, ticker, bollinger=True)
        st.plotly_chart(fig, use_container_width=True)


def exporta_excel(df):
    df.to_excel('opcoes.xlsx')


def gera_estrutura_trava():

    with st.spinner('Gerando Base Travas'):

        lista_tickers = read_lista_tickers()

        sql_where = f" WHERE ticker IN ({','.join([f"'{x}'" for x in lista_tickers])})"

        # Read Opções
        opcoes = read_opcoes(sql_where=sql_where)

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

        df = df[['codneg1', 'codneg2', 'dataneg', 'tipo', 'premio', 'bid',
                'ask', 'totneg', 'quatot', 'valtot', 'strike', 'datven',
                'estilo', 'ticker', 'fm', 'cotacao', 'dias',
                'dist_strike', 'taxa', 'dataneg_2', 'estilo_2', 'premio_2', 'bid_2',
                'ask_2', 'totneg_2', 'quatot_2', 'valtot_2', 'strike_2',
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
    cot = df[df.ticker == ticker].copy()

    # Bandas Bollinger
    WINDOW = 20
    cot['sma'] = cot['close'].rolling(WINDOW).mean()
    cot['std'] = cot['close'].rolling(WINDOW).std(ddof = 0)
    cot['bol_upp'] = cot['sma'] + (cot['std'] * 2)
    cot['bol_low'] = cot['sma'] - (cot['std'] * 2)
    
    # RSI
    n = 14
    cot['rsi'] = cot['close'].diff(1).mask(cot['close'].diff(1) < 0, 0).ewm(alpha=1/n, adjust=False).mean().div(df['close'].diff(1).mask(cot['close'].diff(1) > 0, -0.0).abs().ewm(alpha=1/n, adjust=False).mean()).add(1).rdiv(100).rsub(100)

    # Candlestick

    fig = go.Figure()

    fig.add_trace(
        go.Candlestick(
            x=cot.date,
            open=cot.open,
            high=cot.high,
            low=cot.low,
            close=cot.close,
            name='Candlestick'))

    fig.add_trace(
        go.Scatter(
            x=cot.date,
            y=cot['rsi'],
            mode='markers+lines',
            name='RSI',
            yaxis='y2'))

    # Bandas de Bollinger

    if bollinger:

        fig.add_trace(
            go.Scatter(
                x=cot.date,
                y=cot.sma,
                line_color='orange',
                name='Moving Average'))

        fig.add_trace(
            go.Scatter(
                x=cot.date,
                y=cot.bol_upp,
                line_color='gray',
                line={'dash': 'dash'},
                name='Upper Band'))
        
        fig.add_trace(
            go.Scatter(
                x=cot.date,
                y=cot.bol_low,
                line_color='gray',
                line={'dash': 'dash'},
                name='Lower Band'))
    
    fig.update_layout(
        xaxis_rangeslider_visible=False,
        showlegend=True,
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


def highlight_dates(val, specific_date):
    '''Função para colorir células da coluna 'Data de Negociação' se diferente de uma data específica
    '''
    if pd.isna(val):  # Se o valor é NaN, não aplica a cor
        return ''
    w_color = ''
    if val.date() == specific_date.date():
        w_color = 'color: orange'
    elif val.date() > specific_date.date():
        w_color = 'color: yellow'
    return w_color


def quadro_opcoes():

    # Read bases de opções
    df = read_opcoes()

    max_dataneg = data_maxima()
    # max_dataneg = df[df.origem == 'B3'].dataneg.max()

    opt_datven = read_lista_vencimentos()

    todos_ativos = read_lista_tickers()

    col1, col2, col3, col4 = st.columns([1, 1, 2, 2.5])

    with col1:
        ticker = st.selectbox(
            label = 'Ativos',
            options = [''] + todos_ativos
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

    col_1, col_2, col_3, col_4, col_5 = st.columns([1.5, 1, 1, 1, 1])

    with col_1:
        excl_ticker = st.multiselect(
            label = 'Excluir Ativo',
            options = [''] + todos_ativos
        )
    with col_2:
        taxas_decr = st.checkbox(
            label = 'Taxas Decrescentes',
            value = False
        )
        dist_strike = st.checkbox(
            label = 'Strike superior a 5%',
            value = False
        )
    with col_3:
        ve_decr = st.checkbox(
            label = 'VE Descrescente',
            value = False
        )
        liq_ativo_decr = st.checkbox(
            label = 'Liquidez Descrescente',
            value = False
        )
    with col_4:
        fm = st.checkbox(
            label = 'Formador de Mercado',
            value = False
        )
        sem_liquidez = st.checkbox(
            label = 'Exibir sem Liquidez',
            value = False
        )
    with col_5:
        bid_ask = st.checkbox(
            label = 'Bid e Ask',
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

    if ticker != '':
        df = df[df.ticker == ticker]

    if tipo != '':
        df = df[df.tipo == tipo]

    if len(aiotm) > 0:
        df = df[df.aiotm.isin(aiotm)]

    if len(excl_ticker) > 0:
        df = df[~df.ticker.isin(excl_ticker)]

    if len(datven) > 0:
        datven = [datetime.datetime.strptime(x, '%d/%m/%Y').date().strftime('%Y-%m-%d') for x in datven]
        df = df[df.datven.dt.strftime('%Y-%m-%d').isin(datven)]

    # if dataneg:
    #     if dataneg >= datetime.date.today():
    #         dataneg = max_dataneg
    #     df = df[df.dataneg >= pd.to_datetime(dataneg)]
    
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
        df = df.sort_values(by=['valtot', 'dias', 'tipo', 'strike'], ascending=[False, True, True, True])

    if not sem_liquidez:
        df = df[df.valtot >= 1]

    if bid_ask:
        df = df[(df.bid > 0) & (df.ask > 0)]

    df_aux = df.copy()

    # df_aux = df_aux[['tipo', 'fm', 'estilo', 'codneg', 'datven', 'dias',
    #                 'strike', 'cotacao', 'dist_strike', 'aiotm', 'liq_totneg',
    #                 'dataneg', 'delta', 'volatilidade_implicita', 'bs',
    #                  'preofc', 'preofv', 'totneg', 'valtot',
    #                 'vi', 've', 'breakeven', 've_perc',
    #                 'premio', 'taxa']].reset_index(drop=True)
    
    df_aux = df_aux[['tipo', 'fm', 'estilo', 'codneg', 'datven', 'dias',
                    'strike', 'cotacao', 'dist_strike', 'aiotm', 
                    'bid', 'ask', 'valtot',
                    'vi', 've', 'breakeven', 've_perc',
                    'premio', 'taxa']].reset_index(drop=True)
    
    # Define lista de códigos das opções exibidas
    lista_codneg = df_aux.codneg.tolist()

    # Ajusta nome das colunas
    df_aux.columns = [
        'Tipo', 'FM', 'Est', 'Ticker', 'Vencto', 'Dias',
        'Strike', 'Cot', 'Distância', 'AIO', 
        'Bid', 'Ask', 'Liquidez $',
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
            'Dias': '{:,.0f}',
            'Liquidez $': '{:,.0f}',
            'B&S': '{:,.2f}',
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

    # # Aplicar cor para data do pregão
    # styled_df = styled_df.map(
    #     lambda val: highlight_dates(val, specific_date),
    #     subset=['Preg']
    # )           

    # Aplicar cor para Distância Breakeven
    styled_df = styled_df.map(define_color, subset=['Distância', 'VE %'])

    # Aplicar cor para tipo CALL ou PUT
    styled_df = styled_df.map(define_color_tipo, subset=['Tipo'])

    # Aplicar cor para ATM
    styled_df = styled_df.map(define_color_aio, subset=['AIO'])

    # # Aplicar cor para número de negócios
    # styled_df = styled_df.map(define_color_negocios, subset=['Neg'])

    # Aplicar cor para número de estilo
    styled_df = styled_df.map(define_color_estilo, subset=['Est'])

    # Aplicar cor para cotação
    styled_df = styled_df.map(define_color_cotacao, subset=['Cot'])


    if ticker:

        sql_where = f' WHERE Ticker = \'{ticker}\''
        cot_acoes = read_cotacoes_acoes(sql_where)

        # cot = cotacoes_acoes[cotacoes_acoes.Ticker == ticker].copy()
        # Bandas Bollinger
        WINDOW = 20
        cot_acoes['sma'] = cot_acoes['close'].rolling(WINDOW).mean()
        cot_acoes['std'] = cot_acoes['close'].rolling(WINDOW).std(ddof = 0)
        cot_acoes['bol_upp'] = cot_acoes['sma'] + (cot_acoes['std'] * 2)
        cot_acoes['bol_low'] = cot_acoes['sma'] - (cot_acoes['std'] * 2)

        var = cot_acoes.iloc[-1]['close'] / cot_acoes.iloc[-2]['close'] - 1
        ws_color = define_color(var)

        st.write(f'''<b>{ticker} R$ {cot_acoes.iloc[-1]['close']:,.2f} <span style="{ws_color}"> {var:,.2%}</span></b>''', unsafe_allow_html=True)

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
    df = pd.read_sql(f'SELECT * FROM cotacao_historica_acao {sql_where}', con=conn)
    df.date = pd.to_datetime(df.date)
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


def read_opcoes(sql_where=''):
    df =  pd.read_sql(f'SELECT * FROM opcoes {sql_where}', con=conn)
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


def data_maxima():
    df = pd.read_sql('SELECT max(time) AS data_maxima FROM cotacao_atual_acao', con=conn)
    df.data_maxima = pd.to_datetime(df.data_maxima)
    data_maxima = df.data_maxima.iloc[0]
    return data_maxima


st.set_page_config(
    layout='wide',
    initial_sidebar_state='collapsed',
    page_icon='app.ico',
    page_title='Opções')

# Se for o primeiro processamento do dia, carrega base
hoje = datetime.datetime.today().date()
hoje_str = hoje.strftime('%Y-%m-%d')
controle_data = oplab_to_database.get_controle_data(conn)

if controle_data != hoje_str:
    df = pd.DataFrame({'data_base': [hoje_str]})
    df.to_sql('controle', conn, if_exists='replace', index=False)
    LISTA_ACOES = oplab_to_database.get_lista_acoes(conn, TOKEN)
    oplab_to_database.get_opcoes(conn, hoje, LISTA_ACOES, TOKEN)
    oplab_to_database.get_cotacao_historica(conn, hoje, LISTA_ACOES, TOKEN)


with st.sidebar:

    menu = st.selectbox(
    label = 'OPÇÕES',
    options = [
        'Opções',
        'Travas'])

    # Define data máxima de negociação
    
    # data_base_b3 = obtem_data_base_b3()
    # st.write(f'Base B3: {data_base_b3.strftime('%d/%m/%Y')}')

    if st.button('Atualiza Cotações'):
        atualiza_cotacoes(conn, hoje, TOKEN)

    if st.button('Calcula Estrutura Trava'):
        gera_estrutura_trava()

if menu == 'Opções':
    quadro_opcoes()

if menu == 'Travas':
    estrutura_trava()

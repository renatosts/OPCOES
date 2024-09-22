from scipy.stats import norm
import numpy as np


def black_scholes_greeks(S, K, days_to_maturity, r, sigma, use_business_days=False):
    # Converte dias para anos
    if use_business_days:
        T = days_to_maturity / 252
    else:
        T = days_to_maturity / 365

    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    
    delta_call = norm.cdf(d1)
    delta_put = norm.cdf(d1) - 1

    gamma = norm.pdf(d1) / (S * sigma * np.sqrt(T))
    
    theta_call = (-S * norm.pdf(d1) * sigma / (2 * np.sqrt(T)) 
                  - r * K * np.exp(-r * T) * norm.cdf(d2))
    theta_put = (-S * norm.pdf(d1) * sigma / (2 * np.sqrt(T)) 
                 + r * K * np.exp(-r * T) * norm.cdf(-d2))

    vega = S * norm.pdf(d1) * np.sqrt(T)
    
    rho_call = K * T * np.exp(-r * T) * norm.cdf(d2)
    rho_put = -K * T * np.exp(-r * T) * norm.cdf(-d2)

    return {
        'Delta Call': delta_call,
        'Delta Put': delta_put,
        'Gamma': gamma,
        'Theta Call': theta_call,
        'Theta Put': theta_put,
        'Vega': vega,
        'Rho Call': rho_call,
        'Rho Put': rho_put
    }


def black_scholes_call(S, K, T, r, sigma):
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    call_price = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    return call_price


def black_scholes_put(S, K, T, r, sigma):
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    put_price = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
    return put_price


def implied_volatility_call(C_market, S, K, T, r):
    # Definir limites para a volatilidade
    sigma_low = 0.01
    sigma_high = 3.0
    sigma_mid = (sigma_low + sigma_high) / 2

    # Processo iterativo para busca binária
    tolerance = 1e-5  # Critério de convergência
    while sigma_high - sigma_low > tolerance:
        sigma_mid = (sigma_low + sigma_high) / 2
        C_theoretical = black_scholes_call(S, K, T, r, sigma_mid)

        if C_theoretical < C_market:
            sigma_low = sigma_mid
        else:
            sigma_high = sigma_mid

    return sigma_mid


def implied_volatility_put(P_market, S, K, T, r):
    # Definir limites para a volatilidade
    sigma_low = 0.01
    sigma_high = 3.0
    sigma_mid = (sigma_low + sigma_high) / 2

    # Processo iterativo para busca binária
    tolerance = 1e-5  # Critério de convergência
    while sigma_high - sigma_low > tolerance:
        sigma_mid = (sigma_low + sigma_high) / 2
        P_theoretical = black_scholes_put(S, K, T, r, sigma_mid)

        if P_theoretical < P_market:
            sigma_low = sigma_mid
        else:
            sigma_high = sigma_mid

    return sigma_mid


# Função para calcular a volatilidade implícita com base no tipo de opção
def calcular_volatilidade(row, r):
    S = row['cotacao']
    K = row['strike']
    T = row['dias'] / 365  # Tempo até o vencimento em anos
    # r = 0.1075
    preco_mercado = row['premio']
    
    if row['tipo'] == 'CALL':
        return implied_volatility_call(preco_mercado, S, K, T, r)
    elif row['tipo'] == 'PUT':
        return implied_volatility_put(preco_mercado, S, K, T, r)
    else:
        return np.nan


'''
# Exemplo de uso
S = 100  # Preço da ação
K = 100  # Preço de exercício
days_to_maturity = 90  # Dias até a maturidade
r = 0.05 # Taxa de juros livre de risco
sigma = 0.2  # Volatilidade

# Use use_business_days=True para dias úteis ou use_business_days=False para dias corridos
greeks = black_scholes_greeks(S, K, days_to_maturity, r, sigma, use_business_days=False)
for greek, value in greeks.items():
    print(f"{greek}: {value:.4f}")

# Exemplo de uso
S = 26.20  # Preço da ação
K = 28.17  # Preço de exercício
days_to_maturity = 38  # Dias até a maturidade
r = 0.105 # Taxa de juros livre de risco
sigma = 0.174  # Volatilidade

greeks = black_scholes_greeks(S, K, days_to_maturity, r, sigma, use_business_days=False)
for greek, value in greeks.items():
    print(f"{greek}: {value:.4f}")

    
# Exemplo de uso
S = 100  # preço do ativo
K = 100  # preço de exercício
T = 30 / 365  # tempo até o vencimento (em anos)
r = 0.01  # taxa de juros livre de risco
C_market = 5  # preço de mercado da opção

volatilidade_implicita = implied_volatility_call(C_market, S, K, T, r)
print(f'Volatilidade Implícita: {volatilidade_implicita:.2%}')


# Exemplo de uso
S = 100  # preço do ativo subjacente
K = 100  # preço de exercício (strike)
T = 30 / 365  # tempo até o vencimento (em anos)
r = 0.01  # taxa de juros livre de risco
P_market = 5  # preço de mercado da opção de venda

volatilidade_implicita = implied_volatility_put(P_market, S, K, T, r)
print(f'Volatilidade Implícita: {volatilidade_implicita:.2%}')

'''


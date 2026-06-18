import numpy as np
import pandas as pd

class PortfolioEvaluator:
    def __init__(self, risk_free_rate=0.0):
        self.rf = risk_free_rate 
        
    def calculate_metrics(self, df_resultados):
        df = df_resultados.copy()
        
        # Retorno individual de cada decisión
        df['retorno_estrategia'] = np.where(df['y_pred'] == 1, df['fwd_log_return'],
                                   np.where(df['y_pred'] == -1, -df['fwd_log_return'], 0.0))
        
        # Agrupamos por fecha (index) y promediamos. 
        # Esto soluciona el bicho de panel data dividiendo el capital por día entre los activos
        diario = df.groupby(level=0)[['fwd_log_return', 'retorno_estrategia']].mean()
        
        retornos_estrategia = diario['retorno_estrategia'].values
        retornos_mercado = diario['fwd_log_return'].values
        
        # 1. Retorno Acumulado correcto
        retorno_acum_estrategia = np.exp(np.sum(retornos_estrategia)) - 1
        retorno_acum_mercado = np.exp(np.sum(retornos_mercado)) - 1
        
        # 2. Maximum Drawdown diario
        curva_capital = np.exp(np.cumsum(retornos_estrategia))
        picos_maximos = np.maximum.accumulate(curva_capital)
        drawdowns = (curva_capital - picos_maximos) / picos_maximos
        mdd = np.min(drawdowns) 
        
        # 3. Sharpe Ratio Anualizado basado en retornos diarios del portafolio
        media_diaria = np.mean(retornos_estrategia)
        std_diaria = np.std(retornos_estrategia) + 1e-8
        sharpe_ratio = np.sqrt(252) * (media_diaria - (self.rf / 252)) / std_diaria
        
        return {
            "Total_Return_Strat": retorno_acum_estrategia,
            "Total_Return_Market": retorno_acum_mercado,
            "Max_Drawdown": mdd,
            "Sharpe_Ratio": sharpe_ratio
        }
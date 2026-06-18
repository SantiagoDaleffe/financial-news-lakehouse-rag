import yfinance as yf
import numpy as np

def add_vix_features(df_principal, start_date="2020-01-01", end_date="2024-01-01"):
    """
    Descarga el VIX, calcula su variación y lo pega al DataFrame principal.
    """
    print("Descargando ^VIX...")
    # El ticker del VIX en Yahoo Finance lleva el acento circunflejo
    vix_raw = yf.download("^VIX", start=start_date, end=end_date)
    
    # Nos quedamos solo con el cierre y normalizamos el nombre
    df_vix = vix_raw[['Close']].copy()
    df_vix.columns = ['vix_close']
    df_vix.index.name = 'date'
    
    # 1. Nivel Absoluto de Miedo: El VIX de por sí ya es estacionario (oscila entre 10 y 80)
    # 2. Variación de Miedo (Momentum): Qué tan rápido subió el pánico hoy
    df_vix['vix_return'] = np.log(df_vix['vix_close'] / df_vix['vix_close'].shift(1))
    
    # Pegamos el VIX al DataFrame de nuestro ticker (ej. SPY) usando la fecha (index)
    df_merged = df_principal.merge(df_vix, left_index=True, right_index=True, how='left')
    
    return df_merged
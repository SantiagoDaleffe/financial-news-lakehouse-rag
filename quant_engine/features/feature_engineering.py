import numpy as np
import pandas as pd

class TechnicalFeatureEngineer:
    def __init__(self):
        # Columnas crudas que NO deben ir al modelo
        self.columnas_prohibidas = ['open', 'high', 'low', 'adj_close', 'volume', 'vix_close', 'ret_spy']
        
    def transform(self, df_panel):
        df = df_panel.copy()
        
        # --- 1. SPY COMO REFERENCIA MACRO ---
        df_spy_raw = df[df['ticker'] == 'SPY'].copy()
        df_spy_signals = pd.DataFrame({
            'ret_spy_1d': np.log(df_spy_raw['close'] / df_spy_raw['close'].shift(1)),
            'ret_spy_14d': np.log(df_spy_raw['close'] / df_spy_raw['close'].shift(14))
        }, index=df_spy_raw.index)
        df_spy_signals = df_spy_signals[~df_spy_signals.index.duplicated(keep='first')]
        df = df.join(df_spy_signals, how='left')
        
        grupos_procesados = []
        
        for ticker, grupo in df.groupby('ticker', observed=True):
            grupo = grupo.sort_index()
            c = grupo['close']
            h = grupo['high']
            l = grupo['low']
            
            # --- RETORNOS MULTI-VENTANA ---
            grupo['ret_1d'] = np.log(c / c.shift(1))
            grupo['ret_3d'] = np.log(c / c.shift(3))
            grupo['ret_5d'] = np.log(c / c.shift(5))
            grupo['ret_10d'] = np.log(c / c.shift(10))
            grupo['ret_20d'] = np.log(c / c.shift(20))
            
            # --- CONTEXTO Y FUERZA RELATIVA ---
            grupo['fuerza_rel_14d'] = grupo['ret_14d'] = np.log(c / c.shift(14)) - grupo['ret_spy_14d']
            
            var_spy = grupo['ret_spy_1d'].rolling(20).var()
            cov_ts = grupo['ret_1d'].rolling(20).cov(grupo['ret_spy_1d'])
            grupo['beta_20d'] = cov_ts / (var_spy + 1e-8)
            grupo['corr_spy_20d'] = grupo['ret_1d'].rolling(20).corr(grupo['ret_spy_1d'])
            
            # --- VOLATILIDAD Y SHOCKS ---
            vol_1d = abs(grupo['ret_1d'])
            grupo['vol_20d_std'] = grupo['ret_1d'].rolling(20).std()
            grupo['vol_shock_z'] = (vol_1d - vol_1d.rolling(20).mean()) / (grupo['vol_20d_std'] + 1e-8)
            
            # Aceleración del precio (Segunda derivada)
            grupo['accel_1d'] = grupo['ret_1d'] - grupo['ret_1d'].shift(1)
            
            if 'vix_close' in grupo.columns:
                grupo['div_vix'] = grupo['ret_1d'] * np.log(grupo['vix_close'] / grupo['vix_close'].shift(1))
                
            # --- MEDIAS MÓVILES (Distancias) ---
            for period in [9, 20, 50, 200]:
                sma = c.rolling(period).mean()
                ema = c.ewm(span=period, adjust=False).mean()
                grupo[f'dist_sma_{period}'] = (c - sma) / sma
                grupo[f'dist_ema_{period}'] = (c - ema) / ema
                
            # Ratio de EMAs (Cortes dorados/de la muerte)
            grupo['cross_ema_9_20'] = c.ewm(span=9).mean() / c.ewm(span=20).mean()
            grupo['cross_sma_50_200'] = c.rolling(50).mean() / c.rolling(200).mean()
            
            # --- CANALES DE BOLLINGER ---
            sma_20 = c.rolling(20).mean()
            std_20 = c.rolling(20).std()
            upper_bb = sma_20 + (std_20 * 2)
            lower_bb = sma_20 - (std_20 * 2)
            grupo['bb_width'] = (upper_bb - lower_bb) / sma_20
            grupo['bb_pct_b'] = (c - lower_bb) / (upper_bb - lower_bb + 1e-8) # Dónde está el precio dentro del canal
            
            # --- OSCILADORES DE MOMENTUM ---
            # RSI Múltiple
            delta = c.diff()
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            for period in [9, 14, 21]:
                rs = gain.rolling(period).mean() / (loss.rolling(period).mean() + 1e-8)
                grupo[f'rsi_{period}'] = 100 - (100 / (1 + rs))
                
            # MACD Estándar (12, 26, 9)
            ema_12 = c.ewm(span=12, adjust=False).mean()
            ema_26 = c.ewm(span=26, adjust=False).mean()
            macd_line = ema_12 - ema_26
            signal_line = macd_line.ewm(span=9, adjust=False).mean()
            grupo['macd_hist'] = macd_line - signal_line
            
            # --- MICROESTRUCTURA DE VELAS Y ATR ---
            rango = (h - l) + 1e-8
            grupo['cuerpo_vela'] = abs(grupo['open'] - c) / rango
            grupo['sombra_sup'] = (h - np.maximum(grupo['open'], c)) / rango
            grupo['sombra_inf'] = (np.minimum(grupo['open'], c) - l) / rango
            
            tr = pd.concat([h - l, abs(h - c.shift(1)), abs(l - c.shift(1))], axis=1).max(axis=1)
            grupo['natr_14'] = tr.rolling(14).mean() / c
            
            # --- TARGET (Log retorno de mañana) ---
            grupo['fwd_log_return'] = np.log(grupo['close'].shift(-1) / grupo['close'])
            
            grupos_procesados.append(grupo)
            
        df_res = pd.concat(grupos_procesados).sort_index()
        df_res = df_res.dropna().drop(columns=self.columnas_prohibidas + ['ret_spy_1d', 'ret_spy_14d'], errors='ignore')
        
        return df_res
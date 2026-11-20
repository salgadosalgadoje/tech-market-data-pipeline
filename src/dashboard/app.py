"""Tech Market Data Pipeline - Interactive Analytics Dashboard (Streamlit).

Phase 5: Analytical Consumption Layer directly connected to the PostgreSQL Gold Layer
(`market_daily_metrics`).

Features:
- Executive KPI cards with latest financial metrics and indicators.
- Interactive Plotly time-series visualizations (Close Price, SMA-7, SMA-30, OHLC Candlestick).
- Traded volume and rolling volatility dynamics.
- Cross-asset comparison with normalized returns.
- Full market summary table and CSV data export.
- Graceful exception handling for database connectivity.
"""

from datetime import date, datetime, timedelta
import logging
import os
import sys
from typing import List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import streamlit as st

# Ensure repository root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.load_postgres import MarketDailyMetrics, PostgresLoaderConfig

# Logger configuration
logger = logging.getLogger("market_pipeline.dashboard")

# Streamlit Page Configuration
st.set_page_config(
    page_title="Tech Market Analytics | Capa Oro",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling
st.markdown(
    """
    <style>
    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1.05rem;
        color: #4B5563;
        margin-bottom: 1.5rem;
    }
    .kpi-card {
        background-color: #F8FAFC;
        border-radius: 8px;
        padding: 16px;
        border-left: 4px solid #3B82F6;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource(show_spinner=False)
def get_db_engine() -> Engine:
    """Create and cache the SQLAlchemy database engine."""
    config = PostgresLoaderConfig()
    return create_engine(config.get_connection_uri(), pool_pre_ping=True)


def check_db_connection(engine: Engine) -> Tuple[bool, Optional[str]]:
    """Test database connectivity with a lightweight SELECT 1 query."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, None
    except Exception as exc:
        logger.error("Database connection probe failed: %s", exc)
        return False, str(exc)


@st.cache_data(ttl=60, show_spinner=False)
def load_available_tickers(_engine: Engine) -> List[str]:
    """Retrieve all distinct tickers available in the Gold Layer."""
    with _engine.connect() as conn:
        stmt = select(MarketDailyMetrics.ticker).distinct().order_by(MarketDailyMetrics.ticker.asc())
        rows = conn.execute(stmt).fetchall()
        return [r[0] for r in rows]


@st.cache_data(ttl=60, show_spinner=False)
def load_ticker_metrics(
    _engine: Engine,
    ticker: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> pd.DataFrame:
    """Load historical metrics for a given ticker into a pandas DataFrame."""
    query = (
        select(MarketDailyMetrics)
        .where(MarketDailyMetrics.ticker == ticker)
        .order_by(MarketDailyMetrics.date.asc())
    )
    if start_date:
        query = query.where(MarketDailyMetrics.date >= start_date)
    if end_date:
        query = query.where(MarketDailyMetrics.date <= end_date)

    with _engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if not df.empty and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


@st.cache_data(ttl=60, show_spinner=False)
def load_market_summary(_engine: Engine) -> pd.DataFrame:
    """Generate a comparative summary DataFrame across all tickers."""
    with _engine.connect() as conn:
        tickers_stmt = select(MarketDailyMetrics.ticker).distinct().order_by(MarketDailyMetrics.ticker.asc())
        tickers = [r[0] for r in conn.execute(tickers_stmt).fetchall()]

        rows = []
        for t in tickers:
            latest_stmt = (
                select(MarketDailyMetrics)
                .where(MarketDailyMetrics.ticker == t)
                .order_by(MarketDailyMetrics.date.desc())
                .limit(1)
            )
            latest = conn.execute(latest_stmt).fetchone()
            if not latest:
                continue

            avg_vol_stmt = select(func.avg(MarketDailyMetrics.volatility_7)).where(
                MarketDailyMetrics.ticker == t
            )
            avg_vol = conn.execute(avg_vol_stmt).scalar()

            rows.append(
                {
                    "Ticker": t,
                    "Última Fecha": latest.date.strftime("%Y-%m-%d") if hasattr(latest.date, "strftime") else str(latest.date),
                    "Cierre (USD)": round(float(latest.close), 2),
                    "Retorno Diario (%)": round(float(latest.daily_return * 100), 2) if latest.daily_return is not None else 0.0,
                    "Volatilidad 7d (%)": round(float(latest.volatility_7 * 100), 2) if latest.volatility_7 is not None else 0.0,
                    "Volatilidad 7d Media (%)": round(float(avg_vol * 100), 2) if avg_vol is not None else 0.0,
                    "Volumen (Acciones)": int(latest.volume),
                    "SMA 7": round(float(latest.sma_7), 2) if latest.sma_7 is not None else None,
                    "SMA 30": round(float(latest.sma_30), 2) if latest.sma_30 is not None else None,
                }
            )

        return pd.DataFrame(rows)


# =====================================================================
# Main Application Flow
# =====================================================================

def main() -> None:
    """Streamlit Application Entrypoint."""
    # Top Header
    st.markdown('<div class="main-header">📈 Tech Market Data Pipeline — Dashboard Analítico</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">Capa de Consumo Analítico (Fase 5) conectada a la <b>Capa Oro (PostgreSQL)</b>. '
        'Monitoreo ejecutivo de precios, medias móviles, volatilidad y volúmenes bursátiles.</div>',
        unsafe_allow_html=True,
    )

    # Establish Engine & Validate Connection
    try:
        engine = get_db_engine()
        connected, err_msg = check_db_connection(engine)
    except Exception as exc:
        connected = False
        err_msg = str(exc)

    if not connected:
        config = PostgresLoaderConfig()
        st.error(
            "🚨 **Error de Conexión con PostgreSQL (Capa Oro)**\n\n"
            f"No fue posible establecer conexión con la base de datos en `{config.db_host}:{config.db_port}/{config.db_name}`.\n\n"
            "**Diagnóstico y Solución:**\n"
            "1. Asegúrate de que el contenedor de PostgreSQL en Docker esté en ejecución:\n"
            "   ```bash\n"
            "   docker-compose up -d postgres\n"
            "   ```\n"
            "2. Verifica que las credenciales en el archivo `.env` coincidan con el servicio.\n"
            f"3. Detalle del error: `{err_msg}`"
        )
        if st.button("🔄 Reintentar Conexión"):
            st.cache_resource.clear()
            st.cache_data.clear()
            st.rerun()
        st.stop()

    # Load Tickers
    try:
        available_tickers = load_available_tickers(engine)
    except Exception as exc:
        st.error(f"Error al consultar tickers disponibles en la base de datos: {exc}")
        st.stop()

    if not available_tickers:
        st.warning(
            "⚠️ La tabla `market_daily_metrics` en PostgreSQL no contiene registros aún.\n\n"
            "Ejecuta el pipeline de ingestión de la Capa Oro para poblar los datos:\n"
            "```bash\n"
            "python -m src.load_postgres\n"
            "```"
        )
        st.stop()

    # =====================================================================
    # Sidebar: Filters & Controls
    # =====================================================================
    st.sidebar.header("🎛️ Filtros de Control")

    # Primary Ticker Selector
    selected_ticker = st.sidebar.selectbox(
        "📌 Activo Principal",
        options=available_tickers,
        index=0,
        help="Selecciona el ticker principal para visualizar sus series históricas e indicadores.",
    )

    # Comparison Multi-select
    comparison_tickers = st.sidebar.multiselect(
        "📊 Comparar con Otros Activos",
        options=available_tickers,
        default=[t for t in available_tickers if t != selected_ticker][:2],
        help="Selecciona activos adicionales para comparar retornos y desempeño relativo.",
    )

    # Date Range Filter
    # Fetch date bounds for selected ticker
    df_primary_all = load_ticker_metrics(engine, selected_ticker)
    if not df_primary_all.empty:
        min_date = df_primary_all["date"].min().date()
        max_date = df_primary_all["date"].max().date()
    else:
        min_date = date.today() - timedelta(days=90)
        max_date = date.today()

    date_range = st.sidebar.date_input(
        "📅 Rango de Fechas",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        help="Filtra el intervalo de tiempo para los gráficos y análisis.",
    )

    start_date: Optional[date] = None
    end_date: Optional[date] = None
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_date, end_date = date_range

    # Chart Style Toggle
    chart_type = st.sidebar.radio(
        "📈 Modo de Gráfico Principal",
        options=["Línea con Medias Móviles (SMA-7 / SMA-30)", "Velas Japonesas (OHLC)"],
        index=0,
    )

    # Refresh Button
    if st.sidebar.button("🔄 Refrescar Datos de Base de Datos"):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.caption("💡 **Pipeline:** Bronze ➔ Silver ➔ Gold (PostgreSQL) ➔ FastAPI / Streamlit")

    # Filter Primary DataFrame
    df_primary = load_ticker_metrics(engine, selected_ticker, start_date=start_date, end_date=end_date)

    if df_primary.empty:
        st.warning(f"No se encontraron datos para {selected_ticker} en el rango seleccionado.")
        st.stop()

    # =====================================================================
    # Top KPI Cards
    # =====================================================================
    latest_record = df_primary.iloc[-1]
    prev_record = df_primary.iloc[-2] if len(df_primary) > 1 else None

    # Delta price
    delta_price = (latest_record["close"] - prev_record["close"]) if prev_record is not None else 0.0
    daily_ret_pct = (latest_record["daily_return"] * 100) if latest_record["daily_return"] is not None else 0.0
    vol_7_pct = (latest_record["volatility_7"] * 100) if latest_record["volatility_7"] is not None else 0.0

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)

    with kpi1:
        st.metric(
            label=f"Último Cierre ({selected_ticker})",
            value=f"${latest_record['close']:,.2f}",
            delta=f"{delta_price:+,.2f} USD ({daily_ret_pct:+.2f}%)" if prev_record is not None else None,
        )

    with kpi2:
        st.metric(
            label="Retorno Diario",
            value=f"{daily_ret_pct:+.2f}%",
            delta=f"{daily_ret_pct:+.2f}%",
            delta_color="normal",
        )

    with kpi3:
        st.metric(
            label="Volatilidad Móvil 7d",
            value=f"{vol_7_pct:.2f}%",
            help="Desviación estándar de los retornos diarios calculada en una ventana de 7 sesiones.",
        )

    with kpi4:
        st.metric(
            label="Volumen Transado",
            value=f"{int(latest_record['volume']):,}",
            help="Total de acciones negociadas en la sesión bursátil más reciente.",
        )

    st.markdown("---")

    # =====================================================================
    # Main Price Chart with Technical Indicators
    # =====================================================================
    st.subheader(f"📊 Evolución de Precios e Indicadores Técnicos — {selected_ticker}")

    fig_main = go.Figure()

    if chart_type == "Velas Japonesas (OHLC)":
        fig_main.add_trace(
            go.Candlestick(
                x=df_primary["date"],
                open=df_primary["open"],
                high=df_primary["high"],
                low=df_primary["low"],
                close=df_primary["close"],
                name="Velas OHLC",
                increasing_line_color="#22C55E",
                decreasing_line_color="#EF4444",
            )
        )
    else:
        fig_main.add_trace(
            go.Scatter(
                x=df_primary["date"],
                y=df_primary["close"],
                mode="lines",
                name="Precio de Cierre",
                line=dict(color="#2563EB", width=2.5),
            )
        )

    # Add Moving Averages
    if "sma_7" in df_primary.columns and df_primary["sma_7"].notna().any():
        fig_main.add_trace(
            go.Scatter(
                x=df_primary["date"],
                y=df_primary["sma_7"],
                mode="lines",
                name="SMA 7 (Corto Plazo)",
                line=dict(color="#F59E0B", width=1.8, dash="solid"),
            )
        )

    if "sma_30" in df_primary.columns and df_primary["sma_30"].notna().any():
        fig_main.add_trace(
            go.Scatter(
                x=df_primary["date"],
                y=df_primary["sma_30"],
                mode="lines",
                name="SMA 30 (Medio Plazo)",
                line=dict(color="#8B5CF6", width=1.8, dash="dash"),
            )
        )

    fig_main.update_layout(
        template="plotly_white",
        height=480,
        margin=dict(l=20, r=20, t=30, b=20),
        xaxis=dict(
            title="Fecha",
            rangeslider=dict(visible=False),
            showgrid=True,
            gridcolor="#F3F4F6",
        ),
        yaxis=dict(
            title="Precio en USD ($)",
            tickprefix="$",
            showgrid=True,
            gridcolor="#F3F4F6",
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        hovermode="x unified",
    )

    st.plotly_chart(fig_main, use_container_width=True)

    # =====================================================================
    # Volume & Volatility / Returns Row
    # =====================================================================
    col_vol, col_risk = st.columns(2)

    with col_vol:
        st.subheader("📦 Volumen Diario Negociado")
        colors = [
            "#22C55E" if ret is not None and ret >= 0 else "#EF4444"
            for ret in df_primary["daily_return"]
        ]
        fig_vol = go.Figure(
            data=[
                go.Bar(
                    x=df_primary["date"],
                    y=df_primary["volume"],
                    marker_color=colors,
                    name="Volumen",
                )
            ]
        )
        fig_vol.update_layout(
            template="plotly_white",
            height=320,
            margin=dict(l=20, r=20, t=20, b=20),
            xaxis=dict(title="Fecha", showgrid=True, gridcolor="#F3F4F6"),
            yaxis=dict(title="Volumen (Acciones)", showgrid=True, gridcolor="#F3F4F6"),
        )
        st.plotly_chart(fig_vol, use_container_width=True)

    with col_risk:
        st.subheader("⚡ Volatilidad Móvil 7d y Retorno Diario")
        fig_risk = make_subplots(specs=[[{"secondary_y": True}]])

        # Bar for daily returns
        fig_risk.add_trace(
            go.Bar(
                x=df_primary["date"],
                y=df_primary["daily_return"] * 100,
                name="Retorno Diario (%)",
                marker_color="#93C5FD",
                opacity=0.6,
            ),
            secondary_y=False,
        )

        # Line for 7-day rolling volatility
        fig_risk.add_trace(
            go.Scatter(
                x=df_primary["date"],
                y=df_primary["volatility_7"] * 100,
                name="Volatilidad 7d (%)",
                line=dict(color="#DC2626", width=2.2),
            ),
            secondary_y=True,
        )

        fig_risk.update_layout(
            template="plotly_white",
            height=320,
            margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            hovermode="x unified",
        )
        fig_risk.update_yaxes(title_text="Retorno (%)", secondary_y=False, showgrid=False)
        fig_risk.update_yaxes(title_text="Volatilidad (%)", secondary_y=True, showgrid=True, gridcolor="#F3F4F6")
        st.plotly_chart(fig_risk, use_container_width=True)

    # =====================================================================
    # Cross-Asset Comparison Section
    # =====================================================================
    all_selected_tickers = list(dict.fromkeys([selected_ticker] + comparison_tickers))

    if len(all_selected_tickers) > 1:
        st.markdown("---")
        st.subheader("🔄 Comparativa de Rendimiento Relativo (Base 100)")
        st.caption(
            "Evolución porcentual normalizada (Base 100 al inicio del período) para comparar el rendimiento "
            "entre distintos activos bursátiles."
        )

        fig_comp = go.Figure()

        for t in all_selected_tickers:
            df_t = load_ticker_metrics(engine, t, start_date=start_date, end_date=end_date)
            if not df_t.empty and len(df_t) > 1:
                base_close = df_t["close"].iloc[0]
                if base_close > 0:
                    normalized = (df_t["close"] / base_close) * 100
                    fig_comp.add_trace(
                        go.Scatter(
                            x=df_t["date"],
                            y=normalized,
                            mode="lines",
                            name=f"{t} (Base 100)",
                            line=dict(width=2),
                        )
                    )

        fig_comp.update_layout(
            template="plotly_white",
            height=380,
            margin=dict(l=20, r=20, t=20, b=20),
            xaxis=dict(title="Fecha", showgrid=True, gridcolor="#F3F4F6"),
            yaxis=dict(
                title="Rendimiento Normalizado (Base = 100)",
                showgrid=True,
                gridcolor="#F3F4F6",
            ),
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_comp, use_container_width=True)

    # =====================================================================
    # Market Summary Table & CSV Export
    # =====================================================================
    st.markdown("---")
    st.subheader("📋 Resumen Comparativo del Mercado")

    summary_df = load_market_summary(engine)
    if not summary_df.empty:
        st.dataframe(
            summary_df,
            use_container_width=True,
            hide_index=True,
        )

    # Download Data Button
    st.subheader(f"📥 Descarga de Datos Filtrados — {selected_ticker}")
    csv_data = df_primary.to_csv(index=False).encode("utf-8")
    st.download_button(
        label=f"⬇️ Descargar CSV de {selected_ticker} ({len(df_primary)} registros)",
        data=csv_data,
        file_name=f"{selected_ticker}_gold_metrics_{date.today().strftime('%Y%m%d')}.csv",
        mime="text/csv",
        help="Descarga la serie histórica filtrada de la Capa Oro en formato CSV.",
    )


if __name__ == "__main__":
    main()

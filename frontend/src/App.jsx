import React, { useState, useEffect, useRef, Component } from 'react';
import { createChart, CrosshairMode, LineStyle } from 'lightweight-charts';
import './App.css';

// Error Boundary Component
class ErrorBoundary extends Component {
  state = { hasError: false, error: null };

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="error-message">
          <h2>Error Rendering Chart</h2>
          <p>{this.state.error?.message || 'An unexpected error occurred.'}</p>
          <p>Please check the console for details and try another run.</p>
        </div>
      );
    }
    return this.props.children;
  }
}

const App = () => {
  const [runs, setRuns] = useState([]);
  const [selectedRunId, setSelectedRunId] = useState('');
  const [signals, setSignals] = useState([]);
  const [trades, setTrades] = useState([]);
  const [metrics, setMetrics] = useState(null);
  const [finalPortfolioValue, setFinalPortfolioValue] = useState(null);
  const [error, setError] = useState(null);
  const priceChartRef = useRef(null);
  const tradeChartRef = useRef(null);
  const rsiChartRef = useRef(null);
  const priceChartInstance = useRef(null);
  const tradeChartInstance = useRef(null);
  const rsiChartInstance = useRef(null);
  const priceSeriesRef = useRef(null);
  const tradeSeriesRef = useRef(null);
  const rsiSeriesRef = useRef(null);
  const isUpdatingTimeScale = useRef(false);

  // Fetch runs on mount
  useEffect(() => {
    fetch('http://localhost:8000/runs')
      .then((res) => res.json())
      .then((data) => {
        console.log('Fetched runs:', data);
        setRuns(data);
      })
      .catch((err) => console.error('Error fetching runs:', err));
  }, []);

  // Fetch signals, trades, metrics, and final portfolio value when run_id changes
  useEffect(() => {
    if (selectedRunId) {
      // Fetch signals
      fetch(`http://localhost:8000/signals/${selectedRunId}`)
        .then((res) => res.json())
        .then((data) => {
          console.log('Fetched signals:', data);
          setSignals(data);
          setError(null);
        })
        .catch((err) => {
          console.error('Error fetching signals:', err);
          setError('Failed to fetch signals. Please try another run.');
        });

      // Fetch trades
      fetch(`http://localhost:8000/trades/${selectedRunId}`)
        .then((res) => res.json())
        .then((data) => {
          console.log('Fetched trades:', data);
          setTrades(data);
        })
        .catch((err) => {
          console.error('Error fetching trades:', err);
          setError('Failed to fetch trades. Please try another run.');
        });

      // Fetch metrics
      fetch(`http://localhost:8000/metrics/${selectedRunId}`)
        .then((res) => res.json())
        .then((data) => {
          console.log('Fetched metrics:', data);
          setMetrics(data);
          setError(null);
        })
        .catch((err) => {
          console.error('Error fetching metrics:', err);
          setError('Failed to fetch metrics. Please try another run.');
        });

      // Fetch final portfolio value
      fetch(`http://localhost:8000/portfolio_final/${selectedRunId}`)
        .then((res) => res.json())
        .then((data) => {
          console.log('Fetched final portfolio value:', data);
          setFinalPortfolioValue(data.total);
          setError(null);
        })
        .catch((err) => {
          console.error('Error fetching final portfolio value:', err);
          setError('Failed to fetch final portfolio value. Please try another run.');
        });
    } else {
      // Clear data when no run is selected
      setSignals([]);
      setTrades([]);
      setMetrics(null);
      setFinalPortfolioValue(null);
      setError(null);
    }
  }, [selectedRunId]);

  // Initialize charts
  useEffect(() => {
    if (priceChartRef.current && tradeChartRef.current && rsiChartRef.current) {
      try {
        // Price Chart
        priceChartInstance.current = createChart(priceChartRef.current, {
          width: priceChartRef.current.clientWidth,
          height: 233,
          layout: { background: { color: '#ffffff' }, textColor: '#333' },
          grid: { vertLines: { color: '#e0e0e0' }, horzLines: { color: '#e0e0e0' } },
          crosshair: { mode: CrosshairMode.Normal },
          timeScale: { timeVisible: true, secondsVisible: true },
        });

        // Trade Chart
        tradeChartInstance.current = createChart(tradeChartRef.current, {
          width: tradeChartRef.current.clientWidth,
          height: 233,
          layout: { background: { color: '#ffffff' }, textColor: '#333' },
          grid: { vertLines: { color: '#e0e0e0' }, horzLines: { color: '#e0e0e0' } },
          crosshair: { mode: CrosshairMode.Normal },
          timeScale: { timeVisible: true, secondsVisible: true },
        });

        // RSI Chart
        rsiChartInstance.current = createChart(rsiChartRef.current, {
          width: rsiChartRef.current.clientWidth,
          height: 233,
          layout: { background: { color: '#ffffff' }, textColor: '#333' },
          grid: { vertLines: { color: '#e0e0e0' }, horzLines: { color: '#e0e0e0' } },
          crosshair: { mode: CrosshairMode.Normal },
          timeScale: { timeVisible: true, secondsVisible: true },
        });

        // Synchronize time scales
        const syncTimeScales = (sourceChart, targetCharts) => {
          sourceChart.timeScale().subscribeVisibleTimeRangeChange(() => {
            if (isUpdatingTimeScale.current) return;
            isUpdatingTimeScale.current = true;
            const timeRange = sourceChart.timeScale().getVisibleLogicalRange();
            if (timeRange) {
              targetCharts.forEach((targetChart) => {
              targetChart.timeScale().setVisibleLogicalRange(timeRange);
              });
            }
            isUpdatingTimeScale.current = false;
          });
        };

        syncTimeScales(priceChartInstance.current, [tradeChartInstance.current, rsiChartInstance.current]);
        syncTimeScales(tradeChartInstance.current, [priceChartInstance.current, rsiChartInstance.current]);
        syncTimeScales(rsiChartInstance.current, [priceChartInstance.current, tradeChartInstance.current]);

        // Handle resize
        const handleResize = () => {
          priceChartInstance.current.applyOptions({ width: priceChartRef.current.clientWidth });
          tradeChartInstance.current.applyOptions({ width: tradeChartRef.current.clientWidth });
          rsiChartInstance.current.applyOptions({ width: rsiChartRef.current.clientWidth });
        };
        window.addEventListener('resize', handleResize);

        return () => {
          window.removeEventListener('resize', handleResize);
          priceChartInstance.current.remove();
          tradeChartInstance.current.remove();
          rsiChartInstance.current.remove();
        };
      } catch (err) {
        console.error('Error initializing charts:', err);
        setError('Failed to initialize charts.');
      }
    }
  }, []);

  // Update charts with signal data
  useEffect(() => {
    if (signals.length > 0 && priceChartInstance.current && tradeChartInstance.current && rsiChartInstance.current) {
      try {
        // Clear existing series if they exist
        if (priceSeriesRef.current) {
          priceChartInstance.current.removeSeries(priceSeriesRef.current);
          priceSeriesRef.current = null;
        }
        if (tradeSeriesRef.current) {
          tradeChartInstance.current.removeSeries(tradeSeriesRef.current);
          tradeSeriesRef.current = null;
        }
        if (rsiSeriesRef.current) {
          rsiChartInstance.current.removeSeries(rsiSeriesRef.current);
          rsiSeriesRef.current = null;
        }

        // Validate and prepare data
        const priceData = signals
          .filter((s) => {
            const isValid = s.price != null && !isNaN(s.price) && new Date(s.timestamp).getTime();
            if (!isValid) console.warn('Invalid price data:', s);
            return isValid;
          })
          .map((s) => ({
            time: new Date(s.timestamp).getTime() / 1000,
            value: Number(s.price),
          }));

        const rsiData = signals
          .filter((s) => {
            const isValid = s.rsi != null && !isNaN(s.rsi) && new Date(s.timestamp).getTime();
            if (!isValid) console.warn('Invalid RSI data:', s);
            return isValid;
          })
          .map((s) => ({
            time: new Date(s.timestamp).getTime() / 1000,
            value: Number(s.rsi),
          }));

        const buySignals = signals
          .filter((s) => s.signal === 1 && s.price != null && !isNaN(s.price) && new Date(s.timestamp).getTime())
          .map((s) => ({
            time: new Date(s.timestamp).getTime() / 1000,
            position: 'belowBar',
            color: 'green',
            shape: 'arrowUp',
            text: 'BUY',
          }));

        const sellSignals = signals
          .filter((s) => s.signal === -1 && s.price != null && !isNaN(s.price) && new Date(s.timestamp).getTime())
          .map((s) => ({
            time: new Date(s.timestamp).getTime() / 1000,
            position: 'aboveBar',
            color: 'red',
            shape: 'arrowDown',
            text: 'SELL',
          }));

        // Prepare trade markers
        const tradeMarkers = trades
          .filter((t) => {
            const isValid = t.timestamp && t.action && t.reason && t.price != null && !isNaN(t.price) && new Date(t.timestamp).getTime();
            if (!isValid) console.warn('Invalid trade data:', t);
            return isValid;
          })
          .map((t) => ({
            time: new Date(t.timestamp).getTime() / 1000,
            position: ['Buy', 'Cover'].includes(t.action) ? 'belowBar' : 'aboveBar',
            color: ['Buy', 'Cover'].includes(t.action) ? 'green' : 'red',
            shape: 'circle',
            text: t.reason || 'Trade',
          }));

        // Sort markers by time
        const signalMarkers = [...buySignals, ...sellSignals].sort((a, b) => a.time - b.time);

        console.log('Price data points:', priceData.length);
        console.log('RSI data points:', rsiData.length);
        console.log('Buy signals:', buySignals.length);
        console.log('Sell signals:', sellSignals.length);
        console.log('Total signal markers:', signalMarkers.length);
        console.log('Trade markers:', tradeMarkers.length);

        if (priceData.length === 0) {
          throw new Error('No valid price data to display.');
        }

        // Price Line Series
        priceSeriesRef.current = priceChartInstance.current.addLineSeries({
          color: '#2962FF',
          lineWidth: 2,
        });
        priceSeriesRef.current.setData(priceData);

        // Signal Markers
        priceSeriesRef.current.setMarkers(signalMarkers);

        // Trade Line Series (baseline at y=0)
        tradeSeriesRef.current = tradeChartInstance.current.addLineSeries({
          color: '#2962FF',
          lineWidth: 2,
        });
        tradeSeriesRef.current.setData(priceData);

        // Trade Markers
        tradeSeriesRef.current.setMarkers(tradeMarkers);

        // RSI Line Series
        rsiSeriesRef.current = rsiChartInstance.current.addLineSeries({
          color: '#FF6D00',
          lineWidth: 2,
        });
        rsiSeriesRef.current.setData(rsiData);

        // Add RSI thresholds (e.g., 70 and 30)
        rsiSeriesRef.current.createPriceLine({ 
          price: 70, 
          color: 'red', 
          lineWidth: 1, 
          lineStyle: LineStyle.Dashed,
        });
        rsiSeriesRef.current.createPriceLine({ 
          price: 30, 
          color: 'green', 
          lineWidth: 1, 
          lineStyle: LineStyle.Dashed,
        });

        // Fit content
        priceChartInstance.current.timeScale().fitContent();
        tradeChartInstance.current.timeScale().fitContent();
        rsiChartInstance.current.timeScale().fitContent();
      } catch (err) {
        console.error('Error updating charts:', err);
        setError(`Failed to update charts: ${err.message}`);
      }
    }
  }, [signals, trades]);

  // Format metric value for display
  const formatMetric = (value, key) => {
    if (value == null) return 'N/A';
    if (['max_drawdown_start', 'max_drawdown_end'].includes(key)) {
      return new Date(value).toLocaleString();
    }
    if (['annualized_return', 'max_drawdown', 'win_rate'].includes(key)) {
      return `${Number(value).toFixed(2)}%`;
    }
    if (['sharpe_ratio', 'sortino_ratio', 'calmar_ratio', 'profit_factor'].includes(key)) {
      return Number(value).toFixed(2);
    }
    if (key === 'avg_trade_duration') {
      return `${Number(value).toFixed(2)} min`;
    }
    if (key === 'final_portfolio_value') {
      return `₹${Number(value).toFixed(2)}`;
    }
    return Math.round(value);
  };

  // Define metrics to display
  const metricDisplay = [
    { key: 'final_portfolio_value', label: 'Final Portfolio Value' },
    { key: 'annualized_return', label: 'Annualized Return' },
    { key: 'max_drawdown', label: 'Max Drawdown' },
    { key: 'max_drawdown_start', label: 'Max Drawdown Start' },
    { key: 'max_drawdown_end', label: 'Max Drawdown End' },
    { key: 'win_rate', label: 'Win Rate' },
    { key: 'sharpe_ratio', label: 'Sharpe Ratio' },
    { key: 'sortino_ratio', label: 'Sortino Ratio' },
    { key: 'calmar_ratio', label: 'Calmar Ratio' },
    { key: 'profit_factor', label: 'Profit Factor' },
    { key: 'total_trades', label: 'Total Trades' },
    { key: 'avg_trade_duration', label: 'Avg Trade Duration' },
  ];

  return (
    <div className="app-container">
      <h1>Backtest Signal Visualizer</h1>
      <div className="dropdown-container">
        <label htmlFor="runSelect">Select Run ID: </label>
        <select
          id="runSelect"
          value={selectedRunId}
          onChange={(e) => setSelectedRunId(e.target.value)}
        >
          <option value="">Select a run</option>
          {runs.map((run) => (
            <option key={run.run_id} value={run.run_id}>
              {run.run_id} ({new Date(run.initiated_time).toLocaleString()})
            </option>
          ))}
        </select>
      </div>
      {error && (
        <div className="error-message">
          <h2>Error</h2>
          <p>{error}</p>
        </div>
      )}
      <div className="content-container">
        <div className="charts-container">
      <ErrorBoundary>
        <div className="chart-container">
          <h2>Price Chart</h2>
          <div ref={priceChartRef} className="chart" />
        </div>
        <div className="chart-container">
          <h2>Trade Chart</h2>
          <div ref={tradeChartRef} className="chart" />
        </div>
        <div className="chart-container">
          <h2>RSI Chart</h2>
          <div ref={rsiChartRef} className="chart" />
        </div>
      </ErrorBoundary>
        </div>
        {(metrics || finalPortfolioValue !== null) && (
          <div className="metrics-container">
            <h2>Performance Metrics</h2>
            <table className="metrics-table">
              <thead>
                <tr>
                  <th>Metric</th>
                  <th>Value</th>
                </tr>
              </thead>
              <tbody>
                {metricDisplay.map(({ key, label }) => (
                  <tr key={key}>
                    <td>{label}</td>
                    <td>
                      {key === 'final_portfolio_value'
                        ? formatMetric(finalPortfolioValue, key)
                        : metrics && formatMetric(metrics[key], key)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
};

export default App;
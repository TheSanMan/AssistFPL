'use client';

import { useState, useEffect } from 'react';
import { api, PlayerPrediction, getPositionName, getPositionClass } from './lib/api';
import styles from './page.module.css';

export default function Dashboard() {
  const [topPlayers, setTopPlayers] = useState<PlayerPrediction[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [positionFilter, setPositionFilter] = useState<number | null>(null);

  useEffect(() => {
    loadTopPlayers();
  }, [positionFilter]);

  async function loadTopPlayers() {
    setLoading(true);
    setError(null);
    try {
      const players = await api.getTopPredictions(20, positionFilter || undefined);
      setTopPlayers(players);
    } catch (err) {
      setError('Failed to load predictions. Make sure the API server is running.');
      console.error(err);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="container">
      {/* Hero Section */}
      <section className={styles.hero}>
        <h1 className={styles.title}>
          AI-Powered <span className={styles.gradient}>FPL Assistant</span>
        </h1>
        <p className={styles.subtitle}>
          Get intelligent transfer recommendations powered by machine learning
        </p>
      </section>

      {/* Quick Stats */}
      <section className={styles.statsGrid}>
        <div className={`card ${styles.statCard}`}>
          <div className={styles.statIcon}>⚽</div>
          <div className={styles.statContent}>
            <div className={styles.statValue}>772</div>
            <div className={styles.statLabel}>Players Tracked</div>
          </div>
        </div>
        <div className={`card ${styles.statCard}`}>
          <div className={styles.statIcon}>🧠</div>
          <div className={styles.statContent}>
            <div className={styles.statValue}>ML</div>
            <div className={styles.statLabel}>XGBoost Model</div>
          </div>
        </div>
        <div className={`card ${styles.statCard}`}>
          <div className={styles.statIcon}>📊</div>
          <div className={styles.statContent}>
            <div className={styles.statValue}>0.009</div>
            <div className={styles.statLabel}>Model MAE</div>
          </div>
        </div>
        <div className={`card ${styles.statCard}`}>
          <div className={styles.statIcon}>💬</div>
          <div className={styles.statContent}>
            <div className={styles.statValue}>AI</div>
            <div className={styles.statLabel}>Chat Assistant</div>
          </div>
        </div>
      </section>

      {/* Top Predictions */}
      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <h2>Top Predicted Players</h2>
          <div className={styles.filters}>
            <button
              className={`btn ${positionFilter === null ? 'btn-primary' : 'btn-secondary'}`}
              onClick={() => setPositionFilter(null)}
            >
              All
            </button>
            <button
              className={`btn ${positionFilter === 1 ? 'btn-primary' : 'btn-secondary'}`}
              onClick={() => setPositionFilter(1)}
            >
              GKP
            </button>
            <button
              className={`btn ${positionFilter === 2 ? 'btn-primary' : 'btn-secondary'}`}
              onClick={() => setPositionFilter(2)}
            >
              DEF
            </button>
            <button
              className={`btn ${positionFilter === 3 ? 'btn-primary' : 'btn-secondary'}`}
              onClick={() => setPositionFilter(3)}
            >
              MID
            </button>
            <button
              className={`btn ${positionFilter === 4 ? 'btn-primary' : 'btn-secondary'}`}
              onClick={() => setPositionFilter(4)}
            >
              FWD
            </button>
          </div>
        </div>

        {loading ? (
          <div className={styles.loadingContainer}>
            <div className="loader"></div>
            <p>Loading predictions...</p>
          </div>
        ) : error ? (
          <div className={styles.errorContainer}>
            <p>⚠️ {error}</p>
            <button className="btn btn-primary" onClick={loadTopPlayers}>
              Retry
            </button>
          </div>
        ) : (
          <div className="card">
            <table className="table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Player</th>
                  <th>Position</th>
                  <th>Price</th>
                  <th>Form (3GW)</th>
                  <th>FPL Est.</th>
                  <th>Our Prediction</th>
                </tr>
              </thead>
              <tbody>
                {topPlayers.map((player, index) => (
                  <tr key={player.player_id} className="animate-fadeIn" style={{ animationDelay: `${index * 50}ms` }}>
                    <td>{index + 1}</td>
                    <td>
                      <a href={`/players/${player.player_id}`} className={styles.playerName}>
                        {player.web_name}
                      </a>
                    </td>
                    <td>
                      <span className={`position-badge ${getPositionClass(player.position)}`}>
                        {getPositionName(player.position)}
                      </span>
                    </td>
                    <td>£{player.price.toFixed(1)}m</td>
                    <td>{player.form_points_3.toFixed(1)}</td>
                    <td>{player.api_ep_next.toFixed(1)}</td>
                    <td>
                      <span className={styles.prediction}>
                        {player.predicted_points.toFixed(1)} pts
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {/* CTA Section */}
      <section className={styles.ctaSection}>
        <div className={`card ${styles.ctaCard}`}>
          <h3>Need help with transfers?</h3>
          <p>Ask our AI assistant for personalized recommendations</p>
          <a href="/chat" className="btn btn-primary">
            Start Chatting →
          </a>
        </div>
      </section>
    </div>
  );
}

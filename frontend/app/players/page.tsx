'use client';

import { useState, useEffect } from 'react';
import { api, Player, getPositionName, getPositionClass, formatPrice } from '../lib/api';
import styles from './page.module.css';

export default function PlayersPage() {
  const [players, setPlayers] = useState<Player[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState('');
  const [positionFilter, setPositionFilter] = useState<number | null>(null);
  const [sortBy, setSortBy] = useState<'form' | 'total_points' | 'now_cost' | 'ict_index'>('form');

  useEffect(() => {
    loadPlayers();
  }, []);

  async function loadPlayers() {
    setLoading(true);
    try {
      const data = await api.getPlayers(500);
      setPlayers(data);
    } catch (err) {
      setError('Failed to load players. Make sure the API server is running.');
    } finally {
      setLoading(false);
    }
  }

  const filteredPlayers = players
    .filter(p => {
      const matchesSearch = p.web_name.toLowerCase().includes(search.toLowerCase()) ||
        p.first_name.toLowerCase().includes(search.toLowerCase()) ||
        p.second_name.toLowerCase().includes(search.toLowerCase());
      const matchesPosition = positionFilter === null || p.element_type === positionFilter;
      return matchesSearch && matchesPosition;
    })
    .sort((a, b) => {
      const aVal = a[sortBy] || 0;
      const bVal = b[sortBy] || 0;
      return bVal - aVal;
    })
    .slice(0, 100);

  return (
    <div className="container">
      <div className={styles.header}>
        <h1>Player Explorer</h1>
        <p>Search and filter through all FPL players</p>
      </div>

      {/* Filters */}
      <div className={styles.filters}>
        <input
          type="text"
          className={`input ${styles.searchInput}`}
          placeholder="Search players..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />

        <div className={styles.filterGroup}>
          <label>Position:</label>
          <div className={styles.buttonGroup}>
            {[null, 1, 2, 3, 4].map((pos) => (
              <button
                key={pos ?? 'all'}
                className={`btn ${positionFilter === pos ? 'btn-primary' : 'btn-secondary'}`}
                onClick={() => setPositionFilter(pos)}
              >
                {pos === null ? 'All' : getPositionName(pos)}
              </button>
            ))}
          </div>
        </div>

        <div className={styles.filterGroup}>
          <label>Sort by:</label>
          <select
            className="input"
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value as typeof sortBy)}
          >
            <option value="form">Form</option>
            <option value="total_points">Total Points</option>
            <option value="ict_index">ICT Index</option>
            <option value="now_cost">Price</option>
          </select>
        </div>
      </div>

      {/* Results */}
      {loading ? (
        <div className={styles.loadingContainer}>
          <div className="loader"></div>
          <p>Loading players...</p>
        </div>
      ) : error ? (
        <div className={styles.errorContainer}>
          <p>⚠️ {error}</p>
        </div>
      ) : (
        <>
          <p className={styles.resultCount}>
            Showing {filteredPlayers.length} of {players.length} players
          </p>

          <div className={styles.playerGrid}>
            {filteredPlayers.map((player) => (
              <a href={`/players/${player.id}`} key={player.id} className={styles.cardLink}>
                <div className={`card ${styles.playerCard}`}>
                  <div className={styles.playerHeader}>
                    <span className={`position-badge ${getPositionClass(player.element_type)}`}>
                      {getPositionName(player.element_type)}
                    </span>
                    <span className={styles.price}>{formatPrice(player.now_cost)}</span>
                  </div>
                  <h3 className={styles.playerName}>{player.web_name}</h3>
                  <p className={styles.fullName}>{player.first_name} {player.second_name}</p>

                  <div className={styles.stats}>
                    <div className={styles.stat}>
                      <span className={styles.statLabel}>Form</span>
                      <span className={styles.statValue}>{Number(player.form).toFixed(1) || '-'}</span>
                    </div>
                    <div className={styles.stat}>
                      <span className={styles.statLabel}>Points</span>
                      <span className={styles.statValue}>{player.total_points || 0}</span>
                    </div>
                    <div className={styles.stat}>
                      <span className={styles.statLabel}>ICT</span>
                      <span className={styles.statValue}>{Number(player.ict_index).toFixed(0) || '-'}</span>
                    </div>
                    <div className={styles.stat}>
                      <span className={styles.statLabel}>Owner%</span>
                      <span className={styles.statValue}>{Number(player.selected_by_percent).toFixed(1) || '-'}%</span>
                    </div>
                  </div>

                  {player.status !== 'a' && player.news && (
                    <div className={`badge badge-warning ${styles.newsTag}`}>
                      ⚠️ {player.news.substring(0, 50)}...
                    </div>
                  )}
                </div>
              </a>
            ))}
          </div>
        </>
      )}
    </div>
  );
}

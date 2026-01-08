'use client';

import { useState, useEffect, use } from 'react';
import { api, Player, GameweekHistory, getPositionName, getPositionClass, formatPrice } from '../../lib/api';
import styles from './page.module.css';

export default function PlayerDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const [player, setPlayer] = useState<Player | null>(null);
  const [history, setHistory] = useState<GameweekHistory[]>([]);
  const [fixtures, setFixtures] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    loadData();
  }, [id]);

  async function loadData() {
    setLoading(true);
    try {
      const playerId = parseInt(id);
      const [p, h, f] = await Promise.all([
        api.getPlayer(playerId),
        api.getPlayerHistory(playerId),
        api.getPlayerFixtures(playerId)
      ]);
      setPlayer(p);
      setHistory(h);
      setFixtures(f);
    } catch (err) {
      setError('Failed to load player data');
      console.error(err);
    } finally {
      setLoading(false);
    }
  }

  if (loading) return (
    <div className="container flex items-center justify-center min-h-screen">
      <div className="loader"></div>
    </div>
  );

  if (error || !player) return (
    <div className="container text-center py-20">
      <h1>⚠️ {error || 'Player not found'}</h1>
      <a href="/players" className="btn btn-primary mt-4">Back to Players</a>
    </div>
  );

  // Helper to interpret status
  const getStatusText = (status: string, news: string) => {
    if (status === 'a') return null; // Available
    if (status === 'd') return `Doubtful: ${news}`;
    if (status === 'i') return `Injured: ${news}`;
    if (status === 's') return `Suspended: ${news}`;
    if (status === 'u') return `Unavailable: ${news}`;
    return news;
  };

  const statusMessage = getStatusText(player.status, player.news);

  return (
    <div className={styles.container}>
      <a href="/players" className={styles.backLink}>← Back to Players</a>

      {/* Header */}
      <div className={`card ${styles.headerCard}`}>
        <div className={styles.playerInfo}>
          <h1>{player.web_name}</h1>
          <div className={styles.meta}>
            <span className={`position-badge ${getPositionClass(player.element_type)}`}>
              {getPositionName(player.element_type)}
            </span>
            <span className="text-muted">{(player as any).team_name}</span>
            <span className={styles.price}>{formatPrice(player.now_cost)}</span>
          </div>

          {statusMessage && (
            <div className={styles.statusContainer}>
              <span className={styles.statusLabel}>STATUS UPDATE:</span>
              {statusMessage}
            </div>
          )}
        </div>

        <div className={styles.actions}>
          <a href={`/chat?q=Should I buy ${player.web_name}?`} className="btn btn-primary">
            Ask AI Assistant
          </a>
        </div>
      </div>

      <div className={styles.grid}>
        {/* Main Stats Column */}
        <div className="main-col">
          <h2 className={styles.sectionTitle}>Performance Stats</h2>
          <div className={styles.statsGrid}>
            <div className={styles.statBox}>
              <div className={styles.statLabel}>Total Points</div>
              <div className={styles.statValue}>{player.total_points}</div>
            </div>
            <div className={styles.statBox}>
              <div className={styles.statLabel}>Form</div>
              <div className={styles.statValue}>{player.form}</div>
            </div>
            <div className={styles.statBox}>
              <div className={styles.statLabel}>ICT Index</div>
              <div className={styles.statValue}>{player.ict_index}</div>
            </div>
            <div className={styles.statBox}>
              <div className={styles.statLabel}>Ownership</div>
              <div className={styles.statValue}>{player.selected_by_percent}%</div>
            </div>
            <div className={styles.statBox}>
              <div className={styles.statLabel}>Exp. Points</div>
              <div className={styles.statValue}>{player.ep_next}</div>
            </div>
          </div>

          <h2 className={styles.sectionTitle}>Deep Dive</h2>
          <div className={styles.statsGrid}>
            <div className={styles.statBox}>
              <div className={styles.statLabel}>Creativity</div>
              <div className={styles.statValue}>{(player as any).creativity}</div>
            </div>
            <div className={styles.statBox}>
              <div className={styles.statLabel}>Threat</div>
              <div className={styles.statValue}>{(player as any).threat}</div>
            </div>
            <div className={styles.statBox}>
              <div className={styles.statLabel}>Influence</div>
              <div className={styles.statValue}>{(player as any).influence}</div>
            </div>
          </div>

          <h2 className={styles.sectionTitle}>Recent History</h2>
          <div className="card">
            <table className={styles.historyTable}>
              <thead>
                <tr>
                  <th>GW</th>
                  <th>Opponent</th>
                  <th>Pts</th>
                  <th>Mins</th>
                  <th>G</th>
                  <th>A</th>
                  <th>xG</th>
                  <th>xA</th>
                </tr>
              </thead>
              <tbody>
                {history.slice(0, 5).map((h) => (
                  <tr key={h.gameweek}>
                    <td>{h.gameweek}</td>
                    <td>
                      {(h as any).opponent_short} {(h.was_home ? '(H)' : '(A)')}
                    </td>
                    <td style={{ fontWeight: 700 }}>{h.total_points}</td>
                    <td>{h.minutes}</td>
                    <td>{h.goals_scored}</td>
                    <td>{h.assists}</td>
                    <td>{(h as any).expected_goals}</td>
                    <td>{(h as any).expected_assists}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Sidebar: Fixtures */}
        <div className="sidebar">
          <h2 className={styles.sectionTitle}>Upcoming Fixtures</h2>
          <div className={`card ${styles.fixtureList}`}>
            {fixtures.length > 0 ? fixtures.map((f, i) => {
              const isHome = f.team_h === player.team_id;
              const opponent = isHome ? f.team_a_short : f.team_h_short;
              const difficulty = isHome ? f.difficulty_h : f.difficulty_a;
              const date = new Date(f.kickoff_time).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });

              return (
                <div key={i} className={styles.fixtureItem}>
                  <div className={styles.fixtureMeta}>
                    <span className="text-muted text-sm">{date}</span>
                    <span className={styles.fixtureOpponent}>
                      {opponent} {isHome ? '(H)' : '(A)'}
                    </span>
                  </div>
                  <div className={`${styles.difficulty} ${styles[`diff-${difficulty}`]}`}>
                    {difficulty}
                  </div>
                </div>
              );
            }) : (
              <p className="text-muted text-center py-4">No upcoming fixtures scheduled</p>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

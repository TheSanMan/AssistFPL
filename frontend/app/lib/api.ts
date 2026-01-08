/**
 * API Client for AssistFPL Backend
 */

const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export interface Player {
  id: number;
  web_name: string;
  first_name: string;
  second_name: string;
  team_id: number;
  element_type: number;
  now_cost: number;
  form: number;
  ict_index: number;
  ep_next: number;
  total_points: number;
  selected_by_percent: number;
  status: string;
  news: string;
}

export interface PlayerPrediction {
  player_id: number;
  web_name: string;
  team_id: number;
  position: number;
  price: number;
  predicted_points: number;
  api_ep_next: number;
  form_points_3: number;
}

export interface Insight {
  category: string;
  insight: string;
  sentiment: string;
}

export interface GameweekHistory {
  gameweek: number;
  total_points: number;
  minutes: number;
  goals_scored: number;
  assists: number;
  opponent_team: number;
  was_home: boolean;
}

async function fetchAPI<T>(endpoint: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${endpoint}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  });

  if (!res.ok) {
    throw new Error(`API Error: ${res.status} ${res.statusText}`);
  }

  return res.json();
}

export const api = {
  // Health check
  health: () => fetchAPI<{ status: string }>('/'),

  // Players
  getPlayers: (limit = 50) => fetchAPI<Player[]>(`/players?limit=${limit}`),
  getPlayer: (id: number) => fetchAPI<Player>(`/players/${id}`),
  getPlayerPrediction: (id: number) => fetchAPI<{ player_id: number; predicted_points: number }>(`/players/${id}/prediction`),
  getPlayerInsights: (id: number, opponentId?: number) => {
    const params = opponentId ? `?opponent_id=${opponentId}` : '';
    return fetchAPI<Insight[]>(`/players/${id}/insights${params}`);
  },
  getPlayerHistory: (id: number) => fetchAPI<GameweekHistory[]>(`/players/${id}/history`),
  getPlayerFixtures: (id: number) => fetchAPI<any[]>(`/players/${id}/fixtures`),

  // Predictions
  getTopPredictions: (limit = 20, position?: number) => {
    const params = new URLSearchParams({ limit: String(limit) });
    if (position) params.append('position', String(position));
    return fetchAPI<PlayerPrediction[]>(`/predictions/top?${params}`);
  },

  // Transfers
  suggestTransfers: (budget: number, position?: number, excludePlayers?: number[], topN = 5) =>
    fetchAPI<PlayerPrediction[]>('/transfers/suggest', {
      method: 'POST',
      body: JSON.stringify({
        budget,
        position,
        exclude_players: excludePlayers,
        top_n: topN,
      }),
    }),

  // Chat
  chat: (message: string) =>
    fetchAPI<{ response: string }>('/chat', {
      method: 'POST',
      body: JSON.stringify({ message }),
    }),
};

// Helper functions
export function getPositionName(position: number): string {
  const positions: Record<number, string> = { 1: 'GKP', 2: 'DEF', 3: 'MID', 4: 'FWD' };
  return positions[position] || '?';
}

export function getPositionClass(position: number): string {
  const classes: Record<number, string> = {
    1: 'position-gkp',
    2: 'position-def',
    3: 'position-mid',
    4: 'position-fwd'
  };
  return classes[position] || '';
}

export function formatPrice(cost: number): string {
  return `£${(cost / 10).toFixed(1)}m`;
}

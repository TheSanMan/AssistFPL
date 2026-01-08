-- Normalized schema for AssistFPL

-- 1. Teams Table
CREATE TABLE IF NOT EXISTS teams (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    short_name VARCHAR(5) NOT NULL,
    strength INTEGER,
    strength_overall_home INTEGER,
    strength_overall_away INTEGER,
    strength_attack_home INTEGER,
    strength_attack_away INTEGER,
    strength_defence_home INTEGER,
    strength_defence_away INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Players Table (with API-provided metrics)
CREATE TABLE IF NOT EXISTS players (
    id INTEGER PRIMARY KEY,
    team_id INTEGER REFERENCES teams(id),
    first_name VARCHAR(100),
    second_name VARCHAR(100),
    web_name VARCHAR(100) NOT NULL,
    element_type INTEGER, -- Position: 1=GKP, 2=DEF, 3=MID, 4=FWD
    now_cost INTEGER,     -- Price in 0.1m
    status VARCHAR(5),    -- injuries, etc.
    chance_of_playing_next_round INTEGER,
    chance_of_playing_this_round INTEGER,
    news TEXT,
    -- API-provided metrics
    form NUMERIC,                 -- Official FPL form rating
    points_per_game NUMERIC,      -- PPG
    selected_by_percent NUMERIC,  -- Ownership %
    ict_index NUMERIC,            -- Influence + Creativity + Threat
    influence NUMERIC,
    creativity NUMERIC,
    threat NUMERIC,
    ep_this NUMERIC,              -- Expected points this GW
    ep_next NUMERIC,              -- Expected points next GW
    value_form NUMERIC,           -- Value based on form
    value_season NUMERIC,         -- Value based on season
    total_points INTEGER,         -- Season total points
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Gameweek History Table (Per Player Per Gameweek)
CREATE TABLE IF NOT EXISTS gameweek_history (
    player_id INTEGER REFERENCES players(id),
    gameweek INTEGER NOT NULL,
    opponent_team INTEGER REFERENCES teams(id),
    total_points INTEGER NOT NULL,
    was_home BOOLEAN,
    minutes INTEGER,
    goals_scored INTEGER,
    assists INTEGER,
    clean_sheets INTEGER,
    goals_conceded INTEGER,
    own_goals INTEGER,
    penalties_saved INTEGER,
    penalties_missed INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER,
    saves INTEGER,
    bonus INTEGER,
    bps INTEGER,
    influence NUMERIC,
    creativity NUMERIC,
    threat NUMERIC,
    ict_index NUMERIC,
    value INTEGER,
    transfers_balance INTEGER,
    selected INTEGER,
    transfers_in INTEGER,
    transfers_out INTEGER,
    expected_goals NUMERIC,
    expected_assists NUMERIC,
    expected_goal_involvements NUMERIC,
    expected_goals_conceded NUMERIC,
    PRIMARY KEY (player_id, gameweek)
);

-- 4. Past Season History (from element-summary endpoint)
CREATE TABLE IF NOT EXISTS past_season_history (
    player_id INTEGER REFERENCES players(id),
    season_name VARCHAR(20) NOT NULL,  -- e.g., "2022/23"
    start_cost INTEGER,
    end_cost INTEGER,
    total_points INTEGER,
    minutes INTEGER,
    goals_scored INTEGER,
    assists INTEGER,
    clean_sheets INTEGER,
    goals_conceded INTEGER,
    own_goals INTEGER,
    penalties_saved INTEGER,
    penalties_missed INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER,
    saves INTEGER,
    bonus INTEGER,
    bps INTEGER,
    influence NUMERIC,
    creativity NUMERIC,
    threat NUMERIC,
    ict_index NUMERIC,
    PRIMARY KEY (player_id, season_name)
);

-- 5. Fixtures Table (Upcoming and Past)
CREATE TABLE IF NOT EXISTS fixtures (
    id INTEGER PRIMARY KEY,
    event INTEGER, -- Gameweek (can be null if not scheduled yet)
    team_h INTEGER REFERENCES teams(id),
    team_a INTEGER REFERENCES teams(id),
    team_h_score INTEGER,
    team_a_score INTEGER,
    finished BOOLEAN,
    kickoff_time TIMESTAMP WITH TIME ZONE,
    difficulty_h INTEGER, -- FDR for home team
    difficulty_a INTEGER, -- FDR for away team
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_player_team ON players(team_id);
CREATE INDEX IF NOT EXISTS idx_history_player ON gameweek_history(player_id);
CREATE INDEX IF NOT EXISTS idx_history_gw ON gameweek_history(gameweek);
CREATE INDEX IF NOT EXISTS idx_history_opponent ON gameweek_history(opponent_team);
CREATE INDEX IF NOT EXISTS idx_past_season_player ON past_season_history(player_id);
CREATE INDEX IF NOT EXISTS idx_fixtures_event ON fixtures(event);
CREATE INDEX IF NOT EXISTS idx_fixtures_teams ON fixtures(team_h, team_a);

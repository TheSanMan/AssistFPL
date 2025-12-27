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

-- 2. Players Table
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

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_player_team ON players(team_id);
CREATE INDEX IF NOT EXISTS idx_history_player ON gameweek_history(player_id);
CREATE INDEX IF NOT EXISTS idx_history_gw ON gameweek_history(gameweek);

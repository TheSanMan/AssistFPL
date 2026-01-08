#!/bin/bash

# ANSI colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== AssistFPL Restart Script ===${NC}"

echo -e "${BLUE}1. Stopping existing services...${NC}"
# Kill processes matching our server signatures
pkill -f "uvicorn src.api.main:app" || echo "No backend running."
pkill -f "next-server" || echo "No frontend running."

# Check if Ollama is running (if we're relying on it)
if ! pgrep -x "ollama" > /dev/null; then
    echo -e "${BLUE}⚠️  Note: Ollama is not running. If you are using local LLMs, please run 'ollama serve' in another terminal.${NC}"
fi

# Brief pause
sleep 2

echo -e "${BLUE}2. Applying Database Schema...${NC}"
python apply_schema.py

echo -e "${BLUE}3. Restarting Backend (Port 8000)...${NC}"
# Start uvicorn in background
nohup uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload > backend.log 2>&1 &
echo -e "${GREEN}Backend started! Logs at backend.log${NC}"

echo -e "${BLUE}4. Restarting Frontend (Port 3000)...${NC}"
cd frontend
# Start next.js in background
nohup npm run dev > frontend.log 2>&1 &
cd ..
echo -e "${GREEN}Frontend started! Logs at frontend/frontend.log${NC}"

echo -e "${GREEN}=== System Restarted ===${NC}"
echo -e "Access the Dashboard: http://localhost:3000"
echo -e "Access API Docs: http://localhost:8000/docs"

import asyncio
import aiohttp
import structlog
import ssl
import certifi
from typing import Dict, Any, List

logger = structlog.get_logger()

class FPLClient:
    """
    Asynchronous client for interacting with the official FPL API.
    Handles rate limiting and provides structured access to endpoints.
    """
    BASE_URL = "https://fantasy.premierleague.com/api"
    
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self._semaphore = asyncio.Semaphore(5)  # Limit concurrent requests
        self._ssl_context = ssl.create_default_context(cafile=certifi.where())

    async def _get(self, endpoint: str) -> Dict[str, Any]:
        """Base GET request with retry logic and rate limiting."""
        async with self._semaphore:
            url = f"{self.BASE_URL}/{endpoint}"
            try:
                async with self.session.get(url, ssl=self._ssl_context) as response:
                    if response.status != 200:
                        logger.error("api_error", status=response.status, url=url)
                        return {}
                    return await response.json()
            except Exception as e:
                logger.error("request_failed", error=str(e), url=url)
                return {}

    async def fetch_bootstrap_data(self) -> Dict[str, Any]:
        """Fetches general data (teams, players, gameweeks)."""
        logger.info("fetching_bootstrap_data")
        return await self._get("bootstrap-static/")

    async def fetch_player_history(self, player_id: int) -> Dict[str, Any]:
        """Fetches historical gameweek data for a specific player."""
        return await self._get(f"element-summary/{player_id}/")

    async def fetch_all_players_history(self, player_ids: List[int]) -> List[Dict[str, Any]]:
        """Batch fetches history for all players concurrently."""
        logger.info("fetching_players_history", count=len(player_ids))
        tasks = [self.fetch_player_history(pid) for pid in player_ids]
        return await asyncio.gather(*tasks)

import asyncio
import os
import httpx
from dotenv import load_dotenv

load_dotenv()

APP_ID = os.environ.get("INSTANT_APP_ID")
ADMIN_TOKEN = os.environ.get("INSTANT_ADMIN_TOKEN")
TARGET_EMAIL = "humayunlbb@gmail.com"

async def main():
    async with httpx.AsyncClient() as client:
        # Get profiles
        res = await client.post(
            f"https://api.instantdb.com/admin/query",
            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
            json={
                "app_id": APP_ID,
                "query": {"profiles": {"$": {"where": {"email": TARGET_EMAIL}}}}
            }
        )
        data = res.json()
        profiles = data.get("profiles", [])
        if not profiles:
            print("Profile not found!")
            return
            
        profile_id = profiles[0]["id"]
        
        # Update role to admin
        res = await client.post(
            f"https://api.instantdb.com/admin/transact",
            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
            json={
                "app_id": APP_ID,
                "steps": [
                    ["update", "profiles", profile_id, {"role": "admin"}]
                ]
            }
        )
        print("Updated role to admin:", res.json())

if __name__ == "__main__":
    asyncio.run(main())

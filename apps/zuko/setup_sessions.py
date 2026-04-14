"""
One-time session setup for Zuko's browser scrapers.

Run this once to save SEEK and LinkedIn sessions to data/sessions/.
These are reused on all future scans — no re-login needed unless sessions expire.

Usage:
    cd F:\\TechLearning\\DevOps\\telegram-claude-agent
    python apps/zuko/setup_sessions.py
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


async def main():
    from apps.zuko.scraper import seek_login_interactive, linkedin_login_interactive

    print("\n=== Zuko Session Setup ===\n")
    print("This will open browser windows for you to log in manually.")
    print("Sessions are saved to data/sessions/ and reused automatically.\n")

    setup_seek = input("Set up SEEK session? (y/n): ").strip().lower() == "y"
    if setup_seek:
        print("\n[SEEK] Opening browser...")
        await seek_login_interactive()
        print("[SEEK] Done.\n")

    setup_linkedin = input("Set up LinkedIn session? (y/n): ").strip().lower() == "y"
    if setup_linkedin:
        print("\n[LinkedIn] Opening browser...")
        await linkedin_login_interactive()
        print("[LinkedIn] Done.\n")

    print("Sessions saved to data/sessions/")
    print("Zuko will use these automatically on all future scans.")


if __name__ == "__main__":
    asyncio.run(main())

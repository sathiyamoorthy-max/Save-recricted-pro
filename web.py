# web.py - Render-க்காக மட்டும் (Bot Sleep ஆகாமல் இருக்க)
from flask import Flask
from threading import Thread
import asyncio
import main

app = Flask('')

@app.route('/')
def home():
    return "✅ Bot is running!"

def run_bot():
    # main.py-வில் உள்ள main() function-ஐ Run பண்ணுகிறோம்
    asyncio.run(main.main())

if __name__ == "__main__":
    # Bot-ஐ Background Thread-ல் Start பண்ணு
    t = Thread(target=run_bot)
    t.start()
    # Flask Server-ஐ Start பண்ணு (Port 8080)
    app.run(host='0.0.0.0', port=8080)

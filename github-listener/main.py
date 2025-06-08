from fastapi import FastAPI, Request
from confluent_kafka import Producer
import json

app = FastAPI()

producer = Producer({'bootstrap.servers': 'kafka:9092'})

@app.post("/webhook")
async def github_webhook(request: Request):
    payload = await request.json()

    if payload.get("action") not in ["opened", "synchronize"]:
        return {"message": "Ignored"}

    pr_info = {
        "repo": payload["repository"]["full_name"],
        "pr_number": payload["pull_request"]["number"],
        "author": payload["pull_request"]["user"]["login"],
        "url": payload["pull_request"]["url"]
    }

    producer.produce("new_prs", key=str(pr_info["pr_number"]), value=json.dumps(pr_info))
    producer.flush()

    return {"message": "PR event sent to Kafka"}

from fastapi import FastAPI, HTTPException, Depends, Header
from kafka import KafkaProducer
import json
from pydantic import BaseModel
from typing import Dict
import jwt

app = FastAPI()

# def security_check(token: str):
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
#         return payload
#     except jwt.ExpiredSignatureError:
#         raise HTTPException(status_code=401, detail="Token expired")
#     except jwt.InvalidTokenError:
#         raise HTTPException(status_code=401, detail="Invalid token")
# Mock security check
def security_check(authorization: str = Header(None)):
    if authorization is None or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid or missing Authorization header")
    
    token = authorization.split(" ")[1]
    return {"user_id": "test_user", "role": "admin", "token": token}

class Metadata(BaseModel):
    entity_id: str
    data: Dict[str, str]
    type: str  # PII or standard

# Kafka Producer to send messages to the metadata store
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

@app.post("/ingest_metadata/")
async def ingest_metadata(metadata: Metadata, token: str = Depends(security_check)):
    try:
        # Simulate logging the user info from the token
        print(f"User {token['user_id']} with role {token['role']} is ingesting metadata.")
        
        producer.send('metadata_topic', metadata.dict())
        producer.flush()
        return {"status": "Metadata ingested successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

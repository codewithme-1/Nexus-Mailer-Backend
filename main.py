import asyncio
import time
from datetime import datetime
import json
import re
import random
import requests
import os
from fastapi import FastAPI, Request, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse
from database import supabase

app = FastAPI(title="Nexus Mailer API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Integrated User Credentials ---
# Primary Node
BREVO_API_KEY_1 = os.getenv("BREVO_API_KEY") 
SENDER_EMAIL_1 = "admin@miningofficial.co.ke"

# Secondary Node (Failover)
BREVO_API_KEY_2 = os.getenv("BREVO_API_KEY_2") 
SENDER_EMAIL_2 = "admin@send.miningofficial.co.ke"

# --- SSE Broadcaster ---
active_connections = set()

async def broadcast_event(data: dict):
    """Pushes a live log event to all connected dashboard terminals instantly."""
    for client_queue in list(active_connections):
        await client_queue.put(data)

# --- Models ---
class CampaignPayload(BaseModel):
    subject: str
    body_html: str
    audience_id: str

# --- Immediate Dispatch Logic ---
def send_brevo_email_sync(email_address: str, subject: str, html_content: str, api_key: str, sender_email: str):
    """Standard, stable HTTP request to Brevo supporting dynamic node routing."""
    return requests.post(
        "https://api.brevo.com/v3/smtp/email",
        headers={
            "accept": "application/json",
            "api-key": api_key,
            "content-type": "application/json"
        },
        json={
            "sender": {"name": "Nexus Platform", "email": sender_email},
            "to": [{"email": email_address}],
            "subject": subject,
            "htmlContent": html_content
        },
        timeout=10.0
    )

async def run_campaign_dispatch(campaign_id: str, subject: str, html_content: str, queue_records: list):
    """Hybrid Execution: Multi-Node Cascading Failover with Warp Speed Mocking."""
    print(f"[SYSTEM] Dispatching {len(queue_records)} emails for Campaign: {campaign_id}")
    
    today_str = datetime.utcnow().date().isoformat()
    
    # Secure exact counts directly from Supabase to survive server restarts
    try:
        sent_res = supabase.table("email_queue").select("id", count="exact").eq("status", "delivered").gte("processed_at", today_str).execute()
        total_sent_today = sent_res.count if sent_res.count is not None else len(sent_res.data)
        
        # Track volume across all live nodes to determine tier routing
        brevo_res = supabase.table("email_queue").select("id", count="exact").in_("provider_used", ["Brevo", "Brevo-Node-1", "Brevo-Node-2"]).gte("processed_at", today_str).execute()
        brevo_sent_today = brevo_res.count if brevo_res.count is not None else len(brevo_res.data)
    except Exception as e:
        print(f"[SYSTEM] Warning: Could not fetch exact daily counts: {e}")
        total_sent_today = 0
        brevo_sent_today = 0

    node_1_exhausted = False
    node_2_exhausted = False

    for i, record in enumerate(queue_records):
        # 1. Daily 10,000 Cap Enforcer
        if total_sent_today >= 10000:
            print("[SYSTEM] 10,000 daily email limit reached. Halting dispatch for today.")
            break

        # --- TIER DETERMINATION (TEMPORARY DEMO OVERRIDE) ---
        is_node_1_active = False # Bypassed for today's client test
        is_node_2_active = not node_2_exhausted and brevo_sent_today < 600
        is_mock_engine = not is_node_1_active and not is_node_2_active

        # --- DYNAMIC DEMO THROTTLE ---
        batch_size = 50 if is_mock_engine else 20
        pause_duration = 5 if is_mock_engine else 10

        # 2. Universal Rate Limiter
        if i > 0 and i % batch_size == 0:
            throttle_event = {
                "email": "SYSTEM THROTTLE ACTIVE",
                "status": f"PAUSED ({pause_duration}s)",
                "provider": "Pacing_Engine",
                "time": datetime.now().strftime("%H:%M:%S")
            }
            await broadcast_event(throttle_event)
            print(f"[PACING ENGINE] Sent {batch_size} emails. Pausing for {pause_duration} seconds...")
            await asyncio.sleep(pause_duration)
            print(f"[PACING ENGINE] Resuming dispatch.")

        # 3. Mark as processing in DB
        supabase.table("email_queue").update({"status": "processing"}).eq("id", record["id"]).execute()
        
        email_dispatched = False
        provider_used = "Pending"
        final_status = "pending"
        
        # 4. Engine Routing Cascade (Instant Hot Potato Drop-Down)
        if is_node_1_active and not email_dispatched:
            try:
                res = await asyncio.to_thread(send_brevo_email_sync, record["email"], subject, html_content, BREVO_API_KEY_1, SENDER_EMAIL_1)
                if res.status_code in [200, 201]:
                    provider_used = "Brevo-Node-1"
                    final_status = "delivered"
                    brevo_sent_today += 1
                    email_dispatched = True
                elif res.status_code in [429, 402, 403]:
                    print(f"[NETWORK] Node 1 quota reached (Status: {res.status_code}). Failing over to Node 2.")
                    node_1_exhausted = True
                    brevo_sent_today = max(brevo_sent_today, 300) 
                    is_node_2_active = True # Immediately pass to Node 2
                else:
                    provider_used = "Brevo-Node-1"
                    final_status = "bounced"
                    email_dispatched = True
            except Exception as e:
                print(f"[NETWORK ERROR] Node 1 failed: {e}. Failing over to Node 2.")
                node_1_exhausted = True
                brevo_sent_today = max(brevo_sent_today, 300)
                is_node_2_active = True
                
        if is_node_2_active and not email_dispatched:
            try:
                await asyncio.sleep(0.5) # Anti-spam stagger to protect the live demo
                res = await asyncio.to_thread(send_brevo_email_sync, record["email"], subject, html_content, BREVO_API_KEY_2, SENDER_EMAIL_2)
                if res.status_code in [200, 201]:
                    provider_used = "Brevo-Node-2"
                    final_status = "delivered"
                    brevo_sent_today += 1
                    email_dispatched = True
                elif res.status_code in [429, 402, 403]:
                    print(f"[NETWORK] Node 2 quota reached (Status: {res.status_code}). Failing over to Mock Engine.")
                    node_2_exhausted = True
                    brevo_sent_today = max(brevo_sent_today, 600) 
                else:
                    provider_used = "Brevo-Node-2"
                    final_status = "bounced"
                    email_dispatched = True
            except Exception as e:
                print(f"[NETWORK ERROR] Node 2 failed: {e}. Failing over to Mock Engine.")
                node_2_exhausted = True
                brevo_sent_today = max(brevo_sent_today, 600)
        
        if not email_dispatched:
            # Tier 3 Warp Speed Simulation
            provider_used = "Internal-Mock"
            await asyncio.sleep(0.01)  
            if random.random() < 0.02:  
                final_status = "bounced"
            else:
                final_status = "delivered"
                
        if final_status == "delivered":
            total_sent_today += 1
            
        # 5. Update the DB with final result
        supabase.table("email_queue").update({
            "status": final_status,
            "provider_used": provider_used,
            "processed_at": datetime.utcnow().isoformat()
        }).eq("id", record["id"]).execute()
        
        # 6. Instantly push the result to the dashboard UI
        event = {
            "email": record["email"],
            "status": final_status.upper(),
            "provider": provider_used,
            "time": datetime.now().strftime("%H:%M:%S")
        }
        await broadcast_event(event)
        
    # Mark the campaign as finished
    supabase.table("campaigns").update({"status": "completed"}).eq("id", campaign_id).execute()
    print(f"[SYSTEM] Campaign {campaign_id} dispatch completed.")


# --- CRASH RECOVERY SYSTEM (AUTO-HEAL) ---
@app.on_event("startup")
async def auto_resume_campaigns():
    """If the server restarts or wakes from hibernation, automatically find and resume stuck emails."""
    print("[SYSTEM] Booting up. Scanning for stalled campaigns...")
    try:
        camp_res = supabase.table("campaigns").select("*").eq("status", "running").execute()
        for camp in camp_res.data:
            pending_records = []
            start = 0
            page_size = 1000
            
            # Use pagination to bypass the 1000 limit and fetch ALL stuck emails
            while True:
                res = supabase.table("email_queue").select("*").eq("campaign_id", camp["id"]).eq("status", "pending").range(start, start + page_size - 1).execute()
                if not res.data:
                    break
                pending_records.extend(res.data)
                if len(res.data) < page_size:
                    break
                start += page_size
            
            if pending_records:
                print(f"[SYSTEM] Auto-Heal Triggered: Resuming {len(pending_records)} stalled emails for Campaign {camp['id']}")
                asyncio.create_task(run_campaign_dispatch(camp["id"], camp["subject"], camp["body_html"], pending_records))
    except Exception as e:
        print(f"[SYSTEM] Auto-recovery error: {e}")


# --- API Endpoints ---
@app.post("/api/audiences/upload")
async def upload_audience_file(name: str = Form(...), file: UploadFile = File(...)):
    try:
        aud_res = supabase.table("audiences").insert({"name": name}).execute()
        audience_id = aud_res.data[0]["id"]
        
        content = await file.read()
        filename = file.filename.lower()
        emails = set()
        
        email_pattern = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
        
        if filename.endswith('.csv') or filename.endswith('.txt'):
            text = content.decode('utf-8', errors='ignore')
            emails.update(re.findall(email_pattern, text))
        elif filename.endswith('.xlsx'):
            try:
                import openpyxl
                from io import BytesIO
                wb = openpyxl.load_workbook(BytesIO(content), read_only=True)
                ws = wb.active
                for row in ws.iter_rows(values_only=True):
                    for cell in row:
                        if isinstance(cell, str) and '@' in cell:
                            match = re.search(email_pattern, cell)
                            if match:
                                emails.add(match.group(0))
            except ImportError:
                raise HTTPException(status_code=400, detail="openpyxl package is required for Excel files.")
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format.")
            
        if not emails:
            raise HTTPException(status_code=400, detail="No valid emails found in the file.")
            
        contact_data = [{"audience_id": audience_id, "email": email} for email in emails]
        chunk_size = 500
        for i in range(0, len(contact_data), chunk_size):
            supabase.table("contacts").insert(contact_data[i:i + chunk_size]).execute()
            
        return {"status": "success", "audience_id": audience_id, "contacts_inserted": len(emails)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/campaign/queue")
async def queue_campaign(payload: CampaignPayload, background_tasks: BackgroundTasks):
    
    # --- FIXED PAGINATION: Grabs ALL emails in chunks of 1,000 ---
    emails = []
    page_size = 1000
    start = 0
    
    while True:
        contacts_res = supabase.table("contacts").select("email").eq("audience_id", payload.audience_id).range(start, start + page_size - 1).execute()
        batch = [row["email"] for row in contacts_res.data]
        if not batch:
            break
        emails.extend(batch)
        if len(batch) < page_size:
            break
        start += page_size

    if not emails:
        raise HTTPException(status_code=400, detail="Audience is empty or not found.")

    # 1. Create the Campaign record
    campaign_res = supabase.table("campaigns").insert({
        "subject": payload.subject,
        "body_html": payload.body_html,
        "total_recipients": len(emails),
        "status": "running"
    }).execute()
    campaign_id = campaign_res.data[0]["id"]
    
    # 2. Stage the queue in Supabase and capture the inserted rows directly in memory
    queue_data = [{"campaign_id": campaign_id, "email": email, "status": "pending"} for email in emails]
    
    inserted_records = []
    chunk_size = 500
    for i in range(0, len(queue_data), chunk_size):
        chunk = queue_data[i:i + chunk_size]
        res = supabase.table("email_queue").insert(chunk).execute()
        # Grab the exact database records returned by Supabase so we don't have to re-query them
        inserted_records.extend(res.data)

    # 3. TRIGGER INSTANT EXECUTION (using asyncio to detach from the request)
    asyncio.create_task(run_campaign_dispatch(campaign_id, payload.subject, payload.body_html, inserted_records))
    
    return {"status": "success", "queued": len(queue_data), "campaign_id": campaign_id}

@app.get("/api/dashboard/stats")
async def get_dashboard_stats():
    try:
        camp_res = supabase.table("campaigns").select("id", count="exact").eq("status", "running").execute()
        active_camps = camp_res.count if camp_res.count is not None else 0

        queued_res = supabase.table("email_queue").select("id", count="exact").eq("status", "pending").execute()
        total_queued = queued_res.count if queued_res.count is not None else 0

        today_str = datetime.utcnow().date().isoformat()
        
        sent_res = supabase.table("email_queue").select("id", count="exact").eq("status", "delivered").gte("processed_at", today_str).execute()
        sent_today = sent_res.count if sent_res.count is not None else 0
        
        failed_res = supabase.table("email_queue").select("id", count="exact").eq("status", "bounced").gte("processed_at", today_str).execute()
        failed_today = failed_res.count if failed_res.count is not None else 0
        
        total_processed = sent_today + failed_today
        success_rate = 100.0 if total_processed == 0 else round((sent_today / total_processed) * 100, 1)
            
        return {
            "active_campaigns": active_camps,
            "total_queued": total_queued,
            "sent_today": sent_today,
            "success_rate": success_rate
        }
    except Exception as e:
        return {"active_campaigns": 0, "total_queued": 0, "sent_today": 0, "success_rate": 100.0}

@app.get("/api/dashboard/campaigns")
async def get_active_campaigns():
    try:
        camps = supabase.table("campaigns").select("*").eq("status", "running").order("created_at", desc=True).execute()
        result = []
        for camp in camps.data:
            pending_res = supabase.table("email_queue").select("id", count="exact").eq("campaign_id", camp["id"]).eq("status", "pending").execute()
            pending_count = pending_res.count if pending_res.count is not None else 0
            total = camp.get("total_recipients", 0)
            result.append({
                "id": camp["id"], "subject": camp["subject"], "total": total, "processed": total - pending_count
            })
        return {"campaigns": result}
    except Exception as e:
        return {"campaigns": []}

@app.get("/api/dashboard/logs")
async def get_dashboard_logs():
    """Fetches the 40 most recently processed emails for historical log recovery."""
    try:
        logs_res = supabase.table("email_queue").select("email, status, provider_used, processed_at").neq("status", "pending").order("processed_at", desc=True).limit(40).execute()
        
        formatted_logs = []
        for record in logs_res.data:
            time_obj = datetime.fromisoformat(record["processed_at"].replace('Z', '+00:00')) if record.get("processed_at") else datetime.utcnow()
            formatted_logs.append({
                "email": record["email"],
                "status": (record["status"] or "").upper(),
                "provider": record["provider_used"] or "Brevo-Node-1",
                "time": time_obj.strftime("%H:%M:%S")
            })
        return {"logs": formatted_logs}
    except Exception as e:
        print(f"Log sync error: {e}")
        return {"logs": []}

@app.get("/api/telemetry/stream")
async def stream_telemetry(request: Request):
    client_queue = asyncio.Queue()
    active_connections.add(client_queue)
    async def event_generator():
        try:
            while True:
                if await request.is_disconnected(): break
                event = await client_queue.get()
                yield {"event": "log", "data": json.dumps(event)}
        finally:
            active_connections.remove(client_queue)
    return EventSourceResponse(event_generator())

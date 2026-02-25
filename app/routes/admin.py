import os
import json
from fastapi import APIRouter, Request, Form, Depends, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from app.core.retriever import RetrieverEngine

router = APIRouter()
templates = Jinja2Templates(directory="templates")

ADMIN_SECRET = os.getenv("ADMIN_SECRET", "default_insecure")

def verify_admin(key: str):
    if key != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Invalid Admin Key")
    return True

@router.get("/admin", response_class=HTMLResponse)
def admin_page(request: Request, key: str = ""):
    return templates.TemplateResponse("admin.html", {"request": request, "key": key})

@router.post("/admin/submit")
def admin_submit(
    request: Request, 
    key: str = Form(...), 
    json_data: str = Form(...)
):
    if key != ADMIN_SECRET:
        return templates.TemplateResponse("admin.html", {
            "request": request, 
            "key": key, 
            "error": "Invalid Secret Key"
        })

    try:
        data = json.loads(json_data)
        client_id = data.get("client_id")
        pages = data.get("pages")

        if not client_id or not pages:
            raise ValueError("JSON must contain 'client_id' and 'pages'")

        engine = RetrieverEngine(client_id)
        count = engine.sync_content(pages)

        return templates.TemplateResponse("admin.html", {
            "request": request, 
            "key": key, 
            "success": f"Successfully indexed. Pages updated: {count}"
        })

    except Exception as e:
         return templates.TemplateResponse("admin.html", {
            "request": request, 
            "key": key, 
            "error": f"Error: {str(e)}"
        })
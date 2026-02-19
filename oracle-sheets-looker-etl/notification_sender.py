import requests
import json

# ==========================================
# 1. WIDGETS & CONFIGURATION
# ==========================================
dbutils.widgets.text("webhook_url", "https://chat.googleapis.com/v1/spaces/...", "1. Google Chat Webhook")
dbutils.widgets.text("previous_task_name", "ETL_Task", "3. Previous Task Name")

CHAT_WEBHOOK_URL = dbutils.widgets.get("webhook_url")
PREVIOUS_TASK_KEY = dbutils.widgets.get("previous_task_name")

# -- URLs from your request --
DASHBOARD_URL = "https://lookerstudio.google.com/u/0/reporting/..."
MANUAL_SHEET_URL = "https://docs.google.com/spreadsheets/d/..."

# ==========================================
# 2. FUNCTION DEFINITION
# ==========================================
def send_card(status, rows, time_str, error_msg=None):
    """
    Sends a Card V2 to Google Chat with a horizontal layout for success.
    """
    is_success = status == "SUCCESS"
    
    # 1. Define Header Info
    header_title = "LUU_DG_Stock_Monitor"
    header_subtitle = "✅ Updated successfully" if is_success else "❌ Update Failed"
    header_icon = "https://fonts.gstatic.com/s/i/short_term/release/googlesymbols/check_circle/default/24px.svg" if is_success else "https://fonts.gstatic.com/s/i/short_term/release/googlesymbols/warning/default/24px.svg"

    sections = []

    if is_success:
        # --- SUCCESS LAYOUT (HORIZONTAL) ---
        # Format rows with comma (e.g., 4,578)
        formatted_rows = "{:,}".format(int(rows))
        
        sections.append({
            "widgets": [
                {
                    "columns": {
                        "columnItems": [
                            {
                                "horizontalAlignment": "START",
                                "widgets": [
                                    {
                                        "decoratedText": {
                                            "startIcon": {"knownIcon": "CLOCK"},
                                            "topLabel": "Run Time (Berlin)",
                                            "text": str(time_str)
                                        }
                                    }
                                ]
                            },
                            {
                                "horizontalAlignment": "START",
                                "widgets": [
                                    {
                                        "decoratedText": {
                                            "startIcon": {"knownIcon": "DESCRIPTION"},
                                            "topLabel": "Rows Processed",
                                            "text": formatted_rows
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                },
                {
                    "buttonList": {
                        "buttons": [
                            {
                                "text": "OPEN DASHBOARD 📊",
                                "color": {"red": 0, "green": 0, "blue": 1, "alpha": 1},
                                "onClick": {"openLink": {"url": DASHBOARD_URL}}
                            }
                        ]
                    }
                }
            ]
        })
        
    else:
        # --- FAILURE LAYOUT ---
        sections.append({
            "widgets": [
                {
                    "textParagraph": {
                        "text": "<b>Action Required:</b><br>Please go ahead with the standard import process with infosystem." 
                    }
                },
                {
                    "buttonList": {
                        "buttons": [
                            {
                                "text": "OPEN MANUAL SHEET 📝",
                                "onClick": {"openLink": {"url": MANUAL_SHEET_URL}}
                            }
                        ]
                    }
                }
            ]
        })

    # 2. Construct Final Payload (Card V2)
    card_payload = {
        "cardsV2": [
            {
                "cardId": "stock-monitor-card",
                "card": {
                    "header": {
                        "title": header_title,
                        "subtitle": header_subtitle,
                        "imageUrl": header_icon,
                        "imageType": "CIRCLE"
                    },
                    "sections": sections
                }
            }
        ]
    }

    try:
        requests.post(CHAT_WEBHOOK_URL, json=card_payload)
        print("📨 Notification Card Sent.")
    except Exception as e:
        print(f"⚠️ Failed to send notification: {e}")

# ==========================================
# 3. MAIN EXECUTION
# ==========================================
try:
    print(f"🔄 Fetching results from task: '{PREVIOUS_TASK_KEY}'...")
    
    # Fetching values saved by the ETL Notebook
    # Note: These default values are for testing if the previous task hasn't run
    status = dbutils.jobs.taskValues.get(taskKey=PREVIOUS_TASK_KEY, key="status", default="FAILURE")
    rows = dbutils.jobs.taskValues.get(taskKey=PREVIOUS_TASK_KEY, key="rows", default=0)
    time_str = dbutils.jobs.taskValues.get(taskKey=PREVIOUS_TASK_KEY, key="run_time", default="Unknown Time")
    error_msg = dbutils.jobs.taskValues.get(taskKey=PREVIOUS_TASK_KEY, key="error_msg", default="Unknown Error")

    print(f"📥 Received Context: Status={status}, Rows={rows}")

    # Send Notification
    # Pass arguments based on status
    send_card(status=status, rows=rows, time_str=time_str, error_msg=error_msg)

except Exception as e:
    print(f"❌ Wrapper Error: {e}")
    print("ℹ️ NOTE: dbutils.jobs.taskValues only works when running as a real Job, not interactively.")

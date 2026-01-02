"""
Enhanced Telegram Bot - Inventory Management System v3.0
- Daily file uploads with tracking
- Image storage with auto-generated references
- Database archiving
- Gemini AI natural language queries
"""
import os
import socket
import httpx
#all working .
# Force IPv4 and disable IPv6 issues
os.environ["HTTPX_FORCE_IPV4"] = "true"
os.environ["NO_PROXY"] = "api.telegram.org"
# Set DNS timeout
socket.setdefaulttimeout(30)

# Configure httpx with better defaults
httpx._config.DEFAULT_CONNECT_TIMEOUT = 60
httpx._config.DEFAULT_READ_TIMEOUT = 60
httpx._config.DEFAULT_WRITE_TIMEOUT = 60
httpx._config.DEFAULT_POOL_TIMEOUT = 60

from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, File
from telegram.request import HTTPXRequest
from telegram.error import TimedOut, NetworkError
import duckdb
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
import tempfile
import google.generativeai as genai
from tabulate import tabulate
import hashlib
import base64
import io
from PIL import Image
import uuid
import time
import sys
import asyncio

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Configure Genai client with timeout and connection pooling
# Use HTTPXRequest internally with proper timeout settings
genai_client_config = {
    "api_key": GEMINI_API_KEY,
    # Configure HTTP transport with timeouts
}

try:
    # Create Genai client - it will use httpx internally
    client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    print(f"Warning: Genai client creation: {e}")
    client = genai.Client(api_key=GEMINI_API_KEY)

if not GEMINI_API_KEY:
    raise RuntimeError("❌ GEMINI_API_KEY not found in .env")
if not BOT_TOKEN:
    raise RuntimeError("❌ TELEGRAM_BOT_TOKEN not found in .env")

GEMINI_MODEL = "gemini-2.5-pro"
BOT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(BOT_DIR)
DB_PATH = os.path.join(PARENT_DIR, "product_data.db")
MAX_MSG_LEN = 3800

def convert_numpy_types(df):
    """Convert numpy types to Python native types for DuckDB compatibility"""
    import numpy as np
    for col in df.columns:
        if df[col].dtype == 'object':
            continue
        elif df[col].dtype == 'int64':
            df[col] = df[col].astype('Int64')  # Nullable integer
        elif df[col].dtype == 'float64':
            df[col] = df[col].astype('Float64')  # Nullable float
        elif df[col].dtype == 'int32':
            df[col] = df[col].astype('Int32')
        elif df[col].dtype == 'float32':
            df[col] = df[col].astype('Float32')
    return df

# File to table mapping with column transformations
FILE_MAPPINGS = {
    "Ready Stock Detail.xlsx": {
        "table": "inventory_snapshots",
        "skiprows": 1,
        "column_mapping": {
            "ITEM": "production_code",
            "COLOR": "color",
            "PCS": "ready_pcs",
            "QTY": "bal_pcs",
            "ITEM GRADE": "grade",
            "STOCK TYPE": "stock_type"
        },
        "add_columns": {}
    },
    "Sales Order Detail.xlsx": {
        "table": "sale_order",
        "skiprows": 1,
        "column_mapping": {
            "ORDER NO": "order_no",
            "DATE": "order_date",
            "CUSTOMER": "customer",
            "GST NO": "gst_no",
            "BROKER": "broker",
            "CITY NAME": "city_name",
            "PRODUCT": "product",
            "COLOR": "color",
            "CATALOG": "catalog",
            "ORDER PCS": "order_pcs",
            "DISP PCS": "disp_pcs",
            "BAL PCS": "bal_pcs",
            "AMOUNT": "amount",
            "RATE": "rate",
            "PACKING": "packing",
            "VOL": "product_volume",
            "Over Due Days": "over_due_days",
            "DUE DAYS": "due_days",
            "SALE MAN": "sale_man"
        },
        "add_columns": {}
    },
    "SALES CHALLAN DETAILS REPORT.xlsx": {
        "table": "sale_challan",
        "skiprows": 1,
        "column_mapping": {
            "ORDER NO": "order_no",
            "DATE": "challan_date",
            "CHALAN NO": "challan_no",
            "CUSTOMER": "customer_name",
            "BROKER": "broker",
            "CITY": "city",
            "CATALOG": "catalog",
            "ITEM/DESIGN": "item_design",
            "TRANSPORT": "transport",
            "NO OF PARCEL": "no_of_parcel",
            "LR NO": "lr_no",
            "LR DATE": "lr_date",
            "PCS": "pcs",
            "RATE": "rate",
            "DISC PER": "disc_per",
            "NET AMT": "net_amt"
        },
        "add_columns": {}
    },
    "Sales Return Inward Details.xlsx": {
        "table": "stock_inward",
        "skiprows": 1,
        "column_mapping": {
            "VOUCHER NO": "voucher_no",
            "INWARD DATE": "inward_date",
            "CUSTOMER ": "customer",
            "BROKER ": "broker",
            "TRANSPORT ": "transport",
            "PRODUCTION CODE": "production_code",
            "ITEM NAME": "item_name",
            "BRAND ": "brand",
            "SERIES ": "series",
            "GRADE": "grade",
            "UNIT ": "unit",
            "PCS": "pcs",
            "CUT": "cut",
            "MTS": "mts",
            "RATE": "rate",
            "AMOUNT": "amount",
            "CATALOG ": "catalog"
        },
        "add_columns": {}
    }
}

SCHEMA_CONTEXT = '''
DATABASE SCHEMA - Inventory System (DuckDB):

1. products - Master product catalog
   Columns: id, production_code, product_name, color, grade, mrp_price, spl_price, trd_price, created_at, updated_at

2. inventory_snapshots - Daily stock snapshots
   Columns: id, production_code, item_name, color, snapshot_date, ready_pcs, sales_pcs, bal_pcs, stock_type, grade, cut, qty, rate, amount, upload_id, created_at
   Key searches: "current stock", "ready pcs", "stock status", "balance pcs"

3. sale_order - Customer sales orders
   Columns: id, order_no, order_date, customer, gst_no, broker, city_name, product, color, catalog, order_pcs, disp_pcs, bal_pcs, amount, rate, spl_price, trd_price, mrp_price, grade, packing, product_volume, over_due_days, due_days, sale_man, upload_id, created_at, updated_at
   Key searches: "sale order", "customer orders", "order details", "pending orders"

4. sale_challan - Sales delivery documents  
   Columns: id, challan_date, challan_no, order_no, customer_name, broker, city, catalog, item_design, transport, no_of_parcel, lr_no, lr_date, pcs, rate, disc_per, net_amt, upload_id, created_at
   Key searches: "challan", "delivery", "shipment", "sales challan"

5. stock_inward - Stock returns and inwards
   Columns: id, voucher_no, inward_date, customer, broker, city, item_name, production_code, color, pcs, rate, qty, amount, dc_no, dc_date, transport, lr_no, lr_date, upload_id, created_at
   Key searches: "inward", "return", "stock inward", "return orders"

6. daily_uploads - Upload tracking
   Columns: id, upload_date, file_name, table_name, rows_inserted, rows_updated, rows_failed, processing_status, error_message, uploaded_by, created_at
   Key searches: "uploads", "processing status", "file uploads", "daily uploads"

7. image_storage - Stored images with metadata
   Columns: id, image_name, image_data, reference_text, category, is_auto_generated, created_by, file_size_bytes, tags, related_id, mime_type, created_at
   Key searches: "images", "stored images", "image reference"

8. archive_snapshots - End-of-day archive
   Columns: id, archive_date, production_code, color, ready_pcs, sales_pcs, bal_pcs, created_at
   Key searches: "archive", "historical", "daily archive"

IMPORTANT:
- Use exact table and column names from schema above
- For "stock", "inventory", or "stock status" queries → use inventory_snapshots table
- For "orders" queries → use sale_order table  
- For "challan", "delivery", "shipment" → use sale_challan table
- For "customer name" searches → check customer column in sale_order or customer_name in sale_challan
- Always include date filters when querying for "today" or specific periods
- Use ILIKE for case-insensitive text searches
'''

def get_connection():
    """Get database connection"""
    return duckdb.connect(DB_PATH)

def read_excel_with_smart_headers(filepath, sheet_name=0, skiprows=0):
    """Read Excel file with intelligent header detection"""
    df = pd.read_excel(filepath, sheet_name=sheet_name, skiprows=skiprows, header=0)
    return df

async def send_long_message(update: Update, text: str, parse_mode: str = None):
    """Split long messages for Telegram (max 3800 chars)"""
    chat = update.effective_chat
    if not chat or not text:
        return
    for i in range(0, len(text), MAX_MSG_LEN):
        await chat.send_message(text[i:i + MAX_MSG_LEN], parse_mode=parse_mode)

# ==================== IMAGE STORAGE FUNCTIONS ====================
def format_df_for_telegram(df: pd.DataFrame, max_rows: int = 10) -> str:
    df = df.copy()

    for col in df.columns:
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    for col in df.select_dtypes(include=["float", "int"]).columns:
        df[col] = df[col].apply(lambda x: f"{x:,}" if pd.notna(x) else "")

    df = df.fillna("")
    show_df = df.head(max_rows)

    col_widths = {
        col: max(len(col), show_df[col].astype(str).map(len).max())
        for col in show_df.columns
    }

    sep = "│"
    line = "─"

    top = "┌" + "┬".join(line * (col_widths[c] + 2) for c in show_df.columns) + "┐"
    header = sep + sep.join(f" {c:<{col_widths[c]}} " for c in show_df.columns) + sep
    divider = "├" + "┼".join(line * (col_widths[c] + 2) for c in show_df.columns) + "┤"
    bottom = "└" + "┴".join(line * (col_widths[c] + 2) for c in show_df.columns) + "┘"

    rows = []
    for _, row in show_df.iterrows():
        rows.append(
            sep + sep.join(
                f" {str(row[c]):<{col_widths[c]}} " for c in show_df.columns
            ) + sep
        )

    table = "\n".join([top, header, divider] + rows + [bottom])

    if len(df) > max_rows:
        table += f"\n… {len(df) - max_rows} more rows"

    return table
def format_records_one_by_one(df: pd.DataFrame) -> list[str]:
    messages = []

    df = df.fillna("")

    # Format numbers nicely
    for col in df.select_dtypes(include=["int", "float"]).columns:
        df[col] = df[col].apply(lambda x: f"{int(x):,}" if str(x).isdigit() else x)

    for idx, row in df.iterrows():
        lines = []
        for col in df.columns:
            key = col.upper().replace("_", " ")
            val = row[col]
            lines.append(f"{key:<15}: {val}")

        block = "📄 RECORD\n\n" + "\n".join(lines)
        messages.append(block)

    return messages



def store_image(image_bytes, reference_text, created_by, category=None, is_auto_generated=False, related_id=None, tags=None):
    """
    Store image in database with reference text
    Returns image_id or None on failure
    """
    try:
        image_name = f"{uuid.uuid4().hex}.jpg"
        file_size = len(image_bytes)
        
        con = get_connection()
        result = con.execute("""
            INSERT INTO image_storage 
            (image_name, image_data, reference_text, category, is_auto_generated, created_by, file_size_bytes, tags, related_id, mime_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
        """, [image_name, image_bytes, reference_text, category or "general", is_auto_generated, created_by, file_size, tags or "", related_id, "image/jpeg"])
        
        image_id = result.fetchone()[0]
        con.close()
        return image_id
    except Exception as e:
        print(f"❌ Error storing image: {e}")
        return None

def get_images_by_reference(reference_text):
    """Retrieve all images for a reference"""
    try:
        con = get_connection()
        result = con.execute("""
            SELECT id, image_name, reference_text, category, created_at 
            FROM image_storage 
            WHERE reference_text ILIKE ?
            ORDER BY created_at DESC
        """, [f"%{reference_text}%"]).fetchall()
        con.close()
        return result
    except Exception as e:
        print(f"❌ Error retrieving images: {e}")
        return []

def generate_image_reference(related_id, table_name, upload_date=None):
    """Auto-generate reference text for images"""
    if upload_date is None:
        upload_date = datetime.now().strftime("%Y-%m-%d")
    return f"{table_name}_{related_id}_{upload_date}"
def generate_batch_id() -> str:
    return f"IMG_{int(time.time())}"


MEDIA_GROUP_BUFFER = {}

async def finalize_media_group(group_id, update):
    """Finalize and send confirmation for a media group"""
    if group_id not in MEDIA_GROUP_BUFFER:
        return
    
    batch_data = MEDIA_GROUP_BUFFER[group_id]
    batch_id = batch_data["batch_id"]
    image_count = len(batch_data["images"])
    
    try:
        await send_long_message(
            update,
            f"✅ Album uploaded successfully!\n"
            f"📦 {image_count} image(s) stored\n"
            f"🆔 Reference ID: {batch_id}\n"
            f"🔍 Search tips:\n"
            f"   • /listimages → Find all images\n"
            f"   • /getimage {batch_id} → Get by batch name\n"
            f"   • Use: /getimage {batch_id.split()[0]} → Search by keyword"
        )
    except Exception as e:
        print(f"Error finalizing media group: {e}")
    
    if group_id in MEDIA_GROUP_BUFFER:
        del MEDIA_GROUP_BUFFER[group_id]

async def handle_image_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.photo:
        return

    group_id = update.message.media_group_id

    # 🟢 Single image (not album)
    if not group_id:
        caption = update.message.caption

        if caption:
            batch_id = caption.strip().lower()
        else:
            batch_id = generate_batch_id()

        best_photo = update.message.photo[-1]
        photo_file = await best_photo.get_file()
        image_bytes = await photo_file.download_as_bytearray()

        store_image(
            image_bytes=bytes(image_bytes),
            reference_text=batch_id,
            created_by="telegram_bot",
            category="image_batch"
        )

        await send_long_message(update, f"✅ Single image stored\n🆔 Reference: {batch_id}\n💡 Search by: {batch_id}")
        return

    # 🟡 Album (multiple images with same media_group_id)
    # Initialize batch on first image of group
    if group_id not in MEDIA_GROUP_BUFFER:
        caption = update.message.caption

        if caption:
            batch_id = caption.strip().lower()
        else:
            batch_id = generate_batch_id()

        MEDIA_GROUP_BUFFER[group_id] = {
            "batch_id": batch_id,
            "images": [],
            "last_update": time.time(),
            "finalized": False
        }
        
        # Schedule finalization check after 3 seconds
        context.application.create_task(
            schedule_album_finalization(group_id, update, context)
        )

    # Store current image with batch_id as reference
    best_photo = update.message.photo[-1]
    photo_file = await best_photo.get_file()
    image_bytes = await photo_file.download_as_bytearray()

    store_image(
        image_bytes=bytes(image_bytes),
        reference_text=MEDIA_GROUP_BUFFER[group_id]["batch_id"],
        created_by="telegram_bot",
        category="image_batch"
    )

    MEDIA_GROUP_BUFFER[group_id]["images"].append(image_bytes)
    MEDIA_GROUP_BUFFER[group_id]["last_update"] = time.time()

async def schedule_album_finalization(group_id, update, context):
    """Schedule checking and finalizing album after allowing time for all images"""
    await asyncio.sleep(3)  # Wait 3 seconds for all album images to arrive
    
    if group_id in MEDIA_GROUP_BUFFER and not MEDIA_GROUP_BUFFER[group_id]["finalized"]:
        MEDIA_GROUP_BUFFER[group_id]["finalized"] = True
        await finalize_media_group(group_id, update)

def get_images_by_image_id(image_id: str):
    con = get_connection()
    rows = con.execute("""
        SELECT image_data
        FROM image_storage
        WHERE reference_text = ?
        ORDER BY id
    """, [image_id]).fetchall()
    con.close()
    return rows
def delete_images_by_image_id(image_id: str) -> int:
    con = get_connection()
    result = con.execute("""
        DELETE FROM image_storage
        WHERE reference_text = ?
    """, [image_id])
    deleted = result.rowcount
    con.close()
    return deleted

# ==================== DAILY UPLOAD TRACKING ====================

async def log_upload(filename, table_name, rows_inserted, rows_updated=0, rows_failed=0, status="SUCCESS", error_msg=None, uploaded_by="telegram"):
    """Log file upload to daily_uploads table"""
    try:
        con = get_connection()
        con.execute("""
            INSERT INTO daily_uploads 
            (upload_date, file_name, table_name, rows_inserted, rows_updated, rows_failed, processing_status, error_message, uploaded_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            datetime.now().strftime("%Y-%m-%d"),
            filename,
            table_name,
            rows_inserted,
            rows_updated,
            rows_failed,
            status,
            error_msg,
            uploaded_by
        ])
        con.close()
        return True
    except Exception as e:
        print(f"❌ Error logging upload: {e}")
        return False

async def archive_daily_snapshot():
    """Create daily archive snapshot at end of day"""
    try:
        con = get_connection()
        
        # Get today's snapshots and archive them
        con.execute("""
            INSERT INTO archive_snapshots (archive_date, production_code, color, ready_pcs, sales_pcs, bal_pcs)
            SELECT CURRENT_DATE, production_code, color, ready_pcs, sales_pcs, bal_pcs
            FROM inventory_snapshots
            WHERE snapshot_date = CURRENT_DATE
            ON CONFLICT (archive_date, production_code, color) DO UPDATE SET
                ready_pcs = EXCLUDED.ready_pcs,
                sales_pcs = EXCLUDED.sales_pcs,
                bal_pcs = EXCLUDED.bal_pcs
        """)
        
        con.close()
        return True
    except Exception as e:
        print(f"❌ Error archiving snapshot: {e}")
        return False
async def delete_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await send_long_message(update, "❌ Usage: /deleteimage <IMAGE_ID>")
        return

    image_id = context.args[0]

    deleted = delete_images_by_image_id(image_id)

    if deleted == 0:
        await send_long_message(update, f"❌ No images found for ID: {image_id}")
        return

    await send_long_message(
        update,
        f"🗑️ Deleted {deleted} image(s)\n🆔 Image ID: {image_id}"
    )


# ==================== FILE UPLOAD HANDLER ====================

def extract_and_populate_products(filename, df_mapped, mapping):
    """Extract unique products from uploaded data and populate products master table"""
    try:
        con = get_connection()
        
        # Different extraction logic based on file type
        products_to_insert = []
        
        if filename == "Ready Stock Detail.xlsx":
            # Extract from inventory file: production_code, color, grade
            for _, row in df_mapped.iterrows():
                if pd.notna(row.get('production_code')):
                    products_to_insert.append({
                        'production_code': str(row['production_code']),
                        'color': str(row.get('color', '')),
                        'grade': str(row.get('grade', '')),
                        'product_name': str(row.get('production_code', '')),
                    })
        
        elif filename == "Sales Order Detail.xlsx":
            # Extract from sales order: product, color, grade, prices
            for _, row in df_mapped.iterrows():
                if pd.notna(row.get('product')):
                    products_to_insert.append({
                        'production_code': str(row['product']),
                        'color': str(row.get('color', '')),
                        'grade': str(row.get('grade', '')),
                        'product_name': str(row.get('product', '')),
                        'mrp_price': float(row.get('mrp_price', 0)) if pd.notna(row.get('mrp_price')) else 0,
                        'trd_price': float(row.get('trd_price', 0)) if pd.notna(row.get('trd_price')) else 0,
                        'spl_price': float(row.get('spl_price', 0)) if pd.notna(row.get('spl_price')) else 0,
                    })
        
        elif filename == "SALES CHALLAN DETAILS REPORT.xlsx":
            # Extract from challan: item_design, catalog
            for _, row in df_mapped.iterrows():
                if pd.notna(row.get('item_design')):
                    products_to_insert.append({
                        'production_code': str(row.get('catalog', row['item_design'])),
                        'product_name': str(row['item_design']),
                        'color': '',
                    })
        
        elif filename == "Sales Return Inward Details.xlsx":
            # Extract from inward: production_code, item_name, grade, color
            for _, row in df_mapped.iterrows():
                if pd.notna(row.get('production_code')):
                    products_to_insert.append({
                        'production_code': str(row['production_code']),
                        'product_name': str(row.get('item_name', row['production_code'])),
                        'color': str(row.get('color', '')),
                        'grade': str(row.get('grade', '')),
                    })
        
        # De-duplicate products
        unique_products = {}
        for prod in products_to_insert:
            key = (prod['production_code'], prod.get('color', ''))
            if key not in unique_products:
                unique_products[key] = prod
        
        # Insert unique products
        inserted_count = 0
        for prod in unique_products.values():
            try:
                con.execute("""
                    INSERT OR IGNORE INTO products 
                    (production_code, product_name, color, grade, mrp_price, spl_price, trd_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, [
                    prod.get('production_code', ''),
                    prod.get('product_name', ''),
                    prod.get('color', ''),
                    prod.get('grade', ''),
                    prod.get('mrp_price', 0),
                    prod.get('spl_price', 0),
                    prod.get('trd_price', 0),
                ])
                inserted_count += 1
            except Exception as e:
                print(f"Error inserting product {prod}: {e}")
        
        con.close()
        return inserted_count
    
    except Exception as e:
        print(f"❌ Error extracting products: {e}")
        return 0
def auto_select_columns(df: pd.DataFrame, sql: str) -> pd.DataFrame:
    sql_lower = sql.lower()

    TABLE_COLUMNS = {
        "sale_order": [
            "order_no", "order_date", "customer", "city_name",
            "product", "color",
            "order_pcs", "disp_pcs", "bal_pcs", "amount"
        ],
        "inventory_snapshots": [
            "production_code", "color",
            "ready_pcs", "bal_pcs", "snapshot_date"
        ],
        "sale_challan": [
            "challan_no", "challan_date", "customer_name",
            "item_design", "color", "pcs", "rate", "amount"
        ],
        "stock_inward": [
            "voucher_no", "inward_date",
            "production_code", "item_name", "color",
            "pcs", "rate", "amount"
        ]
    }

    # Detect table used in SQL
    detected_table = None
    for table in TABLE_COLUMNS:
        if table in sql_lower:
            detected_table = table
            break

    if not detected_table:
        return df  # fallback → show all

    wanted = TABLE_COLUMNS[detected_table]

    # Keep only existing columns
    available = [c for c in wanted if c in df.columns]

    # Safety fallback
    if len(available) < 3:
        return df

    return df[available]


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Excel file uploads with daily tracking"""
    if not update.message.document:
        await send_long_message(update, "❌ No file detected")
        return
    
    file = update.message.document
    filename = file.file_name
    
    if not filename.endswith(('.xlsx', '.xls')):
        await send_long_message(update, "❌ Please send an Excel file (.xlsx or .xls)")
        return
    
    await send_long_message(update, f"📥 Processing {filename}...")
    
    temp_path = None
    try:
        # Download file
        file_obj = await file.get_file()
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
            temp_path = tmp_file.name
            await file_obj.download_to_memory(out=tmp_file)
        
        # Check if file is mapped
        if filename not in FILE_MAPPINGS:
            await send_long_message(update, f"⚠️ File '{filename}' not recognized. Supported files:\n" + 
                                  "\n".join(FILE_MAPPINGS.keys()))
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)
            return
        
        mapping = FILE_MAPPINGS[filename]
        table_name = mapping["table"]
        column_mapping = mapping["column_mapping"]
        skiprows = mapping.get("skiprows", 0)
        
        # Read Excel file
        df = read_excel_with_smart_headers(temp_path, sheet_name=0, skiprows=skiprows)
        
        if df.empty:
            await send_long_message(update, f"⚠️ File is empty")
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)
            await log_upload(filename, table_name, 0, 0, 0, "FAILED", "Empty file")
            return
        
        # Map columns
        df_mapped = pd.DataFrame()
        missing_cols = []
        
        for src_col, dst_col in column_mapping.items():
            if src_col in df.columns:
                df_mapped[dst_col] = df[src_col]
            else:
                missing_cols.append(src_col)
        
        if missing_cols:
            await send_long_message(update, f"⚠️ Missing columns: {', '.join(missing_cols)}")
        
        # Add computed columns
        add_columns = mapping.get("add_columns", {})
        for col_name, col_func in add_columns.items():
            if callable(col_func):
                df_mapped[col_name] = col_func(df_mapped)
            else:
                df_mapped[col_name] = col_func
        
        # Remove rows with all NaN
        df_mapped = df_mapped.dropna(how='all')
        
        # Convert numpy types to Python native types
        df_mapped = convert_numpy_types(df_mapped)
        
        rows_count = len(df_mapped)
        if rows_count == 0:
            await send_long_message(update, f"⚠️ No valid data to insert")
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)
            await log_upload(filename, table_name, 0, 0, 0, "FAILED", "No valid data")
            return
        
        # Insert into database
        con = get_connection()
        try:
            # Build INSERT statement with only the mapped columns
            cols_str = ", ".join(df_mapped.columns)
            placeholders = ", ".join(["?"] * len(df_mapped.columns))
            insert_sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"
            
            # Insert row by row
            inserted_rows = 0
            failed_rows = 0
            for idx, row in df_mapped.iterrows():
                try:
                    con.execute(insert_sql, row.values.tolist())
                    inserted_rows += 1
                except Exception as row_error:
                    failed_rows += 1
                    if idx < 5:  # Log first 5 errors
                        print(f"Row {idx} error: {str(row_error)[:100]}")
            
            con.commit()
            con.close()
            
            await send_long_message(update, f"✅ Successfully inserted {inserted_rows} rows into '{table_name}' (Failed: {failed_rows})")
            
            # Extract and populate products master table
            products_extracted = extract_and_populate_products(filename, df_mapped, mapping)
            if products_extracted > 0:
                await send_long_message(update, f"📦 Extracted and added {products_extracted} unique product(s) to master catalog")
            
            # Log upload
            await log_upload(filename, table_name, inserted_rows, 0, failed_rows, "SUCCESS" if failed_rows == 0 else "PARTIAL", None)
            
            # Show preview
            preview = df_mapped.head(3).to_string()
            await send_long_message(update, f"📋 Preview (first 3 rows):\n```\n{preview}\n```")
            
            # Auto-archive if stock data
            if table_name == "inventory_snapshots":
                await archive_daily_snapshot()
                await send_long_message(update, "📦 Daily snapshot archived!")
            
        except Exception as e:
            con.close()
            await send_long_message(update, f"❌ Database error: {str(e)}")
            await log_upload(filename, table_name, 0, 0, rows_count, "FAILED", str(e))
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)
            return
        
        # Cleanup
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
        
        await send_long_message(update, f"🎉 File processed successfully!")
        
    except Exception as e:
        await send_long_message(update, f"❌ File processing error: {str(e)}")
        await log_upload(filename, "unknown", 0, 0, 0, "FAILED", str(e))

# ==================== ANALYSIS REPORT GENERATOR ====================

async def generate_analysis_report(update: Update, df: pd.DataFrame, user_query: str):
    import numpy as np
    from datetime import datetime

    report = []
    report.append("📊 *DETAILED DATA ANALYSIS*\n")

    if df.empty:
        await send_long_message(update, "📭 No data available for analysis")
        return

    cols = df.columns.str.lower()

    # -------------------------------
    # 1️⃣ DATE-BASED ANALYSIS
    # -------------------------------
    date_cols = [c for c in df.columns if "date" in c.lower()]
    amount_cols = [c for c in df.columns if "amount" in c.lower()]

    if date_cols:
        date_col = date_cols[0]
        report.append("📅 *Date Trend Analysis*")

        if amount_cols:
            amt_col = amount_cols[0]
            trend = (
                df.groupby(date_col)[amt_col]
                .sum()
                .sort_index()
            )

            for d, v in trend.items():
                report.append(f"• {d} → ₹{v:,.0f}")

            if len(trend) >= 2:
                first, last = trend.iloc[0], trend.iloc[-1]
                growth = ((last - first) / max(first, 1)) * 100
                report.append(f"📈 Change: {growth:+.1f}%")

        else:
            counts = df[date_col].value_counts().head(5)
            for d, c in counts.items():
                report.append(f"• {d} → {c} records")

    # -------------------------------
    # 2️⃣ NUMERIC STATISTICS
    # -------------------------------
    numeric_cols = df.select_dtypes(include=[np.number]).columns

    if len(numeric_cols) > 0:
        report.append("\n📈 *Numeric Statistics*")
        for col in numeric_cols[:5]:
            report.append(
                f"• {col}: Min={df[col].min()}, Max={df[col].max()}, Avg={df[col].mean():.2f}"
            )

    # -------------------------------
    # 3️⃣ CATEGORICAL DISTRIBUTION
    # -------------------------------
    cat_cols = df.select_dtypes(include=["object"]).columns

    report.append("\n📌 *Top Distributions*")
    for col in cat_cols[:3]:
        top_vals = df[col].value_counts().head(3)
        report.append(f"• {col}")
        for v, c in top_vals.items():
            report.append(f"  - {v} ({c}x)")

    # -------------------------------
    # 4️⃣ VOLUME / QUANTITY ANALYSIS
    # -------------------------------
    qty_cols = [c for c in df.columns if any(k in c.lower() for k in ["pcs", "qty"])]

    if qty_cols:
        qcol = qty_cols[0]
        report.append("\n📦 *Volume Analysis*")
        report.append(f"• Avg {qcol}: {df[qcol].mean():.2f}")
        singles = (df[qcol] == 1).sum()
        if singles / len(df) > 0.6:
            report.append("⚠️ Many single-unit records detected")

    # -------------------------------
    # 5️⃣ CONTRIBUTION / CONCENTRATION
    # -------------------------------
    party_cols = [c for c in df.columns if any(k in c.lower() for k in ["customer", "party"])]

    if party_cols and amount_cols:
        pcol = party_cols[0]
        acol = amount_cols[0]

        contrib = df.groupby(pcol)[acol].sum().sort_values(ascending=False)
        total = contrib.sum()

        report.append("\n👥 *Contribution Analysis*")
        for k, v in contrib.head(3).items():
            report.append(f"• {k[:30]} → {(v/total)*100:.1f}%")

        if contrib.iloc[0] / total > 0.5:
            report.append("⚠️ High dependency on single entity")

    # -------------------------------
    # 6️⃣ ANOMALY DETECTION
    # -------------------------------
    if amount_cols:
        acol = amount_cols[0]
        avg = df[acol].mean()
        outliers = df[df[acol] < avg * 0.1]

        if not outliers.empty:
            report.append("\n🚨 *Anomalies*")
            report.append(f"• {len(outliers)} unusually low-value records")

    # -------------------------------
    # 7️⃣ BUSINESS SUGGESTIONS
    # -------------------------------
    report.append("\n💡 *Suggestions*")

    if qty_cols and singles / len(df) > 0.6:
        report.append("• Consider minimum quantity / batching")

    if party_cols and amount_cols and contrib.iloc[0] / total > 0.5:
        report.append("• Reduce dependency risk by diversifying")

    if date_cols and amount_cols and len(trend) >= 2 and growth > 50:
        report.append("• Sales spike detected — plan inventory accordingly")

    report.append(f"\n✅ Analysis completed at {datetime.now().strftime('%H:%M:%S')}")

    await send_long_message(update, "\n".join(report))


# ==================== GEMINI NL TO SQL HANDLER ====================

async def handle_nl_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle natural language queries via Gemini"""
    user_text = update.message.text.strip()
    if not user_text:
        await send_long_message(update, "❓ Please enter a question")
        return
    
    await send_long_message(update, "⏳ Processing your request...")
    
    prompt = f"""
You are an expert SQL assistant for an inventory system. Generate a safe SQL SELECT query ONLY.

CRITICAL RULES:
1. Return ONLY the SQL query in a code block, nothing else. No explanations.
2. Use exact table and column names from schema below.
3. For stock/inventory queries → use inventory_snapshots table
4. For orders/sales → use sale_order table
5. For delivery/challan → use sale_challan table
6. Always return results sorted by most recent first (ORDER BY relevant_date DESC)
7. Limit results to 50 rows for performance
8. Use ILIKE for case-insensitive text search (% wildcards)

Schema:
{SCHEMA_CONTEXT}

User question: {user_text}

Example good queries:
- SELECT * FROM inventory_snapshots WHERE production_code ILIKE '%ABC%' ORDER BY snapshot_date DESC LIMIT 50
- SELECT customer, order_no, order_date, bal_pcs FROM sale_order WHERE customer ILIKE '%SURYA%' ORDER BY order_date DESC LIMIT 50
- SELECT challan_no, customer_name, pcs FROM sale_challan WHERE customer_name ILIKE '%SURYA%' ORDER BY challan_date DESC LIMIT 50

Generate the SQL query ONLY in ```sql ``` code blocks.
"""
    
    try:
        response = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
        raw_text = response.text.strip()
        
        # Extract SQL from code blocks
        if "```sql" in raw_text:
            start = raw_text.find("```sql") + 6
            end = raw_text.find("```", start)
            sql = raw_text[start:end].strip()
        elif "```" in raw_text:
            start = raw_text.find("```") + 3
            end = raw_text.find("```", start)
            sql = raw_text[start:end].strip()
        else:
            sql = raw_text
        
        # Clean up SQL
        sql = sql.replace("/*", "").replace("*/", "").strip()
        sql = "\n".join(line.strip() for line in sql.split("\n") if line.strip() and not line.strip().startswith("#"))
        
        if not sql or len(sql) < 10:
            await send_long_message(update, "❌ Could not generate valid query. Please rephrase your question.")
            return
            
    except Exception as e:
        await send_long_message(update, f"❌ Error: {str(e)[:100]}")
        return
    
    try:
        con = get_connection()
        df = con.execute(sql).df()
        con.close()
        
        if df.empty:
            await send_long_message(update, "📭 No results found. Try rephrasing your question or use different keywords.")
            return
        
        # 📤 FIRST MESSAGE: Query Results Data (Clean Format)
        result_text = "✅ QUERY RESULTS\n"
        result_text += f"📊 Records: {len(df):,}\n\n"
        
        # Get columns
        columns = df.columns.tolist()
        df = auto_select_columns(df, sql)
        records = format_records_one_by_one(df)
        await send_long_message(
            update,
            f"✅ QUERY RESULTS\n📊 Records Found: {len(records)}"
        )
        for rec in records :
            await send_long_message(update,rec)
        # Build clean table
        '''result_text += "┌" + "─" * 58 + "┐\n"
        
        # Header
        col_widths = [min(len(col), 14) for col in columns]
        header_line = ""
        for col, width in zip(columns, col_widths):
            header_line += f" {col[:width-1]:<{width-1}}│"
        result_text += "│" + header_line + "\n"
        result_text += "├" + "─" * 58 + "┤\n"
        
        # Data rows (limit to 10)
        rows_to_show = min(len(df), 10)
        for idx in range(rows_to_show):
            row = df.iloc[idx]
            row_line = ""
            for col, width in zip(columns, col_widths):
                val = str(row[col])[:width-1]
                row_line += f" {val:<{width-1}}│"
            result_text += "│" + row_line + "\n"
        
        result_text += "└" + "─" * 58 + "┘\n"'''
        
        if len(df) > 10:
            result_text += f"\n... {len(df) - 10} more records"
        
        # Send data results as first message
        await send_long_message(update, result_text)
        
        # 📊 SECOND MESSAGE: Analysis Report
        await generate_analysis_report(update, df, user_text)
        
    except Exception as e:
        error_msg = str(e)
        await send_long_message(update, f"❌ Query Error: {error_msg[:100]}")

# ==================== COMMAND HANDLERS ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command"""
    message = """╔════════════════════════════════════════════════════════╗
║          🤖  INVENTORY BOT v3.0  🤖                   ║
║      Daily Upload + Image Storage + AI Queries       ║
╚════════════════════════════════════════════════════════╝

✨ FEATURES:
├─ 📤 Upload daily Excel files (auto-mapped)
├─ 🖼️ Store product images with reference text
├─ 💬 Natural language database queries
├─ 📊 Automatic daily snapshots
└─ 📁 Full upload history tracking

📝 HOW TO USE:
1️⃣  Upload Excel Files
    └─ Send Excel file → Auto-mapped to database

2️⃣  Store Images
    ├─ Send photo
    └─ Add caption as reference (e.g., "Product_ABC123")

3️⃣  Query Database
    ├─ Type questions naturally
    ├─ "What is current stock?"
    ├─ "Show orders from surya ethnic"
    └─ AI converts to SQL automatically

📋 COMMANDS:
  /uploadstatus - Today's upload history
  /listimages   - View stored images (get IDs)
  /getimage <id> - Download image by ID
  /debugdb      - Database statistics & samples
  /help         - Command list

╭─────────────────────────────────────────────────────────╮
│ Ready to go! Send a file, image, or ask a question.  │
╰─────────────────────────────────────────────────────────╯"""
    await update.message.reply_text(message)

async def upload_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show today's upload status"""
    try:
        con = get_connection()
        df = con.execute("""
            SELECT file_name, processing_status, rows_inserted, rows_updated, rows_failed, created_at
            FROM daily_uploads
            WHERE upload_date = CURRENT_DATE
            ORDER BY created_at DESC
        """).df()
        con.close()
        
        if df.empty:
            await send_long_message(update, "📭 No uploads today\n\nWaiting for file uploads...")
            return
        
        msg = "╔════════════════════════════════════════════════════════╗\n"
        msg += "║  📊  TODAY'S UPLOAD STATUS  📊                          ║\n"
        msg += "╚════════════════════════════════════════════════════════╝\n\n"
        
        total_rows = 0
        total_success = 0
        total_failed = 0
        
        for idx, row in df.iterrows():
            status_icon = "✅" if row['processing_status'] == 'SUCCESS' else "⚠️ "
            msg += f"{status_icon}  FILE: {row['file_name']}\n"
            msg += f"   ├─ Status: {row['processing_status']}\n"
            msg += f"   ├─ Inserted: {row['rows_inserted']:,} rows\n"
            msg += f"   ├─ Updated: {row['rows_updated']:,} rows\n"
            msg += f"   ├─ Failed: {row['rows_failed']:,} rows\n"
            msg += f"   └─ Time: {row['created_at']}\n\n"
            total_rows += row['rows_inserted'] + row['rows_updated']
            if row['processing_status'] == 'SUCCESS':
                total_success += 1
            else:
                total_failed += 1
        
        msg += "╭─────────────────────────────────────────────────────────╮\n"
        msg += "│ SUMMARY:                                                │\n"
        msg += f"│ • Total Uploads: {len(df):<47}│\n"
        msg += f"│ • Successful: {total_success:<4}  │  Failed: {total_failed:<38}│\n"
        msg += f"│ • Total Rows: {total_rows:,}                                   │\n"
        msg += "╰─────────────────────────────────────────────────────────╯"
        
        await send_long_message(update, msg)
    except Exception as e:
        await send_long_message(update, f"❌ Error: {str(e)[:100]}")

async def list_images(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List stored images"""
    try:
        con = get_connection()
        df = con.execute("""
            SELECT id, reference_text, category, created_at, file_size_bytes
            FROM image_storage
            ORDER BY created_at DESC
            LIMIT 30
        """).df()
        con.close()
        
        if df.empty:
            await send_long_message(update, "🖼️ *No Images Stored*\n\nTo store images:\n1. Send a photo to the bot\n2. Add a caption as reference\n3. Image will be stored with an ID")
            return
        
        msg = "╔════════════════════════════════════════════════════════╗\n"
        msg += "║  🖼️  STORED IMAGES  🖼️                                 ║\n"
        msg += "╚════════════════════════════════════════════════════════╝\n\n"
        
        for idx, row in df.iterrows():
            size_kb = row['file_size_bytes'] / 1024
            msg += f"┌─ IMAGE #{row['id']}\n"
            msg += f"├─ Reference: {row['reference_text']}\n"
            msg += f"├─ Category: {row['category']}\n"
            msg += f"├─ Size: {size_kb:.1f} KB\n"
            msg += f"├─ Uploaded: {row['created_at']}\n"
            msg += f"└─ Use: /getimage {row['id']}\n\n"
        
        msg += "╭─────────────────────────────────────────────────────────╮\n"
        msg += f"│ Total Images: {len(df):<49}│\n"
        msg += f"│ (Showing latest {min(len(df), 30):<41} │\n"
        msg += "╰─────────────────────────────────────────────────────────╯"
        
        await send_long_message(update, msg)
    except Exception as e:
        await send_long_message(update, f"❌ Error: {str(e)[:100]}")

async def get_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await send_long_message(update, "❌ Usage: /getimage <keyword>\n\nExample: /getimage surya\n         /getimage ethnic\n         /getimage surya_ethnic")
        return

    keyword = " ".join(context.args).strip().lower()

    con = get_connection()
    rows = con.execute("""
        SELECT id, image_data, reference_text, created_at
        FROM image_storage
        WHERE reference_text ILIKE ? OR reference_text ILIKE ? OR reference_text ILIKE ?
        ORDER BY created_at DESC
    """, [f"%{keyword}%", f"%{keyword.replace(' ', '%')}%", f"%{keyword.replace('_', '%')}%"]).fetchall()
    con.close()

    if not rows:
        await send_long_message(
            update,
            f"❌ No images found for: '{keyword}'\n\n"
            f"💡 Try:\n"
            f"   • /listimages - View all available images\n"
            f"   • Use exact batch reference name"
        )
        return

    # Get unique batch references
    batch_refs = list(set([row[2] for row in rows]))
    
    await send_long_message(
        update,
        f"📦 Found {len(rows)} image(s) matching '{keyword}'\n"
        f"🆔 In batch(es): {', '.join(batch_refs)}\n\n"
        f"Downloading images..."
    )

    # Send all matching images
    for image_id, image_data, ref_text, created_at in rows:
        try:
            await update.message.reply_photo(
                photo=image_data,
                caption=f"📝 ID: {image_id}\n📋 Reference: {ref_text}\n⏰ Stored: {created_at}"
            )
        except Exception as e:
            await send_long_message(update, f"⚠️ Error sending image {image_id}: {str(e)[:50]}")
    
    await send_long_message(update, f"✅ Retrieved {len(rows)} image(s)")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Help command"""
    message = """📋 **USAGE:**

1️⃣ **Upload Daily Files:**
   Send Excel file → Auto-mapped to database
   Tracks: Ready Stock, Sales Orders, Challans, Inwards

2️⃣ **Store Images:**
   Send photo with caption
   Caption = Reference text (e.g., "Product_SKU_001")

3️⃣ **Query Database:**
   Just type your question in natural language!
   AI converts to SQL automatically

**Commands:**
/start - Show features
/uploadstatus - Today's upload history
/listimages - View stored images (shows IDs)
/getimage <id> - Download image by ID
/debugdb - Show database statistics
/help - This message

(Alternative formats: /upload_status, /list_images, /get_image, /debug_db)
    """
    await update.message.reply_text(message, parse_mode='Markdown')

async def debug_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show database statistics and sample data"""
    try:
        con = get_connection()
        
        msg = "╔════════════════════════════════════════════════════════╗\n"
        msg += "║  🔍  DATABASE DIAGNOSTICS  🔍                          ║\n"
        msg += "╚════════════════════════════════════════════════════════╝\n\n"
        
        # Table statistics
        msg += "📊 TABLE STATISTICS:\n"
        msg += "┌────────────────────────────────────────────────────┐\n"
        
        tables = ["products", "inventory_snapshots", "sale_order", "sale_challan", "stock_inward", "daily_uploads", "image_storage"]
        table_stats = []
        
        for table in tables:
            try:
                count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                table_stats.append((table, count))
            except:
                table_stats.append((table, "ERROR"))
        
        for table, count in table_stats:
            if isinstance(count, int):
                status = "✅" if count > 0 else "⚠️ "
                msg += f"│ {status} {table:.<30} {count:>10,} rows │\n"
            else:
                msg += f"│ ❌ {table:.<30} {count:>10} │\n"
        
        msg += "└────────────────────────────────────────────────────┘\n"
        
        # Sample customers
        msg += "\n👥 SAMPLE CUSTOMERS:\n"
        msg += "┌────────────────────────────────────────────────────┐\n"
        try:
            customers = con.execute("""
                SELECT DISTINCT customer FROM sale_order 
                ORDER BY customer
                LIMIT 8
            """).fetchall()
            if customers:
                for i, (cust,) in enumerate(customers, 1):
                    msg += f"│ {i}. {str(cust)[:45]:45} │\n"
            else:
                msg += "│ (No customers found)                           │\n"
        except Exception as e:
            msg += f"│ Error: {str(e)[:40]:45} │\n"
        
        msg += "└────────────────────────────────────────────────────┘\n"
        
        # Latest stock
        msg += "\n📦 LATEST INVENTORY:\n"
        msg += "┌────────────────────────────────────────────────────┐\n"
        try:
            stocks = con.execute("""
                SELECT production_code, color, ready_pcs, snapshot_date 
                FROM inventory_snapshots 
                ORDER BY snapshot_date DESC 
                LIMIT 5
            """).fetchall()
            if stocks:
                for code, color, pcs, date in stocks:
                    msg += f"│ {code} ({color:10}) │ {pcs:6,} pcs │ {str(date):12} │\n"
            else:
                msg += "│ (No inventory data)                            │\n"
        except Exception as e:
            msg += f"│ Error: {str(e)[:40]:45} │\n"
        
        msg += "└────────────────────────────────────────────────────┘\n"
        
        # Summary
        total_records = sum(count for _, count in table_stats if isinstance(count, int))
        msg += "\n╭─────────────────────────────────────────────────────────╮\n"
        msg += f"│ TOTAL RECORDS IN DATABASE: {total_records:,}                       │\n"
        msg += "│ Database Status: ✅ ACTIVE                             │\n"
        msg += "╰─────────────────────────────────────────────────────────╯"
        
        con.close()
        await send_long_message(update, msg)
        
    except Exception as e:
        await send_long_message(update, f"❌ Debug error: {str(e)[:100]}")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle errors during polling gracefully"""
    error = context.error
    
    if isinstance(error, TimedOut):
        print(f"⚠️  Network timeout (recovering): {error}")
        return
    
    if isinstance(error, NetworkError):
        print(f"⚠️  Network error (recovering): {error}")
        return
    
    # Catch ConnectError and other connection-related errors
    error_str = str(error)
    if "ConnectError" in error_str or "connection" in error_str.lower():
        print(f"⚠️  Connection error (recovering): {type(error).__name__}")
        return
    
    print(f"❌ Update error: {error}")

def create_http_client():
    """Create optimized HTTP client with connection pooling"""
    return HTTPXRequest(
        connect_timeout=60,
        read_timeout=60,
        write_timeout=60,
        pool_timeout=60
    )

def main():
    print("\n" + "="*80)
    print("🤖 Enhanced Inventory Bot v3.0 Starting")
    print("="*80)
    print("✓ File uploads: Daily Excel reports")
    print("✓ Image storage: Product images with references")
    print("✓ AI queries: Natural language to SQL")
    print("⛔ Stop: Press CTRL+C to exit")
    print("="*80 + "\n")

    print(f"🚀 Starting bot...\n")
    
    # Create optimized HTTP request
    request = create_http_client()

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .request(request)
        .build()
    )

    # Add error handler
    app.add_error_handler(error_handler)

    # Command handlers (with and without underscores for flexibility)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("debug_db", debug_db))
    app.add_handler(CommandHandler("debugdb", debug_db))
    app.add_handler(CommandHandler("upload_status", upload_status))
    app.add_handler(CommandHandler("uploadstatus", upload_status))
    app.add_handler(CommandHandler("list_images", list_images))
    app.add_handler(CommandHandler("listimages", list_images))
    app.add_handler(CommandHandler("get_image", get_image))
    app.add_handler(CommandHandler("getimage", get_image))
    app.add_handler(CommandHandler("deleteimage", delete_image))

    # Message handlers
    app.add_handler(MessageHandler(filters.Document.ALL, handle_file))
    app.add_handler(MessageHandler(filters.PHOTO, handle_image_upload))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_nl_query))

    # DB status check
    try:
        con = get_connection()
        products = con.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        snapshots = con.execute("SELECT COUNT(*) FROM inventory_snapshots").fetchone()[0]
        images = con.execute("SELECT COUNT(*) FROM image_storage").fetchone()[0]
        con.close()
        print(f"✅ Database ready: {products:,} products, {snapshots:,} snapshots, {images:,} images")
    except Exception as e:
        print(f"⚠️  DB check: {e}")

    print("✅ Bot running and listening for messages...")
    print("💡 Ready!\n")
    
    # Run polling
    app.run_polling(
        allowed_updates=None,
        drop_pending_updates=False
    )


if __name__ == "__main__":
    main()


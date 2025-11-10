import psycopg2, json, re
from datetime import datetime

DB_URL = "postgresql://hunny:QTSFnZ96Zd7VdBTU6wqleJ8pXfPG97Ga@dpg-d47dbpili9vc738l3n00-a.singapore-postgres.render.com/flowbit"

def safe_val(obj, key):
    """Return nested 'value' if dict, else direct string."""
    if not obj:
        return None
    val = obj.get(key) if isinstance(obj, dict) else None
    if isinstance(val, dict):
        return val.get("value")
    elif isinstance(val, (str, int, float)):
        return val
    return None

def extract_value(field):
    """Return .get('value') if dict, else raw string."""
    if isinstance(field, dict):
        return field.get("value")
    return field

def normalize_date(date_str):
    """Convert YYYY or YYYY-MM to YYYY-MM-DD for PostgreSQL."""
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        # If only year
        if re.fullmatch(r"\d{4}", date_str):
            return f"{date_str}-01-01"
        # If year-month
        if re.fullmatch(r"\d{4}-\d{2}", date_str):
            return f"{date_str}-01"
        # Already full date
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except Exception:
        return None

conn = psycopg2.connect(DB_URL, sslmode="require")
cur = conn.cursor()
print("✅ Connected to Render PostgreSQL successfully!")

# ---------- Create Tables ----------
cur.execute("""
CREATE TABLE IF NOT EXISTS vendors (
    id SERIAL PRIMARY KEY,
    vendor_name TEXT,
    vendor_tax_id TEXT,
    vendor_address TEXT
);
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    customer_name TEXT,
    customer_address TEXT
);
CREATE TABLE IF NOT EXISTS invoices (
    id SERIAL PRIMARY KEY,
    invoice_id TEXT,
    invoice_date DATE,
    delivery_date DATE,
    vendor_id INT REFERENCES vendors(id),
    customer_id INT REFERENCES customers(id),
    sub_total NUMERIC,
    total_tax NUMERIC,
    invoice_total NUMERIC,
    currency_symbol TEXT
);
CREATE TABLE IF NOT EXISTS payments (
    id SERIAL PRIMARY KEY,
    invoice_id INT REFERENCES invoices(id),
    due_date TEXT,
    payment_terms TEXT,
    bank_account_number TEXT
);
CREATE TABLE IF NOT EXISTS line_items (
    id SERIAL PRIMARY KEY,
    invoice_id INT REFERENCES invoices(id),
    description TEXT,
    quantity NUMERIC,
    unit_price NUMERIC,
    total_price NUMERIC,
    vat_rate NUMERIC,
    vat_amount NUMERIC
);
""")
conn.commit()
print("✅ Tables created successfully!")

# ---------- Load JSON ----------
with open("Analytics_Test_Data.json") as f:
    data = json.load(f)

inserted = 0
skipped = 0

for item in data:
    llm = item.get("extractedData", {}).get("llmData", {})
    if not llm:
        skipped += 1
        continue

    vendor = extract_value(llm.get("vendor"))
    customer = extract_value(llm.get("customer"))
    invoice = extract_value(llm.get("invoice"))
    payment = extract_value(llm.get("payment"))
    summary = extract_value(llm.get("summary"))
    line_items_block = llm.get("lineItems", {}).get("value", {}).get("items", {}).get("value", [])

    vendor_name = safe_val(vendor, "vendorName")
    vendor_tax_id = safe_val(vendor, "vendorTaxId")
    vendor_address = safe_val(vendor, "vendorAddress")

    customer_name = safe_val(customer, "customerName")
    customer_address = safe_val(customer, "customerAddress")

    cur.execute("""
        INSERT INTO vendors (vendor_name, vendor_tax_id, vendor_address)
        VALUES (%s,%s,%s) RETURNING id
    """, (vendor_name, vendor_tax_id, vendor_address))
    vendor_id = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO customers (customer_name, customer_address)
        VALUES (%s,%s) RETURNING id
    """, (customer_name, customer_address))
    customer_id = cur.fetchone()[0]

    invoice_id = safe_val(invoice, "invoiceId")
    invoice_date = normalize_date(safe_val(invoice, "invoiceDate"))
    delivery_date = normalize_date(safe_val(invoice, "deliveryDate"))

    sub_total = safe_val(summary, "subTotal")
    total_tax = safe_val(summary, "totalTax")
    invoice_total = safe_val(summary, "invoiceTotal")

    currency_field = summary.get("currencySymbol") if isinstance(summary, dict) else summary
    currency = extract_value(currency_field)

    cur.execute("""
        INSERT INTO invoices (invoice_id, invoice_date, delivery_date, vendor_id, customer_id, sub_total, total_tax, invoice_total, currency_symbol)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
    """, (invoice_id, invoice_date, delivery_date, vendor_id, customer_id, sub_total, total_tax, invoice_total, currency))
    inv_id = cur.fetchone()[0]

    if payment:
        due_date = safe_val(payment, "dueDate")
        terms = safe_val(payment, "paymentTerms")
        bank_acc = safe_val(payment, "bankAccountNumber")
        cur.execute("""
            INSERT INTO payments (invoice_id, due_date, payment_terms, bank_account_number)
            VALUES (%s,%s,%s,%s)
        """, (inv_id, due_date, terms, bank_acc))

    for li in line_items_block:
        desc = safe_val(li, "description")
        qty = safe_val(li, "quantity")
        unit_price = safe_val(li, "unitPrice")
        total_price = safe_val(li, "totalPrice")
        vat_rate = safe_val(li, "vatRate")
        vat_amount = safe_val(li, "vatAmount")
        cur.execute("""
            INSERT INTO line_items (invoice_id, description, quantity, unit_price, total_price, vat_rate, vat_amount)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (inv_id, desc, qty, unit_price, total_price, vat_rate, vat_amount))

    inserted += 1

conn.commit()
print(f"✅ Data inserted successfully! Inserted {inserted}, skipped {skipped}.")
cur.close()
conn.close()
print(" Done — Database ready on Render!")

import sqlite3
import logging
import re
from datetime import datetime, date
from dataclasses import dataclass, field
from typing import List
from abc import ABC, abstractmethod

# ==============================================================================
#  LinkMind v101.0 - THE RICO ACT EDITION (Multi-Strike Justice)
#  -----------------------------------------------------------------------------
#  1. CONCURRENT VERDICTS: Detects License + Bandwidth + Zombie issues simultaneously.
#  2. BATCH EXECUTION: Generates a multi-command script for maximum impact.
#  3. AGGREGATE SAVINGS: Calculates the sum of all crimes combined (RICO style).
# ==============================================================================

CONFIG = {
    'DB_PATH': "LinkMind_Network.db",
    'LOG_FILE': "godfather.log",
    'PROBATION_DAYS': 15,
    'SAFETY_MARGIN': 1.2, # 20% Headroom for capacity
    'COSTS': {
        'LICENSE_MBPS': 10.0, # $10 per Mbps
        'BW_MHZ': 20.0,       # $20 per MHz
        'POWER_WATT': 5.0     # $5 per Watt (Zombie Port)
    }
}

logging.basicConfig(filename=CONFIG['LOG_FILE'], level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger("Godfather_RICO")

# --- 🛠️ 1. Safe Utils (Robustness Layer) ---
def safe_extract(pattern, text, default=0, return_type=int):
    """Safely extracts regex groups to prevent AttributeError crashes."""
    match = re.search(pattern, text)
    if not match: return default
    try:
        if return_type == float: return float(match.group(1))
        return int(match.group(1))
    except: return default

def detect_vendor(raw_text):
    """Auto-detects vendor based on log signature."""
    if "Current License Capacity" in raw_text: return "Huawei"
    return "Unknown"

# --- 2. Data Structures (RICO Enabled) ---
@dataclass
class NodeData:
    id: str; vendor: str; bandwidth: int; throughput: float
    license_reserved: int; license_actual: int; admin_status: str

@dataclass
class Offense:
    """Represents a single crime."""
    code: str         # LICENSE, BW, ZOMBIE
    wasted_qty: float
    saving_value: float
    details: str

@dataclass
class Verdict:
    """The Final Judgment: Contains a list of crimes (Indictment)."""
    status: str       # Guilty / Innocent
    offenses: List[Offense] = field(default_factory=list) # 🆕 List of crimes
    total_savings: float = 0.0

# --- 3. Database Manager (The Memory) ---
class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self._init_tables()

    def _get_conn(self): return sqlite3.connect(self.db_path)

    def _init_tables(self):
        with self._get_conn() as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS probation_list 
                          (link_id TEXT PRIMARY KEY, offense_summary TEXT, start_date TEXT, 
                           last_seen TEXT, strike_count INTEGER DEFAULT 1)''')
            conn.execute('''CREATE TABLE IF NOT EXISTS financial_ledger 
                          (id INTEGER PRIMARY KEY, date TEXT, link_id TEXT, 
                           action_taken TEXT, recovered_value REAL)''')

    def check_probation(self, link_id):
        with self._get_conn() as conn:
            return conn.execute("SELECT start_date, strike_count FROM probation_list WHERE link_id=?", (link_id,)).fetchone()

    def add_suspect(self, link_id, summary):
        today = date.today().isoformat()
        with self._get_conn() as conn:
            conn.execute("INSERT OR IGNORE INTO probation_list VALUES (?, ?, ?, ?, 1)", (link_id, summary, today, today))

    def update_suspect(self, link_id):
        today = date.today().isoformat()
        with self._get_conn() as conn:
            conn.execute("UPDATE probation_list SET last_seen=?, strike_count=strike_count+1 WHERE link_id=?", (today, link_id))

    def release_suspect(self, link_id):
        with self._get_conn() as conn:
            conn.execute("DELETE FROM probation_list WHERE link_id=?", (link_id,))

    def record_profit(self, link_id, action, value):
        today = date.today().isoformat()
        with self._get_conn() as conn:
            conn.execute("INSERT INTO financial_ledger (date, link_id, action_taken, recovered_value) VALUES (?, ?, ?, ?)",
                         (today, link_id, action, value))

# --- 4. Vendor Adapters (The Translators) ---
class VendorAdapter(ABC):
    @abstractmethod
    def parse(self, text): pass
    @abstractmethod
    def generate_fix(self, id, val, type): pass

class HuaweiAdapter(VendorAdapter):
    def parse(self, text):
        return {
            'id': safe_extract(r"\+\+\+\s+([A-Z0-9_]+)", text, "UNKNOWN", str),
            'license_reserved': safe_extract(r"Current License Capacity\(Mbps\)\s*:\s*(\d+)", text, 0, int),
            'throughput': safe_extract(r"Air-interface Throughput\(Mbps\)\s*:\s*([\d.]+)", text, 0.0, float),
            'bandwidth': safe_extract(r"Channel Bandwidth\(MHz\)\s*:\s*(\d+)", text, 0, int),
            'status': re.search(r"Port Admin Status\s*:\s*([A-Z]+)", text).group(1) if re.search(r"Port Admin Status", text) else "DOWN"
        }
    
    def generate_fix(self, link_id, value, fix_type):
        if fix_type == "LICENSE": return f"MOD MWLICENSE: ID={link_id}, CAP={int(value)};"
        if fix_type == "BW": return f"MOD MWPORT: ID={link_id}, AM=ENABLE, BW={int(value)}MHZ;"
        if fix_type == "ZOMBIE": return f"MOD MWPORT: ID={link_id}, ADMIN=DOWN;"
        return ""

# --- 5. The Godfather Core (Business Logic) ---
class TheGodfather:
    def __init__(self):
        self.db = DatabaseManager(CONFIG['DB_PATH'])
        self.adapters = {"Huawei": HuaweiAdapter()}

    def ingest_and_audit(self, raw_log):
        """The Main Pipeline: Ingest -> Parse -> Concurrent Judgment -> Process."""
        vendor_name = detect_vendor(raw_log)
        adapter = self.adapters.get(vendor_name)
        if not adapter: return "❌ Unknown Vendor Signature"

        # 1. Parse Data
        data = adapter.parse(raw_log)
        needed_license = int(data['throughput'] * CONFIG['SAFETY_MARGIN'])
        
        node = NodeData(
            id=data['id'], vendor=vendor_name, bandwidth=data['bandwidth'],
            throughput=data['throughput'], license_reserved=data['license_reserved'],
            license_actual=needed_license, admin_status=data['status']
        )

        # 2. Multi-Strike Judgment
        verdict = self._judge_concurrently(node) 
        
        # 3. Execution Logic
        return self._process_verdict(node, verdict, adapter)

    def _judge_concurrently(self, node: NodeData) -> Verdict:
        """
        The RICO Judge: Checks for ALL crimes, doesn't stop at the first one.
        Returns a Verdict containing a list of Offenses.
        """
        offenses = []
        total_saving = 0.0

        # Crime 1: License Hoarding
        if node.license_reserved > (node.license_actual * 1.5) and node.license_reserved > 50:
            wasted = node.license_reserved - node.license_actual
            val = wasted * CONFIG['COSTS']['LICENSE_MBPS']
            offenses.append(Offense("LICENSE", wasted, val, f"Hoarding {wasted}Mbps"))
            total_saving += val

        # Crime 2: Spectrum Waste (The 56MHz Trap)
        if node.bandwidth == 56 and node.throughput < 50:
            wasted = 28 # Difference (56 -> 28)
            val = wasted * CONFIG['COSTS']['BW_MHZ']
            offenses.append(Offense("BW", wasted, val, "Wasting Spectrum"))
            total_saving += val

        # Crime 3: Zombie Port (Ghost Link)
        # Port is UP but carrying almost zero traffic
        if node.admin_status == "UP" and node.throughput < 1.0:
            val = 50.0 # Estimated Power Savings
            offenses.append(Offense("ZOMBIE", 1, val, "Zombie Port Active"))
            total_saving += val

        status = "Guilty" if offenses else "Innocent"
        return Verdict(status, offenses, total_saving)

    def _process_verdict(self, node, verdict, adapter):
        if verdict.status == "Innocent":
            record = self.db.check_probation(node.id)
            if record:
                self.db.release_suspect(node.id)
                return f"✅ {node.id}: Cleared of all charges."
            return f"✅ {node.id}: Clean."

        # Guilty Logic
        record = self.db.check_probation(node.id)
        
        # Summary for DB tracking
        crimes_summary = ",".join([o.code for o in verdict.offenses])
        
        if not record:
            self.db.add_suspect(node.id, crimes_summary)
            return f"📝 {node.id}: Indicted for [{crimes_summary}]. Probation Day 1."
        
        start_date = date.fromisoformat(record[0])
        days_in_jail = (date.today() - start_date).days
        self.db.update_suspect(node.id)

        if days_in_jail >= CONFIG['PROBATION_DAYS']:
            return self._execute_rico_act(node, verdict, adapter, days_in_jail)
        
        return f"⚠️ SURVEILLANCE: {node.id} | Day {days_in_jail} | Crimes: {crimes_summary}"

    def _execute_rico_act(self, node, verdict, adapter, days):
        """
        Executes the RICO Act: Punishes ALL crimes in a single batch.
        """
        scripts = []
        scripts.append(f"// ⚖️ RICO ACT EXECUTION: {node.id} (After {days} days)")
        
        for offense in verdict.offenses:
            # 1. Generate Script for each offense
            if offense.code == "LICENSE":
                cmd = adapter.generate_fix(node.id, node.license_actual, "LICENSE")
                scripts.append(f"// Fix License (Save ${offense.saving_value})")
                scripts.append(cmd)
            elif offense.code == "BW":
                cmd = adapter.generate_fix(node.id, 28, "BW")
                scripts.append(f"// Optimize BW (Save ${offense.saving_value})")
                scripts.append(cmd)
            elif offense.code == "ZOMBIE":
                cmd = adapter.generate_fix(node.id, 0, "ZOMBIE")
                scripts.append(f"// Kill Zombie (Save ${offense.saving_value})")
                scripts.append(cmd)
            
            # 2. Record individual profits in Ledger
            self.db.record_profit(node.id, f"RICO_{offense.code}", offense.saving_value)

        self.db.release_suspect(node.id) # Case Closed
        
        full_script = "\n".join(scripts)
        logger.critical(f"RICO EXECUTED [{node.id}]: Total Savings ${verdict.total_savings}")
        
        return (f"💰 JACKPOT! Executed Multi-Strike Justice on {node.id}\n"
                f"   Total Savings: ${verdict.total_savings:,.2f}\n"
                f"   Crimes Punished: {len(verdict.offenses)}\n"
                f"📜 BATCH SCRIPT:\n{full_script}")

# --- Simulation Execution ---
if __name__ == "__main__":
    godfather = TheGodfather()
    
    # Scenario: A Catastrophic Link (Zombie + Hoarding License + Wasting Spectrum)
    # Throughput 0.5 = Zombie
    # BW 56MHz = Waste
    # License 400M = Waste
    catastrophic_log = """
    +++    DJELFA_GHOST_LINK     2026-02-04
    Current License Capacity(Mbps)  : 400
    Air-interface Throughput(Mbps)  : 0.5
    Channel Bandwidth(MHz)          : 56
    Port Admin Status               : UP
    """

    print("--- 🕵️ RICO Act Audit (Day 1) ---")
    
    # 1. First Indictment
    print(godfather.ingest_and_audit(catastrophic_log))
    
    # 2. Cheat Time (Simulate 15 days passing in DB)
    with sqlite3.connect(CONFIG['DB_PATH']) as c:
        c.execute("UPDATE probation_list SET start_date='2026-01-01' WHERE link_id='DJELFA_GHOST_LINK'")
        c.commit()
        
    # 3. Final Judgment (The Knockout Punch)
    print("\n--- ⚖️ 15 Days Later (Judgment Day) ---")
    print(godfather.ingest_and_audit(catastrophic_log))
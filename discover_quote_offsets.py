# discover_quote_offsets.py
import asyncio, base64, struct, yaml, aiohttp

BUY_DISC  = bytes([103,244,82,31,44,245,119,119])
SELL_DISC = bytes([62,47,55,10,165,3,220,42])
PUMPSWAP  = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

BUYS  = [
    "5Q3LDQ5KK9nsYvRwe7sKWRpqo3fM8qt9fBtZCLPAqLWNPcrSWVRLYfmGGB89yLgMQn7BqGQzntMyRGFWDzhKRBRE",
    "3RReX4jgoYhyt9ncAF79AiTdf1qx8zAeXzo24q2DeGSUUAFRti2SfsAe3saQ4ejLNBaXAEwxASeqkH2qWXvZKu8j",
]
SELLS = [
    "2xugERa7eBXQaFMvAEvQ83gm7gr4zvVD81PtWgBZmP8azGsowkuCrf45daRus4pFK29pxcCZRj6CHoTu2MLnDwFt",
    "4CzLWsMHYDJa32H2cdvjnK676g1YAVnvG5YdfYqYy8vweX5e2XuY8TkUazGrsP4ndfPLZFSRg9yUTV3MRdqHgZQy",
]

def rpc_url():
    cfg = yaml.safe_load(open("config.yaml"))
    return cfg["helius"]["rpc_url"]

async def get_tx(session, url, sig):
    body = {"jsonrpc":"2.0","id":1,"method":"getTransaction",
            "params":[sig,{"encoding":"base64","maxSupportedTransactionVersion":0,"commitment":"confirmed"}]}
    async with session.post(url, json=body) as r:
        return (await r.json())["result"]

def extract_events(tx, disc):
    """Pull Program data: base64 lines, filter by discriminator."""
    logs = tx["meta"]["logMessages"]
    out = []
    for line in logs:
        if "Program data:" not in line: continue
        b64 = line.split("Program data:",1)[1].strip()
        try:
            raw = base64.b64decode(b64)
        except Exception:
            continue
        if raw[:8] == disc:
            out.append(raw[8:])  # strip discriminator
    return out

def u64(buf, off):
    if off+8 > len(buf): return None
    return struct.unpack("<Q", buf[off:off+8])[0]

def scan(payload, label):
    print(f"\n--- {label} (payload {len(payload)} bytes) ---")
    print("offset | lamports              | SOL")
    print("-------+-----------------------+-------------")
    # scan every 8-byte u64 on 8-byte boundaries
    for off in range(0, len(payload)-7, 8):
        v = u64(payload, off)
        sol = v / 1e9
        # only show plausible trade/fee values (0.0001 SOL to 100k SOL)
        if 100_000 <= v <= 100_000 * 10**9:
            mark = ""
            if off == 80:  mark = "  <- lp_fee"
            if off == 96:  mark = "  <- protocol_fee"
            if off == 352: mark = "  <- creator_fee"
            print(f"  {off:4d} | {v:21d} | {sol:12.6f}{mark}")

async def main():
    url = rpc_url()
    async with aiohttp.ClientSession() as s:
        for sig in BUYS:
            tx = await get_tx(s, url, sig)
            if not tx: print(f"{sig}: NOT FOUND"); continue
            events = extract_events(tx, BUY_DISC)
            for i, p in enumerate(events):
                scan(p, f"BUY {sig[:12]}... event#{i}")
        for sig in SELLS:
            tx = await get_tx(s, url, sig)
            if not tx: print(f"{sig}: NOT FOUND"); continue
            events = extract_events(tx, SELL_DISC)
            for i, p in enumerate(events):
                scan(p, f"SELL {sig[:12]}... event#{i}")

if __name__ == "__main__":
    asyncio.run(main())
import asyncio, yaml, aiohttp
from solders.pubkey import Pubkey

PUMPFUN = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
MINT = "Hc2CZzfiuBuSjTg6mpUByHVryy5e4yu8L7L4vtppump"

with open("config.yaml") as f:
    HELIUS = yaml.safe_load(f)["helius"]["rpc_url"]

def pda(mint):
    p, _ = Pubkey.find_program_address([b"bonding-curve", bytes(Pubkey.from_string(mint))], Pubkey.from_string(PUMPFUN))
    return str(p)

async def paginate_all(addr, session, cap=5000):
    all_sigs = []
    before = None
    while len(all_sigs) < cap:
        params = [addr, {"limit": 1000, "commitment": "confirmed"}]
        if before: params[1]["before"] = before
        async with session.post(HELIUS, json={"jsonrpc":"2.0","id":1,"method":"getSignaturesForAddress","params":params}, timeout=aiohttp.ClientTimeout(total=30)) as r:
            data = await r.json()
        batch = data.get("result", []) or []
        if not batch: break
        all_sigs.extend(batch)
        if len(batch) < 1000: break
        before = batch[-1]["signature"]
        await asyncio.sleep(0.1)
    return all_sigs

async def main():
    pda_addr = pda(MINT)
    print(f"PDA: {pda_addr}\n")
    async with aiohttp.ClientSession() as s:
        mint_sigs = await paginate_all(MINT, s)
        pda_sigs = await paginate_all(pda_addr, s)
    print(f"TOTAL sigs on mint: {len(mint_sigs)}")
    print(f"TOTAL sigs on PDA:  {len(pda_sigs)}")
    print(f"\nOldest slot on mint: {mint_sigs[-1]['slot'] if mint_sigs else 'N/A'}")
    print(f"Oldest slot on PDA:  {pda_sigs[-1]['slot'] if pda_sigs else 'N/A'}")
    print(f"\nNewest slot on mint: {mint_sigs[0]['slot'] if mint_sigs else 'N/A'}")
    print(f"Newest slot on PDA:  {pda_sigs[0]['slot'] if pda_sigs else 'N/A'}")
    mint_set = {s['signature'] for s in mint_sigs}
    pda_set = {s['signature'] for s in pda_sigs}
    only_mint = mint_set - pda_set
    only_pda = pda_set - mint_set
    print(f"\nSigs ONLY in mint (not in PDA): {len(only_mint)}")
    print(f"Sigs ONLY in PDA (not in mint): {len(only_pda)}")

asyncio.run(main())
# save as find_true_inception.py
import asyncio, yaml, aiohttp

MINT = "AnAVK1B3ZQRcUVriqLNTYFZ32PgJUSDDvJmT4WdXpump"
with open("config.yaml") as f:
    HELIUS = yaml.safe_load(f)["helius"]["rpc_url"]

async def paginate_exhaustive(addr, session):
    all_sigs = []
    before = None
    page = 0
    while True:
        page += 1
        params = [addr, {"limit": 1000, "commitment": "confirmed"}]
        if before: params[1]["before"] = before
        async with session.post(HELIUS, json={"jsonrpc":"2.0","id":1,"method":"getSignaturesForAddress","params":params}, timeout=aiohttp.ClientTimeout(total=30)) as r:
            data = await r.json()
        batch = data.get("result", []) or []
        print(f"Page {page}: {len(batch)} sigs, oldest slot in batch = {batch[-1]['slot'] if batch else 'N/A'}")
        if not batch: break
        all_sigs.extend(batch)
        before = batch[-1]["signature"]
        if page > 20: break  # hard cap
        await asyncio.sleep(0.1)
    return all_sigs

async def main():
    async with aiohttp.ClientSession() as s:
        sigs = await paginate_exhaustive(MINT, s)
        if sigs:
            sigs.sort(key=lambda x: x["slot"])
            print(f"\nTotal sigs: {len(sigs)}")
            print(f"TRUE oldest slot: {sigs[0]['slot']}")
            print(f"Newest slot: {sigs[-1]['slot']}")
            print(f"First 5 slots: {sorted(set(x['slot'] for x in sigs))[:5]}")

asyncio.run(main())
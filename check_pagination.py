import asyncio, yaml, aiohttp

MINT = "Hc2CZzfiuBuSjTg6mpUByHVryy5e4yu8L7L4vtppump"
with open("config.yaml") as f:
    HELIUS = yaml.safe_load(f)["helius"]["rpc_url"]

async def main():
    async with aiohttp.ClientSession() as s:
        all_sigs = []
        before = None
        for page_num in range(1, 15):
            params = [MINT, {"limit": 1000, "commitment": "confirmed"}]
            if before: params[1]["before"] = before
            async with s.post(HELIUS, json={"jsonrpc":"2.0","id":1,"method":"getSignaturesForAddress","params":params}, timeout=aiohttp.ClientTimeout(total=30)) as r:
                data = await r.json()
            batch = data.get("result", []) or []
            print(f"Page {page_num}: {len(batch)} sigs, oldest slot={batch[-1]['slot'] if batch else 'N/A'}")
            if not batch:
                print("  → empty, stopping")
                break
            all_sigs.extend(batch)
            before = batch[-1]["signature"]
            await asyncio.sleep(0.1)
        print(f"\nTotal: {len(all_sigs)} sigs, oldest slot: {all_sigs[-1]['slot']}")

asyncio.run(main())

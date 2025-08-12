import { kvDiagnostics, listSubmissions } from "../lib/kv.js";

export default async function handler(_req, res) {
  try {
    const diag = await kvDiagnostics();
    const items = await listSubmissions(5);
    res.status(200).json({ ok: true, diag, sampleCount: items.length, sample: items });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
}



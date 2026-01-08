import { promises as fs } from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const SCRIPT_PATH = path.join(__dirname, "..", "lib", "admin-client.js");

export default async function handler(req, res) {
    if (req.method !== "GET") {
        res.status(405).send("Method not allowed");
        return;
    }
    try {
        const script = await fs.readFile(SCRIPT_PATH, "utf8");
        res.setHeader("Content-Type", "application/javascript; charset=utf-8");
        res.setHeader("Cache-Control", "no-store");
        res.status(200).send(script);
    } catch (error) {
        res.status(500).send(`// admin runtime failed: ${error?.message || String(error)}`);
    }
}

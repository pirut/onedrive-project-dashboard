import "isomorphic-fetch";
import Busboy from "busboy";

function readEnv(name, required = false) {
    const val = process.env[name];
    if (required && !val) throw new Error(`Missing env ${name}`);
    return val;
}

const USPS_CLIENT_ID = readEnv("USPS_CLIENT_ID", true);
const USPS_CLIENT_SECRET = readEnv("USPS_CLIENT_SECRET", true);
const USPS_SCOPE = readEnv("USPS_SCOPE", false);
const USPS_AUDIENCE = readEnv("USPS_AUDIENCE", false);
const USPS_API_BASE = readEnv("USPS_API_BASE") || "https://api.usps.com";
const USPS_TOKEN_URL = readEnv("USPS_TOKEN_URL") || `${USPS_API_BASE}/oauth2/v3/token`;
const USPS_VALIDATE_URL = readEnv("USPS_VALIDATE_URL") || `${USPS_API_BASE}/addresses/v3/address/validate`;

let cachedToken = null; // { token: string, expiresAt: number }

function decodeCsv(text) {
    const rows = [];
    let current = [];
    let value = "";
    let inQuotes = false;
    let i = 0;
    const len = text.length;
    while (i < len) {
        const ch = text[i];
        if (inQuotes) {
            if (ch === "\"") {
                if (text[i + 1] === "\"") {
                    value += "\"";
                    i += 1;
                } else {
                    inQuotes = false;
                }
            } else {
                value += ch;
            }
        } else if (ch === "\"") {
            inQuotes = true;
        } else if (ch === ",") {
            current.push(value);
            value = "";
        } else if (ch === "\n") {
            current.push(value);
            rows.push(current);
            current = [];
            value = "";
        } else if (ch === "\r") {
            // swallow CR; treat CRLF as LF
        } else {
            value += ch;
        }
        i += 1;
    }
    // push last value
    if (value.length > 0 || current.length > 0) {
        current.push(value);
    }
    if (current.length) rows.push(current);
    return rows
        .map((row) => row.map((cell) => cell.replace(/\ufeff/g, "").trim()))
        .filter((row) => row.some((cell) => cell !== ""));
}

function canonicalHeader(name = "") {
    return name
        .toLowerCase()
        .replace(/[^a-z0-9]/g, "");
}

function mapHeaders(headerRow) {
    const aliasMap = {
        address1: ["address1", "address", "addressline1", "line1", "street", "streetaddress", "addr1", "primaryaddress"],
        address2: ["address2", "addressline2", "line2", "secondaryaddress", "unit", "suite", "apt", "apt#", "addr2", "secondary"],
        city: ["city", "town", "municipality"],
        state: ["state", "statecode", "province", "region", "stateabbr", "stateabbreviation"],
        zip5: ["zip", "zipcode", "zip5", "postal", "postalcode", "postcode", "zip_code"],
        zip4: ["zip4", "plus4", "zipplus4", "zipcode4", "zip4code", "zip+4"],
        urbanization: ["urbanization", "urbanizacion"],
    };

    const indices = {};
    const canonHeaders = headerRow.map(canonicalHeader);

    for (const [key, aliases] of Object.entries(aliasMap)) {
        const idx = canonHeaders.findIndex((name) => aliases.includes(name));
        if (idx !== -1) indices[key] = idx;
    }

    return indices;
}

function columnValue(row, idx) {
    if (idx === undefined || idx === -1) return "";
    return (row[idx] || "").trim();
}

function parseCsvBuffer(buffer) {
    const text = buffer.toString("utf8");
    const rows = decodeCsv(text);
    if (!rows.length) return { header: [], records: [] };
    const [header, ...data] = rows;
    return { header, records: data };
}

function toCsv(rows, columns) {
    const header = columns.map((col) => col.label);
    const lines = [header.map(csvEscape).join(",")];
    for (const row of rows) {
        const line = columns.map((col) => csvEscape(typeof col.value === "function" ? col.value(row) : row[col.key] ?? ""));
        lines.push(line.join(","));
    }
    return lines.join("\r\n");
}

function csvEscape(val) {
    if (val === null || val === undefined) return "";
    const str = String(val);
    if (/[",\n\r]/.test(str)) return `"${str.replaceAll("\"", "\"\"")}"`;
    return str;
}

function splitZip(zipRaw) {
    const clean = (zipRaw || "").replace(/[^0-9-]/g, "").trim();
    if (!clean) return { zip5: "", zip4: "" };
    if (clean.includes("-")) {
        const [zip5 = "", zip4 = ""] = clean.split("-", 2).map((part) => part.trim());
        return { zip5, zip4 };
    }
    if (clean.length === 9) return { zip5: clean.slice(0, 5), zip4: clean.slice(5) };
    if (clean.length > 5) return { zip5: clean.slice(0, 5), zip4: clean.slice(5, 9) };
    return { zip5: clean, zip4: "" };
}

async function getUspsToken(force = false) {
    if (!force && cachedToken && cachedToken.expiresAt > Date.now() + 60_000) {
        return cachedToken.token;
    }

    const params = new URLSearchParams({ grant_type: "client_credentials" });
    if (USPS_SCOPE) params.set("scope", USPS_SCOPE);
    if (USPS_AUDIENCE) params.set("audience", USPS_AUDIENCE);
    const basic = Buffer.from(`${USPS_CLIENT_ID}:${USPS_CLIENT_SECRET}`).toString("base64");

    const res = await fetch(USPS_TOKEN_URL, {
        method: "POST",
        headers: {
            "Content-Type": "application/x-www-form-urlencoded",
            Accept: "application/json",
            Authorization: `Basic ${basic}`,
            "x-ibm-client-id": USPS_CLIENT_ID,
            "x-ibm-client-secret": USPS_CLIENT_SECRET,
        },
        body: params,
    });

    if (!res.ok) {
        const text = await res.text();
        throw new Error(`USPS token request failed (${res.status}): ${text}`);
    }

    const data = await res.json();
    const token = data.access_token || data.accessToken;
    if (!token) throw new Error("USPS token response missing access_token");
    const expiresIn = Number(data.expires_in || data.expiresIn || 0) || 600;
    cachedToken = { token, expiresAt: Date.now() + expiresIn * 1000 };
    return token;
}

async function validateSingleAddress(address, attempt = 0) {
    const token = await getUspsToken(attempt > 0);
    const payloadAddress = {
        id: `row-${address.row}`,
        streetAddress: address.address1,
        addressLine1: address.address1,
        addressLine2: address.address2 || undefined,
        secondaryAddress: address.address2 || undefined,
        city: address.city || undefined,
        state: address.state || undefined,
        zipCode: address.zip5 || undefined,
        zipPlus4: address.zip4 || undefined,
        urbanization: address.urbanization || undefined,
    };

    const res = await fetch(USPS_VALIDATE_URL, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            Accept: "application/json",
            Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({ addresses: [payloadAddress] }),
    });

    if (res.status === 401 && attempt === 0) {
        cachedToken = null;
        return validateSingleAddress(address, attempt + 1);
    }

    const text = await res.text();
    let data;
    try {
        data = text ? JSON.parse(text) : null;
    } catch (err) {
        throw new Error(`USPS response parse error: ${err.message || err}`);
    }

    if (!res.ok) {
        const msg = data?.error?.message || data?.error_description || data?.message || text || `Status ${res.status}`;
        throw new Error(msg);
    }

    const entry = data?.addresses?.[0] || data?.address;
    if (!entry) {
        throw new Error("No address data returned by USPS");
    }

    const status = entry.result?.status || entry.status || entry.result || entry.addressStatus;
    const normalized = entry.standardizedAddress || entry.address || entry.validatedAddress || entry.deliveryAddress || entry;
    const dpv = entry.dpv || entry.dpvConfirmation || entry.dpvcodes || entry.dpvConfirm;
    const footnotes = entry.footnotes || (entry.dpv && entry.dpv.footnotes) || entry.result?.footnotes;

    const extract = (keys) => {
        for (const key of keys) {
            if (normalized && normalized[key]) return normalized[key];
            if (entry && entry[key]) return entry[key];
        }
        return "";
    };

    const address1 = extract(["addressLine1", "streetAddress", "deliveryLine1", "address1"]);
    const address2 = extract(["addressLine2", "deliveryLine2", "secondaryAddress", "address2"]);
    const city = extract(["city", "cityName"]);
    const state = extract(["state", "stateAbbreviation", "stateCode"]);
    const zip5 = extract(["zipCode", "zip5", "zip", "postalCode"]);
    const zip4 = extract(["zipPlus4", "zip4", "plus4"]);

    return {
        row: address.row,
        input: address,
        status: status || (entry.validated === true ? "validated" : ""),
        dpvConfirmation: typeof dpv === "object" ? dpv.dpvConfirmation || dpv.confirmation : dpv || "",
        footnotes: Array.isArray(footnotes) ? footnotes.join(" ") : footnotes || "",
        address1,
        address2,
        city,
        state,
        zip5,
        zip4,
    };
}

export const config = { api: { bodyParser: false } };

export default async function handler(req, res) {
    if (req.method === "OPTIONS") {
        res.setHeader("Access-Control-Allow-Origin", process.env.CORS_ORIGIN || "*");
        res.setHeader("Access-Control-Allow-Methods", "POST,OPTIONS");
        res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
        res.status(204).end();
        return;
    }

    if (req.method !== "POST") {
        res.status(405).json({ error: "Method not allowed" });
        return;
    }

    try {
        const { fileBuffer, filename } = await readMultipartFile(req);
        if (!fileBuffer) {
            res.status(400).json({ error: "CSV file is required" });
            return;
        }
        const { header, records } = parseCsvBuffer(fileBuffer);
        if (!records.length) {
            res.status(400).json({ error: "CSV contained no data rows" });
            return;
        }

        const headerMap = mapHeaders(header);
        if (headerMap.address1 === undefined) {
            res.status(400).json({ error: "CSV must include an address column (e.g. Address1, Street)" });
            return;
        }

        const inputs = [];
        records.forEach((row, idx) => {
            const rowNumber = idx + 2; // include header offset
            const address1 = columnValue(row, headerMap.address1);
            const address2 = columnValue(row, headerMap.address2);
            const city = columnValue(row, headerMap.city);
            const state = columnValue(row, headerMap.state);
            const urbanization = columnValue(row, headerMap.urbanization);
            const zip5Raw = columnValue(row, headerMap.zip5);
            const zip4Raw = columnValue(row, headerMap.zip4);
            const split = splitZip(zip5Raw);
            const zip5 = split.zip5;
            const zip4 = zip4Raw || split.zip4;

            const hasLocation = (city && state) || zip5;
            if (!address1 || !hasLocation) {
                inputs.push({
                    row: rowNumber,
                    address1,
                    address2,
                    city,
                    state,
                    zip5,
                    zip4,
                    urbanization,
                    error: "Missing required address / location fields",
                });
                return;
            }

            inputs.push({
                row: rowNumber,
                address1,
                address2,
                city,
                state,
                zip5,
                zip4,
                urbanization,
            });
        });

        const outputs = [];
        let successCount = 0;
        let errorCount = 0;

        for (const input of inputs) {
            if (input.error) {
                outputs.push({
                    row: input.row,
                    input_address1: input.address1,
                    input_address2: input.address2,
                    input_city: input.city,
                    input_state: input.state,
                    input_zip5: input.zip5,
                    input_zip4: input.zip4,
                    address1: "",
                    address2: "",
                    city: "",
                    state: "",
                    zip5: "",
                    zip4: "",
                    dpvConfirmation: "",
                    footnotes: "",
                    status: "",
                    error: input.error,
                });
                errorCount += 1;
                continue;
            }
            try {
                const validated = await validateSingleAddress(input);
                outputs.push({
                    row: validated.row,
                    input_address1: input.address1,
                    input_address2: input.address2,
                    input_city: input.city,
                    input_state: input.state,
                    input_zip5: input.zip5,
                    input_zip4: input.zip4,
                    address1: validated.address1,
                    address2: validated.address2,
                    city: validated.city,
                    state: validated.state,
                    zip5: validated.zip5,
                    zip4: validated.zip4,
                    dpvConfirmation: validated.dpvConfirmation,
                    footnotes: validated.footnotes,
                    status: validated.status || "success",
                    error: "",
                });
                successCount += 1;
            } catch (err) {
                outputs.push({
                    row: input.row,
                    input_address1: input.address1,
                    input_address2: input.address2,
                    input_city: input.city,
                    input_state: input.state,
                    input_zip5: input.zip5,
                    input_zip4: input.zip4,
                    address1: "",
                    address2: "",
                    city: "",
                    state: "",
                    zip5: "",
                    zip4: "",
                    dpvConfirmation: "",
                    footnotes: "",
                    status: "error",
                    error: err.message || String(err),
                });
                errorCount += 1;
            }
        }

        const columns = [
            { key: "row", label: "row" },
            { key: "input_address1", label: "input_address1" },
            { key: "input_address2", label: "input_address2" },
            { key: "input_city", label: "input_city" },
            { key: "input_state", label: "input_state" },
            { key: "input_zip5", label: "input_zip5" },
            { key: "input_zip4", label: "input_zip4" },
            { key: "address1", label: "standard_address1" },
            { key: "address2", label: "standard_address2" },
            { key: "city", label: "standard_city" },
            { key: "state", label: "standard_state" },
            { key: "zip5", label: "standard_zip5" },
            { key: "zip4", label: "standard_zip4" },
            { key: "dpvConfirmation", label: "dpv_confirmation" },
            { key: "footnotes", label: "footnotes" },
            { key: "status", label: "status" },
            { key: "error", label: "error" },
        ];
        const csv = toCsv(outputs, columns);

        res.status(200).json({
            ok: true,
            filename: filename ? `${filename.replace(/\.[^.]+$/, "") || "addresses"}-standardized.csv` : "addresses-standardized.csv",
            summary: { total: outputs.length, success: successCount, errors: errorCount },
            csv,
            rows: outputs,
        });
    } catch (err) {
        res.status(500).json({ error: err.message || String(err) });
    }
}

async function readMultipartFile(req) {
    return await new Promise((resolve, reject) => {
        const bb = new Busboy({ headers: req.headers });
        const chunks = [];
        let filename = null;

        bb.on("file", (_name, file, info) => {
            filename = info.filename;
            file.on("data", (data) => chunks.push(data));
        });
        bb.on("error", reject);
        bb.on("finish", () => {
            resolve({ fileBuffer: chunks.length ? Buffer.concat(chunks) : null, filename });
        });
        req.pipe(bb);
    });
}

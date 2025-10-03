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

const STATE_ABBREVIATIONS = {
    AL: "ALABAMA",
    AK: "ALASKA",
    AZ: "ARIZONA",
    AR: "ARKANSAS",
    CA: "CALIFORNIA",
    CO: "COLORADO",
    CT: "CONNECTICUT",
    DE: "DELAWARE",
    DC: "DISTRICT OF COLUMBIA",
    FL: "FLORIDA",
    GA: "GEORGIA",
    HI: "HAWAII",
    ID: "IDAHO",
    IL: "ILLINOIS",
    IN: "INDIANA",
    IA: "IOWA",
    KS: "KANSAS",
    KY: "KENTUCKY",
    LA: "LOUISIANA",
    ME: "MAINE",
    MD: "MARYLAND",
    MA: "MASSACHUSETTS",
    MI: "MICHIGAN",
    MN: "MINNESOTA",
    MS: "MISSISSIPPI",
    MO: "MISSOURI",
    MT: "MONTANA",
    NE: "NEBRASKA",
    NV: "NEVADA",
    NH: "NEW HAMPSHIRE",
    NJ: "NEW JERSEY",
    NM: "NEW MEXICO",
    NY: "NEW YORK",
    NC: "NORTH CAROLINA",
    ND: "NORTH DAKOTA",
    OH: "OHIO",
    OK: "OKLAHOMA",
    OR: "OREGON",
    PA: "PENNSYLVANIA",
    RI: "RHODE ISLAND",
    SC: "SOUTH CAROLINA",
    SD: "SOUTH DAKOTA",
    TN: "TENNESSEE",
    TX: "TEXAS",
    UT: "UTAH",
    VT: "VERMONT",
    VA: "VIRGINIA",
    WA: "WASHINGTON",
    WV: "WEST VIRGINIA",
    WI: "WISCONSIN",
    WY: "WYOMING",
};

const STATE_NAME_TO_CODE = Object.entries(STATE_ABBREVIATIONS).reduce((acc, [abbr, name]) => {
    acc[name] = abbr;
    return acc;
}, {});

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
        country: ["country", "countrycode", "nation", "nationcode"],
        full: ["fulladdress", "addressfull", "formattedaddress", "addressstring", "address_text"],
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

function splitStreetAndUnit(streetRaw = "") {
    const street = String(streetRaw || "").trim();
    if (!street) return { address1: "", address2: "" };
    const unitMatch = street.match(/^(.*?)(?:\s+(Apt|Apartment|Unit|Suite|Ste|Floor|Fl|Bldg|Building|Room|Rm|#)\.?\s*(.+))$/i);
    if (unitMatch) {
        const base = unitMatch[1].trim();
        const label = unitMatch[2] || "";
        const rest = unitMatch[3] ? `${label} ${unitMatch[3]}` : label;
        return { address1: base, address2: rest.trim() };
    }
    const hashIndex = street.lastIndexOf("#");
    if (hashIndex > 0 && hashIndex >= street.length - 10) {
        const base = street.slice(0, hashIndex).trim();
        const rest = street.slice(hashIndex).trim();
        if (base && rest) return { address1: base, address2: rest };
    }
    return { address1: street, address2: "" };
}

function normalizeCountry(countryRaw) {
    const val = String(countryRaw || "").trim();
    if (!val) return "";
    const upper = val.toUpperCase();
    if (["US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA", "UNITEDSTATES"].includes(upper)) return "US";
    if (["CA", "CAN", "CANADA"].includes(upper)) return "CA";
    if (/^[A-Z]{2}$/.test(upper)) return upper;
    return val;
}

function normalizeState(value) {
    if (!value) return "";
    const raw = String(value).trim();
    if (!raw) return "";
    const upper = raw.toUpperCase();
    if (STATE_ABBREVIATIONS[upper]) return upper;
    const compact = upper.replace(/\./g, "");
    if (STATE_ABBREVIATIONS[compact]) return compact;
    const collapsed = upper.replace(/\s+/g, " ");
    if (STATE_NAME_TO_CODE[collapsed]) return STATE_NAME_TO_CODE[collapsed];
    if (STATE_NAME_TO_CODE[upper.replace(/\s+/g, "")]) return STATE_NAME_TO_CODE[upper.replace(/\s+/g, "")];
    return "";
}

function consumeStateFromTokens(tokens) {
    if (!Array.isArray(tokens) || !tokens.length) return null;
    const maxLen = Math.min(3, tokens.length);
    for (let len = maxLen; len >= 1; len -= 1) {
        const candidateTokens = tokens.slice(-len);
        const candidate = candidateTokens.join(" ");
        const abbr = normalizeState(candidate);
        if (abbr) {
            tokens.splice(tokens.length - len, len);
            return abbr;
        }
    }
    return null;
}

function enrichFromSingleLine(raw, seed) {
    const result = { ...seed };
    const cleaned = String(raw || "").replace(/\s+/g, " ").trim();
    if (!cleaned) return result;

    const segments = cleaned.split(",").map((segment) => segment.trim()).filter(Boolean);
    let streetSegment = segments[0] || cleaned;
    const remainderSegments = segments.length > 1 ? segments.slice(1) : [];

    const { address1: baseAddress1, address2: baseAddress2 } = splitStreetAndUnit(streetSegment);
    if (baseAddress1) result.address1 = baseAddress1;
    if (!result.address2 && baseAddress2) result.address2 = baseAddress2;

    let lastSegment = remainderSegments.length ? remainderSegments[remainderSegments.length - 1] : "";
    let midSegments = remainderSegments.length > 1 ? remainderSegments.slice(0, -1) : [];

    if (!lastSegment && segments.length === 1) lastSegment = segments[0];

    let tokens = lastSegment ? lastSegment.split(/\s+/).filter(Boolean) : [];
    const knownCountry = /^(?:US|USA|UNITED STATES|UNITED STATES OF AMERICA|CAN|CANADA)$/i;

    for (let i = tokens.length - 1; i >= 0; i -= 1) {
        const token = tokens[i];
        if (!result.zip5 && /^\d{5}(?:-?\d{4})?$/.test(token)) {
            const normalizedZip = splitZip(token);
            result.zip5 = normalizedZip.zip5;
            if (!result.zip4 && normalizedZip.zip4) result.zip4 = normalizedZip.zip4;
            tokens.splice(i, 1);
            continue;
        }
        if (!result.state) {
            if (/^[A-Za-z]{2}$/.test(token)) {
                const normalizedState = normalizeState(token);
                if (normalizedState) {
                    result.state = normalizedState;
                    tokens.splice(i, 1);
                    continue;
                }
            }
            const normalizedState = normalizeState(token);
            if (normalizedState) {
                result.state = normalizedState;
                tokens.splice(i, 1);
                continue;
            }
        }
        if (!result.country && knownCountry.test(token)) {
            result.country = normalizeCountry(token);
            tokens.splice(i, 1);
            continue;
        }
    }

    if (!result.state) {
        const consumedState = consumeStateFromTokens(tokens);
        if (consumedState) result.state = consumedState;
    }

    const leftover = tokens.join(" ").trim();
    if (!result.city && leftover) {
        result.city = leftover;
    } else if (leftover && !result.country && /^[A-Za-z ]{3,}$/.test(leftover)) {
        result.country = leftover;
    }

    if (!result.city && midSegments.length) {
        result.city = midSegments.join(", ");
    }

    if (!result.country && remainderSegments.length >= 2) {
        const maybeCountry = remainderSegments[remainderSegments.length - 1];
        const normalizedCountry = normalizeCountry(maybeCountry);
        if (normalizedCountry) result.country = normalizedCountry;
    }

    if (!result.country) result.country = normalizeCountry(result.country) || "US";
    else result.country = normalizeCountry(result.country);

    return result;
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
    const countryCode = normalizeCountry(address.country) || undefined;
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
        country: countryCode,
        countryCode,
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
    const country = normalizeCountry(
        extract(["countryCode", "country", "countryName", "originCountry", "destinationCountry"])
    );

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
        country: country || (countryCode || "US"),
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
        if (headerMap.address1 === undefined && headerMap.full === undefined) {
            res.status(400).json({ error: "CSV must include an address column (e.g. Address1, Street, FullAddress)" });
            return;
        }

        const inputs = [];
        records.forEach((row, idx) => {
            const rowNumber = idx + 2; // include header offset
            const address1Raw = columnValue(row, headerMap.address1);
            const address2Raw = columnValue(row, headerMap.address2);
            const cityRaw = columnValue(row, headerMap.city);
            const stateRaw = columnValue(row, headerMap.state);
            const urbanization = columnValue(row, headerMap.urbanization);
            const zip5Raw = columnValue(row, headerMap.zip5);
            const zip4Raw = columnValue(row, headerMap.zip4);
            const countryRaw = columnValue(row, headerMap.country);
            const fullRaw = columnValue(row, headerMap.full);

            let data = {
                address1: address1Raw || "",
                address2: address2Raw || "",
                city: cityRaw || "",
                state: stateRaw || "",
                zip5: zip5Raw || "",
                zip4: zip4Raw || "",
                country: countryRaw || "",
                urbanization,
            };

            if (data.state) data.state = normalizeState(data.state) || data.state;
            if (!data.address1 && fullRaw) {
                data.address1 = fullRaw;
            }

            const candidates = [data.address1, fullRaw, [address1Raw, address2Raw].filter(Boolean).join(" ")]
                .filter(Boolean)
                .map((candidate) => candidate.trim())
                .filter(Boolean);
            const seenCandidates = new Set();
            for (const candidate of candidates) {
                const key = candidate.toLowerCase();
                if (seenCandidates.has(key)) continue;
                seenCandidates.add(key);
                data = enrichFromSingleLine(candidate, data);
                if (data.city && data.state && data.zip5) break;
            }

            const zipNormalized = splitZip(data.zip5 || data.zip4);
            data.zip5 = zipNormalized.zip5;
            if (!data.zip4) data.zip4 = zipNormalized.zip4;
            data.country = normalizeCountry(data.country) || "US";

            const hasLocation = (data.city && data.state) || data.zip5;
            if (!data.address1 || !hasLocation) {
                inputs.push({
                    row: rowNumber,
                    address1: data.address1,
                    address2: data.address2,
                    city: data.city,
                    state: data.state,
                    zip5: data.zip5,
                    zip4: data.zip4,
                    country: data.country,
                    urbanization,
                    error: "Missing required address / location fields",
                });
                return;
            }

            inputs.push({
                row: rowNumber,
                address1: data.address1,
                address2: data.address2,
                city: data.city,
                state: data.state,
                zip5: data.zip5,
                zip4: data.zip4,
                country: data.country,
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
                    input_country: input.country,
                    address1: "",
                    address2: "",
                    city: "",
                    state: "",
                    zip5: "",
                    zip4: "",
                    country: "",
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
                    input_country: input.country,
                    address1: validated.address1,
                    address2: validated.address2,
                    city: validated.city,
                    state: validated.state,
                    zip5: validated.zip5,
                    zip4: validated.zip4,
                    country: validated.country || input.country || "US",
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
                    input_country: input.country,
                    address1: "",
                    address2: "",
                    city: "",
                    state: "",
                    zip5: "",
                    zip4: "",
                    country: "",
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
            { key: "input_country", label: "input_country" },
            { key: "address1", label: "standard_address1" },
            { key: "address2", label: "standard_address2" },
            { key: "city", label: "standard_city" },
            { key: "state", label: "standard_state" },
            { key: "zip5", label: "standard_zip5" },
            { key: "zip4", label: "standard_zip4" },
            { key: "country", label: "standard_country" },
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
        const bb = Busboy({ headers: req.headers });
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

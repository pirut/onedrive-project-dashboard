import crypto from "crypto";
import {
    getAppToken,
    resolveDrive,
    getParentItemId,
    graphFetch,
    moveFileFromStaging,
} from "../api/ingest-pdf.js";

function buildFilenameCandidates(rawName) {
    const candidates = [];
    const add = (name) => {
        const trimmed = String(name || "").trim();
        if (trimmed && !candidates.includes(trimmed)) candidates.push(trimmed);
    };
    const base = rawName || "";
    if (base) add(base);
    if (base && !/\.pdf$/i.test(base)) add(`${base}.pdf`);
    if (base) {
        const sanitized = base.replace(/[\\/:*?"<>|]/g, " ").replace(/\s+/g, " ").trim();
        if (sanitized && sanitized !== base) {
            add(sanitized);
            if (!/\.pdf$/i.test(sanitized)) add(`${sanitized}.pdf`);
        }
    }
    const lower = base ? base.toLowerCase() : "";
    if (lower && !/\.pdf$/i.test(lower)) add(`${lower}.pdf`);
    return candidates.length ? candidates : [rawName];
}

function ensureEnv(name, fallback = undefined) {
    const val = process.env[name];
    if (val) return val;
    if (fallback !== undefined) return fallback;
    throw new Error(`Missing environment variable: ${name}`);
}

function isMissingDestinationFolderError(err) {
    if (!err) return false;
    if (err.phase === "find_main_folder") return true;
    const message = String(err.message || "").toLowerCase();
    return message.includes("target folder not found under library");
}

export async function processStagingJobWalks({
    stagingSiteUrl = process.env.FASTFIELD_STAGING_SITE_URL || process.env.DEFAULT_SITE_URL,
    stagingLibraryPath = process.env.FASTFIELD_STAGING_LIBRARY_PATH,
    destinationSiteUrl = process.env.DEFAULT_SITE_URL,
    destinationLibraryPath = process.env.DEFAULT_LIBRARY,
    onResult = () => {},
    treatMissingFolderAsSkip = true,
} = {}) {
    if (!stagingLibraryPath) throw new Error("FASTFIELD_STAGING_LIBRARY_PATH is required");
    if (!destinationSiteUrl || !destinationLibraryPath) {
        throw new Error("DEFAULT_SITE_URL and DEFAULT_LIBRARY must be configured");
    }

    const token = await getAppToken();
    const { drive, subPathParts } = await resolveDrive(token, stagingSiteUrl, stagingLibraryPath);
    let parentId = "root";
    if (subPathParts.length) {
        parentId = await getParentItemId(token, drive.id, subPathParts);
    }

    const listPath =
        parentId === "root"
            ? `/drives/${drive.id}/root/children`
            : `/drives/${drive.id}/items/${parentId}/children`;
    const listing = await graphFetch(listPath, token);
    const pdfFiles = (listing.value || []).filter((item) => item.file && /\.pdf$/i.test(item.name || ""));

    const summary = [];
    const skipped = [];
    const errors = [];

    if (!pdfFiles.length) {
        return { ok: true, processed: summary, skipped, errors, filesScanned: 0 };
    }

    for (const file of pdfFiles) {
        const traceId = crypto.randomBytes(8).toString("hex");
        const steps = [];
        const push = (msg, meta = {}) => {
            steps.push({ ts: new Date().toISOString(), msg, ...meta });
        };
        let phase = "start";
        try {
            const moveResult = await moveFileFromStaging({
                stagingSiteUrl,
                stagingLibraryPath,
                stagingFilename: file.name,
                stagingFilenames: buildFilenameCandidates(file.name),
                destinationSiteUrl,
                destinationLibraryPath,
                push,
                setPhase: (p) => {
                    phase = p;
                },
            });
            const record = { traceId, filename: moveResult.filename, folderName: moveResult.folderName, steps };
            summary.push(record);
            onResult({ status: "ok", ...record });
        } catch (err) {
            if (treatMissingFolderAsSkip && isMissingDestinationFolderError(err)) {
                const skipRecord = {
                    traceId,
                    filename: file.name,
                    reason: err?.message || String(err),
                    phase,
                    steps,
                };
                skipped.push(skipRecord);
                onResult({ status: "skipped", ...skipRecord });
                continue;
            }

            const record = {
                traceId,
                filename: file.name,
                error: err?.message || String(err),
                phase,
                steps,
            };
            errors.push(record);
            onResult({ status: "error", ...record });
        }
    }

    return { ok: errors.length === 0, processed: summary, skipped, errors, filesScanned: pdfFiles.length };
}

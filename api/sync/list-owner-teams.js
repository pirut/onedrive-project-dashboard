import { DataverseClient } from "../../lib/dataverse-client.js";
import { logger } from "../../lib/logger.js";

function readString(value) {
    return typeof value === "string" ? value.trim() : "";
}

function mapTeam(row) {
    const teamId = readString(row?.teamid);
    const name = readString(row?.name);
    const teamTypeRaw = row?.teamtype;
    const teamType = Number.isFinite(Number(teamTypeRaw)) ? Number(teamTypeRaw) : null;
    const aadObjectId =
        readString(row?.azureactivedirectoryobjectid) ||
        readString(row?.aadobjectid) ||
        readString(row?.msdyn_aadobjectid) ||
        readString(row?.msdyn_azureactivedirectoryobjectid);
    return {
        teamId: teamId || null,
        name: name || null,
        teamType,
        aadObjectId: aadObjectId || null,
    };
}

async function listTeams(dataverse) {
    const selectVariants = [
        ["teamid", "name", "teamtype", "azureactivedirectoryobjectid", "aadobjectid"],
        ["teamid", "name", "teamtype", "azureactivedirectoryobjectid"],
        ["teamid", "name", "teamtype"],
    ];
    for (const select of selectVariants) {
        try {
            const response = await dataverse.list("teams", {
                select,
                orderBy: "name asc",
                top: 500,
            });
            return {
                teams: (response.value || []).map(mapTeam).filter((team) => team.teamId),
                selectUsed: select,
            };
        } catch (error) {
            const message = (error && error.message) || String(error || "");
            if (message.includes("Could not find a property named")) {
                continue;
            }
            throw error;
        }
    }
    return { teams: [], selectUsed: [] };
}

export default async function handler(req, res) {
    res.setHeader("Cache-Control", "no-store");
    if (req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }
    try {
        const dataverse = new DataverseClient();
        const q = readString(req.query?.q || "").toLowerCase();
        const includeAll = String(req.query?.includeAll || "").trim() === "1";
        const result = await listTeams(dataverse);
        let teams = result.teams;
        if (!includeAll) {
            teams = teams.filter((team) => team.aadObjectId);
        }
        if (q) {
            teams = teams.filter((team) => String(team.name || "").toLowerCase().includes(q));
        }
        res.status(200).json({
            ok: true,
            total: teams.length,
            includeAll,
            filteredBy: q || null,
            selectUsed: result.selectUsed,
            teams,
        });
    } catch (error) {
        const message = (error && error.message) || String(error);
        logger.error("List Dataverse teams failed", { error: message });
        res.status(500).json({ ok: false, error: message });
    }
}


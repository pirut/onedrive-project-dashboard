import { DataverseClient } from "../../../../../lib/dataverse-client";
import { logger } from "../../../../../lib/logger";
import type { DataverseEntity } from "../../../../../lib/dataverse-client";

function readString(value: unknown) {
    return typeof value === "string" ? value.trim() : "";
}

function mapTeam(row: DataverseEntity) {
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

async function listTeams(dataverse: DataverseClient) {
    const selectVariants = [
        ["teamid", "name", "teamtype", "azureactivedirectoryobjectid", "aadobjectid"],
        ["teamid", "name", "teamtype", "azureactivedirectoryobjectid"],
        ["teamid", "name", "teamtype"],
    ];
    for (const select of selectVariants) {
        try {
            const response = await dataverse.list<DataverseEntity>("teams", {
                select,
                orderBy: "name asc",
                top: 500,
            });
            return {
                teams: (response.value || []).map(mapTeam).filter((team) => team.teamId),
                selectUsed: select,
            };
        } catch (error) {
            const message = (error as Error)?.message || String(error || "");
            if (message.includes("Could not find a property named")) {
                continue;
            }
            throw error;
        }
    }
    return { teams: [], selectUsed: [] as string[] };
}

export async function GET(request: Request) {
    try {
        const url = new URL(request.url);
        const q = readString(url.searchParams.get("q") || "").toLowerCase();
        const includeAll = String(url.searchParams.get("includeAll") || "").trim() === "1";
        const dataverse = new DataverseClient();
        const result = await listTeams(dataverse);
        let teams = result.teams;
        if (!includeAll) {
            teams = teams.filter((team) => team.aadObjectId);
        }
        if (q) {
            teams = teams.filter((team) => String(team.name || "").toLowerCase().includes(q));
        }
        return new Response(
            JSON.stringify(
                {
                    ok: true,
                    total: teams.length,
                    includeAll,
                    filteredBy: q || null,
                    selectUsed: result.selectUsed,
                    teams,
                },
                null,
                2
            ),
            {
                status: 200,
                headers: { "Content-Type": "application/json" },
            }
        );
    } catch (error) {
        const message = (error as Error)?.message || String(error);
        logger.error("List Dataverse teams failed", { error: message });
        return new Response(JSON.stringify({ ok: false, error: message }, null, 2), {
            status: 500,
            headers: { "Content-Type": "application/json" },
        });
    }
}


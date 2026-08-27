import { describe, expect, it } from "vitest";
import type { CdcSinkTableConfig } from "@/api/generated/server-api";
import { parseConfigFile, parseConfigTables } from "@/pages/setup/add-app-wizard/config-io";

function configFile(config: unknown) {
    return new File([JSON.stringify(config)], "Quill-app-config.json", { type: "application/json" });
}

function validConfig(overrides: Record<string, unknown> = {}) {
    return {
        provider: "Npgsql",
        connectionString: "Host=localhost;Database=northwind",
        tables: [{ collectionName: "Customers" }],
        ...overrides,
    };
}

function rootTable(collectionName: string): CdcSinkTableConfig {
    return {
        collectionName,
        sourceTableSchema: "public",
        sourceTableName: "customers",
        primaryKeyColumns: ["id"],
        columns: [{ column: "id", name: "Id", type: "Default" }],
        patch: null,
        onDelete: null,
        disabled: false,
        embeddedTables: [],
        linkedTables: [],
    } as unknown as CdcSinkTableConfig;
}

describe("parseConfigFile provider", () => {
    it("names the accepted providers when the file uses the product's own wording", async () => {
        const error = await parseConfigFile(configFile(validConfig({ provider: "PostgreSQL" }))).catch(
            (e: Error) => e.message,
        );

        expect(error).toContain("Npgsql");
        expect(error).toContain("SqlClient");
        expect(error).toContain("MySqlConnectorFactory");
        // the ADO factory names are unguessable from a UI that calls the same thing PostgreSQL
        expect(error).toContain("PostgreSQL");
    });

    it("names the accepted providers when the key is missing entirely", async () => {
        const error = await parseConfigFile(
            configFile({ connectionString: "Host=localhost", tables: [{ collectionName: "Customers" }] }),
        ).catch((e: Error) => e.message);

        expect(error).toContain("Npgsql");
    });
});

describe("parseConfigFile connection string", () => {
    it("reports a missing connection string in the wizard's own words", async () => {
        const error = await parseConfigFile(
            configFile({ provider: "Npgsql", tables: [{ collectionName: "Customers" }] }),
        ).catch((e: Error) => e.message);

        expect(error).toBe("The configuration is missing a connection string.");
    });

    it("reports an empty connection string the same way", async () => {
        const error = await parseConfigFile(configFile(validConfig({ connectionString: "   " }))).catch(
            (e: Error) => e.message,
        );

        expect(error).toBe("The configuration is missing a connection string.");
    });
});

describe("parseConfigTables", () => {
    it("reports what the mapping schema objected to, not that something is wrong somewhere", () => {
        const duplicated = [rootTable("Customers"), rootTable("Customers")];

        expect(() => parseConfigTables(duplicated)).toThrowError(/Customers/);
    });

    it("accepts a mapping the wizard would accept", () => {
        expect(parseConfigTables([rootTable("Customers")])).toHaveLength(1);
    });
});

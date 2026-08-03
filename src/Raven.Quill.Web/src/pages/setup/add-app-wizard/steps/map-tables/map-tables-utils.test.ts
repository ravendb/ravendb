import type {
    DiscoverColumnResponse,
    DiscoverForeignKeyResponse,
    DiscoverTableResponse,
} from "@/api/generated/server-api";
import {
    propertyNameFromJoinColumn,
    scaffoldRootTable,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { describe, expect, it } from "vitest";

function table(overrides: Partial<DiscoverTableResponse> = {}): DiscoverTableResponse {
    return {
        sourceTableSchema: "public",
        sourceTableName: "Customer",
        primaryKeyColumns: ["Id"],
        columns: [],
        foreignKeys: [],
        isCdcEnabled: true,
        warnings: [],
        ...overrides,
    };
}

function column(name: string): DiscoverColumnResponse {
    return {
        name,
        nativeType: "int",
        suggestedType: "Default",
        isPrimaryKey: false,
        isCdcCapturable: true,
    };
}

function foreignKey(columns: string[], referencedTable: string): DiscoverForeignKeyResponse {
    return {
        columns,
        referencedSchema: "public",
        referencedTable,
        referencedColumns: ["Id"],
    };
}

describe("propertyNameFromJoinColumn", () => {
    it("strips a snake_case id suffix", () => {
        expect(propertyNameFromJoinColumn("customer_id")).toBe("Customer");
        expect(propertyNameFromJoinColumn("billing_address_id")).toBe("BillingAddress");
    });

    it("strips a PascalCase id suffix", () => {
        expect(propertyNameFromJoinColumn("CustomerId")).toBe("Customer");
        expect(propertyNameFromJoinColumn("HubRefId")).toBe("HubRef");
    });

    it("strips an all-caps ID suffix", () => {
        expect(propertyNameFromJoinColumn("CustomerID")).toBe("Customer");
        expect(propertyNameFromJoinColumn("CUSTOMER_ID")).toBe("CUSTOMER");
    });

    it("keeps names that only end in the letters id", () => {
        expect(propertyNameFromJoinColumn("uuid")).toBe("Uuid");
        expect(propertyNameFromJoinColumn("UUID")).toBe("UUID");
    });

    it("keeps a bare id column", () => {
        expect(propertyNameFromJoinColumn("id")).toBe("Id");
        expect(propertyNameFromJoinColumn("_id")).toBe("Id");
    });
});

describe("scaffoldRootTable", () => {
    it("names a link after the referenced entity, not after the foreign key column", () => {
        const scaffolded = scaffoldRootTable(
            null,
            table({
                columns: [column("Id"), column("LanguageId")],
                foreignKeys: [foreignKey(["LanguageId"], "Language")],
            }),
        );

        expect(scaffolded.columns.map((mapping) => mapping.name)).toEqual(["Id", "LanguageId"]);
        expect(scaffolded.linkedTables.map((linked) => linked.propertyName)).toEqual(["Language"]);
    });

    it("renames a link whose property name would shadow a column mapping", () => {
        const scaffolded = scaffoldRootTable(
            null,
            table({
                columns: [column("Id"), column("Language"), column("LanguageId")],
                foreignKeys: [foreignKey(["LanguageId"], "Language")],
            }),
        );

        expect(scaffolded.linkedTables.map((linked) => linked.propertyName)).toEqual(["Language2"]);
    });

    it("renames a link whose property name would shadow an earlier link", () => {
        const scaffolded = scaffoldRootTable(
            null,
            table({
                columns: [column("Id"), column("LanguageId"), column("language_id")],
                foreignKeys: [foreignKey(["LanguageId"], "Language"), foreignKey(["language_id"], "Language")],
            }),
        );

        expect(scaffolded.linkedTables.map((linked) => linked.propertyName)).toEqual(["Language", "Language2"]);
    });

    it("joins a composite foreign key into one property name", () => {
        const scaffolded = scaffoldRootTable(
            null,
            table({
                columns: [column("StoreId"), column("LanguageId")],
                foreignKeys: [foreignKey(["StoreId", "LanguageId"], "StoreLanguage")],
            }),
        );

        expect(scaffolded.linkedTables.map((linked) => linked.propertyName)).toEqual(["StoreAndLanguage"]);
    });
});

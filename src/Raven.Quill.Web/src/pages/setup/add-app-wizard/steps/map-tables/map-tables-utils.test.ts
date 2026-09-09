import type {
    DiscoverColumnResponse,
    DiscoverForeignKeyResponse,
    DiscoverTableResponse,
} from "@/api/generated/server-api";
import {
    collectMappedSourceTableKeys,
    collectMappedSourceTables,
    createEmptyEmbeddedTable,
    createEmptyRootTable,
    makeUniquePropertyName,
    propertyNameFromJoinColumn,
    scaffoldRootTable,
    toTakenPropertyNames,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import type { FormEmbeddedTable, FormRootTable } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
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

function rootTable(name: string, overrides: Partial<FormRootTable> = {}): FormRootTable {
    return { ...createEmptyRootTable(), sourceTableSchema: "public", sourceTableName: name, ...overrides };
}

function embeddedTable(name: string, overrides: Partial<FormEmbeddedTable> = {}): FormEmbeddedTable {
    return { ...createEmptyEmbeddedTable(), sourceTableSchema: "public", sourceTableName: name, ...overrides };
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

describe("makeUniquePropertyName", () => {
    it("keeps a name nothing else claims", () => {
        expect(makeUniquePropertyName("Language", toTakenPropertyNames(["Id", "Email"]))).toBe("Language");
    });

    it("suffixes a taken name", () => {
        expect(makeUniquePropertyName("Language", toTakenPropertyNames(["Language"]))).toBe("Language2");
    });

    it("keeps counting past an already suffixed name", () => {
        expect(makeUniquePropertyName("Language", toTakenPropertyNames(["Language", "Language2"]))).toBe("Language3");
    });

    it("compares case-insensitively, like the server", () => {
        expect(makeUniquePropertyName("Language", toTakenPropertyNames(["LANGUAGE"]))).toBe("Language2");
    });
});

describe("collectMappedSourceTables", () => {
    const collectNames = (tables: FormRootTable[]) =>
        collectMappedSourceTables(tables).map((mapped) => mapped.sourceTableName);

    it("collects root tables and nested embedded tables", () => {
        const tables = [
            rootTable("Customer", {
                embeddedTables: [embeddedTable("Address", { embeddedTables: [embeddedTable("Country")] })],
            }),
            rootTable("Order"),
        ];

        expect(collectNames(tables)).toEqual(["Customer", "Address", "Country", "Order"]);
    });

    it("skips linked tables - a link needs its own root mapping to be captured", () => {
        const tables = [
            rootTable("Order", {
                linkedTables: [
                    {
                        sourceTableSchema: "public",
                        sourceTableName: "Customer",
                        propertyName: "Customer",
                        joinColumns: [],
                        linkedCollectionName: "Customers",
                    },
                ],
            }),
        ];

        expect(collectNames(tables)).toEqual(["Order"]);
    });

    it("skips disabled roots including their embedded tables", () => {
        const tables = [
            rootTable("Customer", { disabled: true, embeddedTables: [embeddedTable("Address")] }),
            rootTable("Order"),
        ];

        expect(collectNames(tables)).toEqual(["Order"]);
    });

    it("dedupes tables case-insensitively and drops unnamed ones", () => {
        const tables = [
            rootTable("Customer", { embeddedTables: [embeddedTable("CUSTOMER"), embeddedTable("")] }),
            rootTable("customer"),
        ];

        expect(collectMappedSourceTables(tables)).toEqual([
            { sourceTableSchema: "public", sourceTableName: "Customer" },
        ]);
    });

    it("trims names and schemas, so a stray space cannot reach the server", () => {
        const tables = [rootTable(" Order ", { sourceTableSchema: " sales " })];

        expect(collectMappedSourceTables(tables)).toEqual([{ sourceTableSchema: "sales", sourceTableName: "Order" }]);
    });

    it("reports a blank schema as none, matching how the key treats it", () => {
        const tables = [rootTable("Order", { sourceTableSchema: "  " })];

        expect(collectMappedSourceTables(tables)).toEqual([{ sourceTableSchema: null, sourceTableName: "Order" }]);
    });
});

describe("collectMappedSourceTableKeys", () => {
    it("keeps disabled roots, so the unmapped alert does not offer to map them again", () => {
        const tables = [rootTable("Customer", { disabled: true, embeddedTables: [embeddedTable("Address")] })];

        expect([...collectMappedSourceTableKeys(tables)]).toEqual(["public::customer", "public::address"]);
    });
});

describe("toTakenPropertyNames", () => {
    it("drops blank entries and normalizes the rest", () => {
        expect(toTakenPropertyNames(["Language", " Currency ", "", null, undefined])).toEqual(
            new Set(["language", "currency"]),
        );
    });
});

import { tablesSchema } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { describe, expect, it } from "vitest";
import type { z } from "zod";

type FormTable = z.input<typeof tablesSchema>[number];

function rootTable(overrides: Partial<FormTable> = {}): FormTable {
    return {
        collectionName: "Customers",
        sourceTableSchema: "public",
        sourceTableName: "Customer",
        columns: [
            { column: "Id", name: "Id", type: "Default" },
            { column: "LanguageId", name: "LanguageId", type: "Default" },
        ],
        primaryKeyColumns: [{ value: "Id" }],
        patch: null,
        onDelete: null,
        disabled: false,
        embeddedTables: [],
        linkedTables: [],
        ...overrides,
    };
}

function linkedTable(propertyName: string) {
    return {
        sourceTableSchema: "public",
        sourceTableName: "Language",
        propertyName,
        joinColumns: [{ value: "LanguageId" }],
        linkedCollectionName: "Languages",
    };
}

function parseIssues(tables: FormTable[]) {
    const result = tablesSchema.safeParse(tables);

    return result.success ? [] : result.error.issues.map((issue) => ({ ...issue, path: issue.path.join(".") }));
}

describe("tablesSchema property names", () => {
    it("accepts a link whose property name does not collide", () => {
        expect(parseIssues([rootTable({ linkedTables: [linkedTable("Language")] })])).toEqual([]);
    });

    it("rejects a link whose property name collides with a column mapping", () => {
        const issues = parseIssues([rootTable({ linkedTables: [linkedTable("LanguageId")] })]);

        expect(issues).toHaveLength(1);
        expect(issues[0].path).toBe("0.linkedTables.0.propertyName");
        expect(issues[0].message).toContain("already used");
    });

    it("compares property names case-insensitively, like the server", () => {
        const issues = parseIssues([rootTable({ linkedTables: [linkedTable("languageid")] })]);

        expect(issues.map((issue) => issue.path)).toEqual(["0.linkedTables.0.propertyName"]);
    });

    it("compares trimmed property names, and reports the trimmed name", () => {
        const issues = parseIssues([rootTable({ linkedTables: [linkedTable("  LanguageId  ")] })]);

        expect(issues.map((issue) => issue.path)).toEqual(["0.linkedTables.0.propertyName"]);
        expect(issues[0].message).toContain('"LanguageId"');
    });

    it("rejects a whitespace-only property name as missing", () => {
        const issues = parseIssues([rootTable({ linkedTables: [linkedTable("   ")] })]);

        expect(issues.map((issue) => issue.path)).toEqual(["0.linkedTables.0.propertyName"]);
        expect(issues[0].message).toBe("Property name is required");
    });

    it("flags the second of two links that share a property name", () => {
        const issues = parseIssues([rootTable({ linkedTables: [linkedTable("Language"), linkedTable("Language")] })]);

        expect(issues.map((issue) => issue.path)).toEqual(["0.linkedTables.1.propertyName"]);
    });

    it("flags a duplicate target property on the offending column", () => {
        const issues = parseIssues([
            rootTable({
                columns: [
                    { column: "Id", name: "Id", type: "Default" },
                    { column: "LanguageId", name: "Id", type: "Default" },
                ],
            }),
        ]);

        expect(issues.map((issue) => issue.path)).toEqual(["0.columns.1.name"]);
    });
});

import type { CdcSinkTableConfig } from "@/api/generated/server-api";
import { tablesSchema } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { mapDtoTablesToForm } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";
import { describe, expect, it } from "vitest";

describe("mapDtoTablesToForm", () => {
    it("fills values a configuration leaves out so it passes validation", () => {
        const sparseTables = [
            {
                collectionName: "Customers",
                sourceTableName: "Customer",
                columns: [{ column: "Id", name: "Id" }],
                primaryKeyColumns: ["Id"],
                embeddedTables: [
                    {
                        sourceTableName: "Address",
                        propertyName: "Addresses",
                        columns: [{ column: "Id", name: "Id" }],
                        primaryKeyColumns: ["Id"],
                        joinColumns: ["CustomerId"],
                        onDelete: { ignoreDeletes: true },
                    },
                ],
            },
        ] as CdcSinkTableConfig[];

        const result = tablesSchema.safeParse(mapDtoTablesToForm(sparseTables));

        expect(result.success).toBe(true);
        expect(result.data?.[0]).toMatchObject({
            sourceTableSchema: null,
            patch: null,
            onDelete: { ignoreDeletes: false, patch: null },
            disabled: false,
            linkedTables: [],
            columns: [{ column: "Id", name: "Id", type: "Default" }],
        });
        expect(result.data?.[0].embeddedTables[0]).toMatchObject({
            type: "Array",
            onDelete: { ignoreDeletes: true, patch: null },
            caseSensitiveKeys: false,
            embeddedTables: [],
        });
    });
});

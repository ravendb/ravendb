import { toRavenDbTableJson } from "@/pages/setup/add-app-wizard/steps/map/collection/ravendb-table-json";
import { describe, expect, it } from "vitest";

describe("toRavenDbTableJson", () => {
    it("writes the table the way RavenDB stores it", () => {
        const json = toRavenDbTableJson({
            collectionName: "Loans",
            sourceTableSchema: "public",
            sourceTableName: "m_loan",
            columns: [
                { column: "id", name: "LoanId", type: "Default" },
                { column: "terms", name: "Terms", type: "Json" },
            ],
            primaryKeyColumns: ["id"],
            patch: null,
            onDelete: { patch: null, ignoreDeletes: false },
            disabled: false,
            embeddedTables: [
                {
                    sourceTableName: "m_loan_charge",
                    propertyName: "Charges",
                    columns: [{ column: "amount", name: "Amount" }],
                    primaryKeyColumns: ["id"],
                    joinColumns: ["loan_id"],
                },
            ],
            linkedTables: [
                {
                    sourceTableSchema: "public",
                    sourceTableName: "m_client",
                    propertyName: "ClientId",
                    joinColumns: ["client_id"],
                    linkedCollectionName: "Clients",
                },
            ],
        });

        expect(JSON.stringify(json, null, 2)).toBe(
            JSON.stringify(
                {
                    CollectionName: "Loans",
                    SourceTableSchema: "public",
                    SourceTableName: "m_loan",
                    Columns: [
                        { Column: "id", Name: "LoanId" },
                        { Column: "terms", Name: "Terms", Type: "Json" },
                    ],
                    PrimaryKeyColumns: ["id"],
                    Patch: null,
                    OnDelete: { Patch: null, IgnoreDeletes: false },
                    Disabled: false,
                    EmbeddedTables: [
                        {
                            SourceTableSchema: null,
                            SourceTableName: "m_loan_charge",
                            PropertyName: "Charges",
                            Columns: [{ Column: "amount", Name: "Amount" }],
                            PrimaryKeyColumns: ["id"],
                            JoinColumns: ["loan_id"],
                            Type: "Array",
                            Patch: null,
                            OnDelete: null,
                            CaseSensitiveKeys: false,
                            EmbeddedTables: [],
                            LinkedTables: [],
                        },
                    ],
                    LinkedTables: [
                        {
                            SourceTableSchema: "public",
                            SourceTableName: "m_client",
                            PropertyName: "ClientId",
                            JoinColumns: ["client_id"],
                            LinkedCollectionName: "Clients",
                        },
                    ],
                },
                null,
                2,
            ),
        );
    });
});

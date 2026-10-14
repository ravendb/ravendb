import type { DiscoverTableResponse } from "@/api/generated/server-api";

type ColumnSpec = string | [name: string, nativeType: string];

/**
 * A discovered table for stories and tests. `Id` is its primary key, a column given by name alone is an
 * int, and each reference is a foreign key to a table in the same schema.
 */
export function discoveredTable(
    schema: string,
    name: string,
    columns: ColumnSpec[],
    references: Record<string, string> = {},
): DiscoverTableResponse {
    return {
        sourceTableSchema: schema,
        sourceTableName: name,
        columns: columns.map((column) => {
            const [columnName, nativeType] = typeof column === "string" ? [column, "int"] : column;

            return {
                name: columnName,
                nativeType,
                suggestedType: "Default",
                isPrimaryKey: columnName === "Id",
                isCdcCapturable: true,
            };
        }),
        primaryKeyColumns: ["Id"],
        foreignKeys: Object.entries(references).map(([column, referencedTable]) => ({
            columns: [column],
            referencedSchema: schema,
            referencedTable,
            referencedColumns: ["Id"],
        })),
        isCdcEnabled: true,
        warnings: [],
    };
}

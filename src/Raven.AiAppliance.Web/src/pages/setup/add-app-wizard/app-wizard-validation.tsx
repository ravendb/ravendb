import type { CdcColumnType, CdcSinkRelationType } from "@/api/generated/server-api";
import { z } from "zod";

// Mirrors the generated CdcSinkTableConfig graph. Kept in sync with the API enums via `satisfies`.
const COLUMN_TYPES = ["Default", "Json", "Attachment"] as const satisfies readonly CdcColumnType[];
const RELATION_TYPES = ["Array", "Map", "Value"] as const satisfies readonly CdcSinkRelationType[];

const hasUniqueValues = (values: Array<string | null | undefined>) => {
    const normalized = values.map((value) => value?.trim()).filter((value): value is string => Boolean(value));

    return new Set(normalized).size === normalized.length;
};

const getSourceTableKey = (table: { sourceTableSchema?: string | null; sourceTableName?: string | null }) => {
    const name = table.sourceTableName?.trim().toLowerCase();

    if (!name) {
        return null;
    }

    return `${table.sourceTableSchema?.trim().toLowerCase() ?? ""}::${name}`;
};

const getSourceTableLabel = (table: { sourceTableSchema?: string | null; sourceTableName?: string | null }) =>
    table.sourceTableSchema ? `${table.sourceTableSchema}.${table.sourceTableName}` : table.sourceTableName;

const requiredUniqueStringsSchema = (requiredMessage: string, uniqueMessage: string) =>
    z
        .array(z.string())
        .min(1, requiredMessage)
        .refine((values) => hasUniqueValues(values), { message: uniqueMessage });

const columnMappingSchema = z.object({
    column: z.string().min(1, "Source column is required"),
    name: z.string().min(1, "Target property name is required"),
    type: z.enum(COLUMN_TYPES),
});

const columnMappingsSchema = z
    .array(columnMappingSchema)
    .min(1, "At least one column mapping is required")
    .refine((columns) => hasUniqueValues(columns.map((column) => column.column)), {
        message: "Source columns must be unique",
    })
    .refine((columns) => hasUniqueValues(columns.map((column) => column.name)), {
        message: "Target properties must be unique",
    });

const onDeleteSchema = z.object({
    patch: z.string().nullable(),
    ignoreDeletes: z.boolean().optional(),
});

const linkedTableSchema = z.object({
    sourceTableSchema: z.string().nullable(),
    sourceTableName: z.string().min(1, "Source table name is required"),
    propertyName: z.string().min(1, "Property name is required"),
    joinColumns: requiredUniqueStringsSchema("At least one join column is required", "Join columns must be unique"),
    linkedCollectionName: z.string().min(1, "Linked collection name is required"),
});

const embeddedTableSchema = z.object({
    sourceTableSchema: z.string().nullable(),
    sourceTableName: z.string().min(1, "Source table name is required"),
    propertyName: z.string().min(1, "Property name is required"),
    columns: columnMappingsSchema,
    primaryKeyColumns: requiredUniqueStringsSchema(
        "At least one primary key column is required",
        "Primary key columns must be unique",
    ),
    joinColumns: requiredUniqueStringsSchema("At least one join column is required", "Join columns must be unique"),
    type: z.enum(RELATION_TYPES),
    patch: z.string().nullable(),
    onDelete: onDeleteSchema.nullable(),
    caseSensitiveKeys: z.boolean().optional(),
    linkedTables: z.array(linkedTableSchema),
    get embeddedTables() {
        return z.array(embeddedTableSchema);
    },
});

const tableSchema = z.object({
    collectionName: z.string().min(1, "Collection name is required"),
    sourceTableSchema: z.string().nullable(),
    sourceTableName: z.string().min(1, "Source table name is required"),
    columns: columnMappingsSchema,
    primaryKeyColumns: requiredUniqueStringsSchema(
        "At least one primary key column is required",
        "Primary key columns must be unique",
    ),
    patch: z.string().nullable(),
    onDelete: onDeleteSchema.nullable(),
    disabled: z.boolean(),
    embeddedTables: z.array(embeddedTableSchema),
    linkedTables: z.array(linkedTableSchema),
});

export const tablesSchema = z
    .array(tableSchema)
    .min(1, "At least one table is required")
    .refine((tables) => hasUniqueValues(tables.map((table) => table.collectionName)), {
        message: "Collection names must be unique",
    })
    .superRefine((tables, ctx) => {
        const countByKey = new Map<string, number>();

        for (const table of tables) {
            const key = getSourceTableKey(table);

            if (key) {
                countByKey.set(key, (countByKey.get(key) ?? 0) + 1);
            }
        }

        // A source table can be a root table only once; flag each duplicate on its own row.
        tables.forEach((table, index) => {
            const key = getSourceTableKey(table);

            if (!key || (countByKey.get(key) ?? 0) < 2) {
                return;
            }

            ctx.addIssue({
                code: "custom",
                path: [index, "sourceTableName"],
                message: `Source table "${getSourceTableLabel(table)}" is already configured as another root table. CDC Sink can process a source table only once.`,
            });
        });
    });

export const appSchema = z.object({
    dataSource: z.object({
        source: z.union([z.literal("external"), z.literal("ravendb")]),
    }),
    externalConnection: z.object({
        appName: z.string().trim().min(1, "Application name is required"),
        provider: z.union([z.literal("Npgsql"), z.literal("SqlClient"), z.literal("MySqlConnectorFactory")]),
        connectionString: z.string().trim().min(1, "Connection string is required."),
    }),
    verifySchema: z.object({
        tables: z.array(
            z.object({
                sourceTableSchema: z.string().nullable().optional(),
                sourceTableName: z.string(),
            }),
        ),
    }),
    map: z
        .object({
            source: z.union([z.literal("ai-suggested"), z.literal("manual")]),
            aiPrompt: z.string(),
        })
        .superRefine((map, ctx) => {
            if (map.source === "ai-suggested" && map.aiPrompt.trim().length === 0) {
                ctx.addIssue({
                    code: "custom",
                    path: ["aiPrompt"],
                    message: "AI prompt is required",
                });
            }
        }),
    mapAiSuggest: z.object({
        tables: tablesSchema,
    }),
    mapManual: z.object({
        tables: tablesSchema,
    }),
    preview: z.object({
        table: z.string(),
    }),
});

export type AppFormData = z.infer<typeof appSchema>;
export type AppStepId = keyof AppFormData;

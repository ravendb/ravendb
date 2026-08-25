import type { CdcColumnType, CdcSinkRelationType } from "@/api/generated/server-api";
import { z } from "zod";
import { MAX_SELECTED_TABLES } from "@/pages/setup/add-app-wizard/discover-utils";
import { MAX_SLUG_LENGTH, toSlug } from "@/pages/setup/add-app-wizard/slugify";

// Mirrors the generated CdcSinkTableConfig graph. Kept in sync with the API enums via `satisfies`.
const COLUMN_TYPES = ["Default", "Json", "Attachment"] as const satisfies readonly CdcColumnType[];
const RELATION_TYPES = ["Array", "Map", "Value"] as const satisfies readonly CdcSinkRelationType[];

// Optional override; when empty the server derives the slug from the app name. Mirrors the
// server's normalization checks so obvious problems surface before provisioning (reserved
// names are only known server-side and come back as a 400).
export const slugSchema = z
    .string()
    .trim()
    .superRefine((value, ctx) => {
        if (value === "") {
            return;
        }

        const normalized = toSlug(value);

        if (normalized === "") {
            ctx.addIssue({ code: "custom", message: "Slug must contain at least one letter or digit (a–z, 0–9)" });
        } else if (normalized.length > MAX_SLUG_LENGTH) {
            ctx.addIssue({ code: "custom", message: `Slug must be at most ${MAX_SLUG_LENGTH} characters` });
        }
    });

export const providerSchema = z.union([
    z.literal("Npgsql"),
    z.literal("SqlClient"),
    z.literal("MySqlConnectorFactory"),
]);

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

/** String list in the FormStringList shape: useFieldArray needs object items, not plain strings. */
const requiredUniqueStringsSchema = (requiredMessage: string, uniqueMessage: string) =>
    z
        .array(z.object({ value: z.string().trim().min(1, "Value is required") }))
        .min(1, requiredMessage)
        .refine((items) => hasUniqueValues(items.map((item) => item.value)), { message: uniqueMessage });

const columnMappingSchema = z.object({
    column: z.string().trim().min(1, "Source column is required"),
    name: z.string().trim().min(1, "Target property name is required"),
    type: z.enum(COLUMN_TYPES),
});

const columnMappingsSchema = z
    .array(columnMappingSchema)
    .min(1, "At least one column mapping is required")
    .refine((columns) => hasUniqueValues(columns.map((column) => column.column)), {
        message: "Source columns must be unique",
    });

const addPropertyNameIssues = (
    table: {
        columns: { name: string }[];
        embeddedTables: { propertyName: string }[];
        linkedTables: { propertyName: string }[];
    },
    ctx: z.RefinementCtx,
) => {
    const takenNames = new Set<string>();

    const check = (name: string, path: (number | string)[]) => {
        const normalized = name.toLowerCase();

        if (!normalized) {
            return;
        }

        if (takenNames.has(normalized)) {
            ctx.addIssue({
                code: "custom",
                path,
                message: `Property name "${name}" is already used by another column, embedded table, or linked table.`,
            });

            return;
        }

        takenNames.add(normalized);
    };

    table.columns.forEach((column, index) => check(column.name, ["columns", index, "name"]));
    table.embeddedTables.forEach((embedded, index) =>
        check(embedded.propertyName, ["embeddedTables", index, "propertyName"]),
    );
    table.linkedTables.forEach((linked, index) => check(linked.propertyName, ["linkedTables", index, "propertyName"]));
};

const onDeleteSchema = z.object({
    patch: z.string().nullable(),
    ignoreDeletes: z.boolean().optional(),
});

const linkedTableSchema = z.object({
    sourceTableSchema: z.string().nullable(),
    sourceTableName: z.string().trim().min(1, "Source table name is required"),
    propertyName: z.string().trim().min(1, "Property name is required"),
    joinColumns: requiredUniqueStringsSchema("At least one join column is required", "Join columns must be unique"),
    linkedCollectionName: z.string().trim().min(1, "Linked collection name is required"),
});

const embeddedTableSchema = z
    .object({
        sourceTableSchema: z.string().nullable(),
        sourceTableName: z.string().trim().min(1, "Source table name is required"),
        propertyName: z.string().trim().min(1, "Property name is required"),
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
    })
    .superRefine(addPropertyNameIssues);

const tableSchema = z
    .object({
        collectionName: z.string().trim().min(1, "Collection name is required"),
        sourceTableSchema: z.string().nullable(),
        sourceTableName: z.string().trim().min(1, "Source table name is required"),
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
    })
    .superRefine(addPropertyNameIssues);

export const tablesSchema = z
    .array(tableSchema)
    .min(1, "At least one table is required")
    // A mapping with every root disabled would ingest nothing, and it would skip the CDC dry run
    // that gates the step, so it must not advance.
    .refine((tables) => tables.length === 0 || tables.some((table) => !table.disabled), {
        message: "At least one enabled table is required",
    })
    // Collection names must be unique; flagged per row so the message names the duplicate and the
    // blocked "Next" can focus the table holding it.
    .superRefine((tables, ctx) => {
        const countByName = new Map<string, number>();

        for (const table of tables) {
            const name = table.collectionName.trim();

            if (name) {
                countByName.set(name, (countByName.get(name) ?? 0) + 1);
            }
        }

        tables.forEach((table, index) => {
            const name = table.collectionName.trim();

            if (!name || (countByName.get(name) ?? 0) < 2) {
                return;
            }

            ctx.addIssue({
                code: "custom",
                path: [index, "collectionName"],
                message: `Collection name "${name}" is already used by another root table. Collection names must be unique.`,
            });
        });
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

export const connectionModeSchema = z.union([z.literal("fields"), z.literal("raw")]);

/** "default" leaves the choice to the driver: the connection string then carries no SSL keyword. */
export const sslModeSchema = z.enum(["default", "require", "disable"]);

const connectionFieldsShape = {
    host: z.string(),
    port: z.number().nullable(),
    database: z.string(),
    username: z.string(),
    password: z.string(),
    ssl: sslModeSchema,
};

const filledConnectionFieldsSchema = z.object({
    ...connectionFieldsShape,
    host: z.string().trim().min(1, "Host is required"),
    port: z
        .number()
        .int("Port must be a whole number")
        .min(1, "Port must be between 1 and 65535")
        .max(65535, "Port must be between 1 and 65535")
        .nullable()
        .refine((port) => port !== null, "Port is required"),
    database: z.string().trim().min(1, "Database name is required"),
    username: z.string().trim().min(1, "Username is required"),
});

/** `takenSlugs` are the slugs of the already existing apps; the new app must not reuse one. */
export const createExternalConnectionSchema = (takenSlugs: string[] = []) => {
    const normalizedTakenSlugs = new Set(takenSlugs.map((slug) => toSlug(slug)));

    return z
        .object({
            appName: z.string().trim().min(1, "Application name is required"),
            slug: slugSchema,
            // Empty until the operator picks a source database type; superRefine rejects it so the
            // connect step can't advance without a choice.
            provider: providerSchema.or(z.literal("")),
            mode: connectionModeSchema,
            fields: z.object(connectionFieldsShape),
            connectionString: z.string(),
        })
        .superRefine((values, ctx) => {
            if (values.provider === "") {
                ctx.addIssue({
                    code: "custom",
                    path: ["provider"],
                    message: "Select a source database type",
                });
            }

            const overrideSlug = values.slug.trim();
            // With an empty override the server derives the slug from the app name, so the name is
            // what has to be changed to free up the conflict.
            const slug = toSlug(overrideSlug || values.appName);

            if (slug !== "" && normalizedTakenSlugs.has(slug)) {
                ctx.addIssue({
                    code: "custom",
                    path: [overrideSlug === "" ? "appName" : "slug"],
                    message: `Slug "${slug}" is already used by another app`,
                });
            }

            if (values.mode === "raw") {
                if (values.connectionString.trim() === "") {
                    ctx.addIssue({
                        code: "custom",
                        path: ["connectionString"],
                        message: "Connection string is required",
                    });
                }

                return;
            }

            const fields = filledConnectionFieldsSchema.safeParse(values.fields);

            if (fields.success) {
                return;
            }

            for (const issue of fields.error.issues) {
                ctx.addIssue({ code: "custom", path: ["fields", ...issue.path], message: issue.message });
            }
        });
};

export const appSchema = z.object({
    dataSource: z.object({
        source: z.union([z.literal("external"), z.literal("ravendb")]),
    }),
    externalConnection: createExternalConnectionSchema(),
    verifySchema: z.object({
        tables: z
            .array(
                z.object({
                    sourceTableSchema: z.string().nullable().optional(),
                    sourceTableName: z.string(),
                }),
            )
            .min(1, "At least one table is required")
            .max(
                MAX_SELECTED_TABLES,
                `During the beta, at most ${MAX_SELECTED_TABLES} tables can be processed by one app`,
            ),
    }),
    map: z.object({
        source: z.union([z.literal("ai-suggested"), z.literal("manual")]),
        aiPrompt: z.string(),
    }),
    mapTables: z.object({
        tables: tablesSchema,
    }),
    preview: z.object({
        table: z.string(),
        maxRows: z.number().int().positive().max(1000, "Max rows must be less than or equal to 1000").optional(),
    }),
});

export type AppFormData = z.infer<typeof appSchema>;
export type AppStepId = keyof AppFormData;

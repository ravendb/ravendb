import { zodResolver } from "@hookform/resolvers/zod";
import { ArrowLeft, Database, Play } from "lucide-react";
import { Controller, useForm } from "react-hook-form";
import { Link, useNavigate } from "react-router";
import { toast } from "sonner";
import { z } from "zod";
import { api } from "@/api/api";
import type { CdcSinkConfiguration, CdcSinkSourceSchema, CdcSinkSourceTable, ConnectResult } from "@/api/setup-service";
import { FormInput } from "@/components/form/form-input";
import { Button } from "@/components/shadcn/ui/button";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { Textarea } from "@/components/shadcn/ui/textarea";

export function SetupConnect() {
    const navigate = useNavigate();
    const {
        control,
        formState: { isSubmitting },
        handleSubmit,
    } = useForm<SetupConnectFormValues>({
        defaultValues: {
            appName: "",
            provider: "Npgsql",
            connectionString: "",
            tableNames: "",
        },
        resolver: zodResolver(setupConnectSchema),
    });

    async function handleProvision(values: SetupConnectFormValues) {
        const tableNames = parseLines(values.tableNames);
        const connectRequest = {
            provider: values.provider,
            connectionString: values.connectionString,
            tableNames: tableNames.length ? tableNames : null,
        };

        try {
            const connectResult = await api.services.setup.connect(connectRequest);
            if (!isSuccess(connectResult)) {
                toast.error(firstMessage(connectResult.errors) ?? "Connection verification failed.");
                return;
            }

            const schema = await api.services.setup.discover(connectRequest);
            if (schema.errors.length) {
                toast.error(firstMessage(schema.errors) ?? "Schema discovery failed.");
                return;
            }

            const configuration = buildConfiguration(schema, tableNames);
            if (configuration.tables.length === 0) {
                toast.error("No CDC-ready tables were discovered.");
                return;
            }

            await api.services.setup.map(configuration);
            const provisionResult = await api.services.setup.provision({
                appName: values.appName,
            });

            toast.success("App provisioned.");
            navigate(`/apps/${provisionResult.slug}`);
        } catch (error) {
            toast.error(error instanceof Error ? error.message : "Provisioning failed.");
        }
    }

    return (
        <div className="flex min-h-full w-full items-start">
            <section className="w-full rounded-lg border bg-card p-6 text-card-foreground shadow-xs">
                <Button asChild variant="ghost" size="sm" className="mb-6 w-fit">
                    <Link to="/">
                        <ArrowLeft className="size-4" aria-hidden="true" />
                        Apps
                    </Link>
                </Button>

                <div className="flex max-w-3xl gap-4">
                    <div className="flex size-9 shrink-0 items-center justify-center rounded-md bg-accent text-accent-foreground">
                        <Database className="size-5" aria-hidden="true" />
                    </div>
                    <form className="grid flex-1 gap-5" onSubmit={handleSubmit(handleProvision)}>
                        <div>
                            <h2 className="text-base font-semibold tracking-normal">Connect source database</h2>
                        </div>

                        <div className="grid gap-4 md:grid-cols-2">
                            <FormInput control={control} name="appName" label="App name" />
                            <FormInput control={control} name="provider" label="Provider" />
                        </div>

                        <Controller
                            control={control}
                            name="connectionString"
                            render={({ field, fieldState }) => (
                                <Field data-invalid={fieldState.invalid}>
                                    <FieldLabel>Connection string</FieldLabel>
                                    <Textarea className="min-h-28 font-mono text-xs" {...field} />
                                    {fieldState.error?.message && (
                                        <FieldDescription className="text-destructive">
                                            {fieldState.error.message}
                                        </FieldDescription>
                                    )}
                                </Field>
                            )}
                        />

                        <Controller
                            control={control}
                            name="tableNames"
                            render={({ field }) => (
                                <Field>
                                    <FieldLabel>Tables</FieldLabel>
                                    <Textarea className="min-h-20 font-mono text-xs" {...field} />
                                </Field>
                            )}
                        />

                        <div className="flex justify-end">
                            <Button disabled={isSubmitting}>
                                <Play className="size-4" aria-hidden="true" />
                                {isSubmitting ? "Provisioning..." : "Provision app"}
                            </Button>
                        </div>
                    </form>
                </div>
            </section>
        </div>
    );
}

function buildConfiguration(schema: CdcSinkSourceSchema, tableNames: string[]): CdcSinkConfiguration {
    const requestedTables = new Set(tableNames.map((name) => name.toLowerCase()));
    const tables = schema.tables
        .filter((table) => isTableUsable(table))
        .filter((table) => requestedTables.size === 0 || requestedTables.has(table.sourceTableName.toLowerCase()))
        .map((table) => ({
            collectionName: toCollectionName(table.sourceTableName),
            sourceTableSchema: table.sourceTableSchema,
            sourceTableName: table.sourceTableName,
            primaryKeyColumns: table.primaryKeyColumns,
            columns: table.columns
                .filter((column) => column.isCdcCapturable)
                .map((column) => ({
                    column: column.name,
                    name: toPropertyName(column.name),
                    type: toColumnMappingType(column.suggestedType),
                })),
            embeddedTables: [],
            linkedTables: [],
        }))
        .filter((table) => table.primaryKeyColumns.length > 0 && table.columns.length > 0);

    return {
        tables,
    };
}

function isTableUsable(table: CdcSinkSourceTable) {
    return table.isCdcEnabled && !table.unsupportedReason;
}

function toColumnMappingType(type: CdcSinkSourceTable["columns"][number]["suggestedType"]) {
    if (type === "Json" || type === 1) {
        return 1 as const;
    }

    if (type === "Attachment" || type === 2) {
        return 2 as const;
    }

    return undefined;
}

function isSuccess(result: ConnectResult) {
    return result.success && result.errors.length === 0;
}

function firstMessage(messages: string[]) {
    return messages.find(Boolean);
}

function parseLines(value: string) {
    return value
        .split(/\r?\n|,/)
        .map((line) => line.trim())
        .filter(Boolean);
}

function toCollectionName(value: string) {
    const normalized = toPascalCase(value);
    return normalized ? `${normalized[0].toUpperCase()}${normalized.slice(1)}` : value;
}

function toPropertyName(value: string) {
    const normalized = toPascalCase(value);
    return normalized ? `${normalized[0].toLowerCase()}${normalized.slice(1)}` : value;
}

function toPascalCase(value: string) {
    return value
        .split(/[^a-zA-Z0-9]+/)
        .filter(Boolean)
        .map((part) => `${part[0]?.toUpperCase() ?? ""}${part.slice(1)}`)
        .join("");
}

const setupConnectSchema = z.object({
    appName: z.string().trim().min(1, "App name is required."),
    provider: z.string().trim().min(1, "Provider is required."),
    connectionString: z.string().trim().min(1, "Connection string is required."),
    tableNames: z.string(),
});

type SetupConnectFormValues = z.infer<typeof setupConnectSchema>;

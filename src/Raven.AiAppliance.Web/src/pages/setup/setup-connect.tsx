import { zodResolver } from "@hookform/resolvers/zod";
import { ArrowLeft, Database, Play, Search, TestTube2, WandSparkles } from "lucide-react";
import { useState, type ReactNode } from "react";
import { useForm, useWatch } from "react-hook-form";
import { Link, useNavigate } from "react-router";
import { toast } from "sonner";
import { z } from "zod";
import { api } from "@/api/api";
import type {
    CdcSinkConfiguration,
    CdcSinkSourceSchema,
    CdcSinkSourceTable,
    ConnectResult,
    TestMappingResult,
} from "@/api/setup-service";
import { FormInput } from "@/components/form/form-input";
import { FormStringList } from "@/components/form/form-string-list";
import { FormTextarea } from "@/components/form/form-textarea";
import { Button } from "@/components/shadcn/ui/button";

export function SetupConnect() {
    const navigate = useNavigate();
    const [connectResult, setConnectResult] = useState<ConnectResult | null>(null);
    const [schema, setSchema] = useState<CdcSinkSourceSchema | null>(null);
    const [mappedConfiguration, setMappedConfiguration] = useState<CdcSinkConfiguration | null>(null);
    const [testResult, setTestResult] = useState<TestMappingResult | null>(null);
    const [isWorking, setIsWorking] = useState(false);
    const { control, handleSubmit } = useForm<SetupConnectFormValues>({
        defaultValues: {
            appName: "",
            provider: "Npgsql",
            connectionString: "",
            tableNames: [],
            maxRows: 5,
        },
        resolver: zodResolver(setupConnectSchema),
    });

    async function handleConnect(values: SetupConnectFormValues) {
        await runBusy(async () => {
            const result = await api.services.setup.connect(toConnectRequest(values));
            setConnectResult(result);
            toast[result.success ? "success" : "error"](result.success ? "Connection verified." : "Connection failed.");
        });
    }

    async function handleDiscover(values: SetupConnectFormValues) {
        await runBusy(async () => {
            const request = toConnectRequest(values);
            const result = await api.services.setup.connect(request);
            setConnectResult(result);

            if (!isSuccess(result)) {
                toast.error(firstMessage(result.errors) ?? "Connection verification failed.");
                return;
            }

            const discoveredSchema = await api.services.setup.discover(request);
            setSchema(discoveredSchema);
            setMappedConfiguration(null);
            setTestResult(null);
            toast[discoveredSchema.errors.length ? "error" : "success"](
                discoveredSchema.errors.length ? "Schema discovery returned errors." : "Schema discovered.",
            );
        });
    }

    async function handleMap(values: SetupConnectFormValues) {
        await runBusy(async () => {
            const discoveredSchema = schema ?? (await api.services.setup.discover(toConnectRequest(values)));
            setSchema(discoveredSchema);

            const configuration = buildConfiguration(discoveredSchema, parseTableNames(values.tableNames));
            if (configuration.tables.length === 0) {
                toast.error("No CDC-ready tables were discovered.");
                return;
            }

            const mapped = await api.services.setup.map(configuration);
            setMappedConfiguration(mapped);
            toast.success("Mapping prepared.");
        });
    }

    async function handleTestMapping(values: SetupConnectFormValues) {
        await runBusy(async () => {
            const configuration =
                mappedConfiguration ??
                buildConfiguration(
                    schema ?? (await api.services.setup.discover(toConnectRequest(values))),
                    parseTableNames(values.tableNames),
                );
            const table = configuration.tables[0];

            if (!table) {
                toast.error("Map at least one table first.");
                return;
            }

            const result = await api.services.setup.testMapping({
                maxRows: values.maxRows,
                sourceTableName: table.sourceTableName,
                sourceTableSchema: table.sourceTableSchema,
            });
            setTestResult(result);
            toast[result.errors.length ? "error" : "success"](
                result.errors.length ? "Mapping test returned errors." : "Mapping test completed.",
            );
        });
    }

    async function handleProvision(values: SetupConnectFormValues) {
        await runBusy(async () => {
            const request = toConnectRequest(values);
            const connection = await api.services.setup.connect(request);
            setConnectResult(connection);

            if (!isSuccess(connection)) {
                toast.error(firstMessage(connection.errors) ?? "Connection verification failed.");
                return;
            }

            const discoveredSchema = await api.services.setup.discover(request);
            setSchema(discoveredSchema);

            if (discoveredSchema.errors.length) {
                toast.error(firstMessage(discoveredSchema.errors) ?? "Schema discovery failed.");
                return;
            }

            const configuration = buildConfiguration(discoveredSchema, parseTableNames(values.tableNames));
            if (configuration.tables.length === 0) {
                toast.error("No CDC-ready tables were discovered.");
                return;
            }

            const mapped = await api.services.setup.map(configuration);
            setMappedConfiguration(mapped);

            const provisionResult = await api.services.setup.provision({
                appName: values.appName,
            });

            toast.success("App provisioned.");
            navigate(`/apps/${provisionResult.slug}`);
        });
    }

    async function runBusy(action: () => Promise<void>) {
        setIsWorking(true);
        try {
            await action();
        } catch (error) {
            toast.error(error instanceof Error ? error.message : "Setup request failed.");
        } finally {
            setIsWorking(false);
        }
    }

    const watchedTableNames = useWatch({
        control,
        name: "tableNames",
    });
    const previewConfiguration = schema ? buildConfiguration(schema, parseTableNames(watchedTableNames)) : null;

    return (
        <div className="grid gap-4">
            <Button asChild variant="ghost" size="sm" className="w-fit">
                <Link to="/">
                    <ArrowLeft className="size-4" aria-hidden="true" />
                    Apps
                </Link>
            </Button>

            <section className="rounded-lg border bg-card p-6 text-card-foreground shadow-xs">
                <div className="flex max-w-5xl gap-4">
                    <div className="flex size-9 shrink-0 items-center justify-center rounded-md bg-accent text-accent-foreground">
                        <Database className="size-5" aria-hidden="true" />
                    </div>
                    <form className="grid flex-1 gap-5" onSubmit={handleSubmit(handleProvision)}>
                        <h2 className="text-base font-semibold tracking-normal">Connect source database</h2>

                        <div className="grid gap-4 md:grid-cols-3">
                            <FormInput control={control} name="appName" label="App name" />
                            <FormInput control={control} name="provider" label="Provider" />
                            <FormInput control={control} name="maxRows" label="Test rows" type="number" min={1} />
                        </div>

                        <FormTextarea
                            control={control}
                            name="connectionString"
                            label="Connection string"
                            className="font-mono text-xs"
                        />

                        <FormStringList
                            control={control}
                            name="tableNames"
                            label="Tables"
                            addButtonLabel="Add table"
                            defaultValue={{
                                name: "",
                            }}
                            description="Leave empty to use all supported CDC tables."
                            emptyLabel="All supported CDC tables will be used."
                            fieldName={(index) => `tableNames.${index}.name`}
                            itemLabel={(index) => (index === 0 ? "Table name" : undefined)}
                        />

                        <div className="flex flex-wrap justify-end gap-2">
                            <Button
                                type="button"
                                variant="outline"
                                disabled={isWorking}
                                onClick={handleSubmit(handleConnect)}
                            >
                                <TestTube2 className="size-4" aria-hidden="true" />
                                Test connection
                            </Button>
                            <Button
                                type="button"
                                variant="outline"
                                disabled={isWorking}
                                onClick={handleSubmit(handleDiscover)}
                            >
                                <Search className="size-4" aria-hidden="true" />
                                Discover
                            </Button>
                            <Button
                                type="button"
                                variant="outline"
                                disabled={isWorking}
                                onClick={handleSubmit(handleMap)}
                            >
                                <WandSparkles className="size-4" aria-hidden="true" />
                                Map
                            </Button>
                            <Button
                                type="button"
                                variant="outline"
                                disabled={isWorking}
                                onClick={handleSubmit(handleTestMapping)}
                            >
                                <Play className="size-4" aria-hidden="true" />
                                Test mapping
                            </Button>
                            <Button disabled={isWorking}>
                                <Play className="size-4" aria-hidden="true" />
                                {isWorking ? "Working..." : "Provision app"}
                            </Button>
                        </div>
                    </form>
                </div>
            </section>

            <SetupResults
                connectResult={connectResult}
                schema={schema}
                configuration={mappedConfiguration ?? previewConfiguration}
                testResult={testResult}
            />
        </div>
    );
}

function SetupResults({
    configuration,
    connectResult,
    schema,
    testResult,
}: {
    configuration: CdcSinkConfiguration | null;
    connectResult: ConnectResult | null;
    schema: CdcSinkSourceSchema | null;
    testResult: TestMappingResult | null;
}) {
    return (
        <div className="grid gap-4 xl:grid-cols-2">
            <ResultPanel title="Connection">
                {connectResult ? (
                    <MessageList
                        messages={[
                            connectResult.success ? "Connection OK." : "Connection failed.",
                            ...connectResult.errors,
                            ...connectResult.warnings,
                            connectResult.hasPermissionToSetup ? "Setup permission OK." : "Missing setup permission.",
                        ]}
                    />
                ) : (
                    <p className="text-sm text-muted-foreground">Not tested yet.</p>
                )}
            </ResultPanel>

            <ResultPanel title="Discovered tables">
                {schema ? (
                    <div className="space-y-3">
                        <MessageList messages={schema.errors} />
                        <div className="grid gap-2">
                            {schema.tables.map((table) => (
                                <div key={getTableKey(table)} className="rounded-md border bg-background p-3 text-sm">
                                    <div className="flex flex-wrap items-center justify-between gap-2">
                                        <span className="font-medium">{getTableLabel(table)}</span>
                                        <span className="text-xs text-muted-foreground">
                                            {table.columns.length} columns
                                        </span>
                                    </div>
                                    <p className="mt-1 text-xs text-muted-foreground">
                                        {isTableUsable(table)
                                            ? "CDC-ready"
                                            : table.unsupportedReason || "Not supported for CDC mapping"}
                                    </p>
                                </div>
                            ))}
                        </div>
                    </div>
                ) : (
                    <p className="text-sm text-muted-foreground">No schema discovered yet.</p>
                )}
            </ResultPanel>

            <ResultPanel title="Mapping">
                {configuration ? (
                    <div className="grid gap-2">
                        {configuration.tables.map((table) => (
                            <div key={getMappedTableKey(table)} className="rounded-md border bg-background p-3 text-sm">
                                <p className="font-medium">{table.collectionName}</p>
                                <p className="text-xs text-muted-foreground">
                                    {getMappedTableKey(table)} {"->"} {table.columns.length} fields
                                </p>
                            </div>
                        ))}
                    </div>
                ) : (
                    <p className="text-sm text-muted-foreground">No mapping prepared yet.</p>
                )}
            </ResultPanel>

            <ResultPanel title="Mapping test">
                {testResult ? (
                    <div className="space-y-3">
                        <MessageList messages={[...testResult.errors, ...testResult.warnings]} />
                        {testResult.results.map((result, index) => (
                            <pre
                                key={index}
                                className="max-h-64 overflow-auto rounded-md border bg-background p-3 text-xs"
                            >
                                {result.error || result.document || result.sourceRow || "Empty result"}
                            </pre>
                        ))}
                    </div>
                ) : (
                    <p className="text-sm text-muted-foreground">No test run yet.</p>
                )}
            </ResultPanel>
        </div>
    );
}

function ResultPanel({ children, title }: { children: ReactNode; title: string }) {
    return (
        <section className="rounded-lg border bg-card p-4 text-card-foreground shadow-xs">
            <h2 className="mb-3 text-sm font-semibold">{title}</h2>
            {children}
        </section>
    );
}

function MessageList({ messages }: { messages: string[] }) {
    const visibleMessages = messages.filter(Boolean);

    if (visibleMessages.length === 0) {
        return null;
    }

    return (
        <ul className="grid gap-1 text-sm text-muted-foreground">
            {visibleMessages.map((message, index) => (
                <li key={index}>{message}</li>
            ))}
        </ul>
    );
}

function buildConfiguration(schema: CdcSinkSourceSchema, tableNames: string[]): CdcSinkConfiguration {
    const requestedTables = new Set(tableNames.map((name) => name.toLowerCase()));
    const tables = schema.tables
        .filter((table) => isTableUsable(table))
        .filter(
            (table) =>
                requestedTables.size === 0 ||
                requestedTables.has(getTableKey(table).toLowerCase()) ||
                requestedTables.has(table.sourceTableName.toLowerCase()),
        )
        .map((table) => ({
            collectionName: toCollectionName(table.sourceTableName),
            columns: table.columns
                .filter((column) => column.isCdcCapturable)
                .map((column) => ({
                    column: column.name,
                    name: toPropertyName(column.name),
                    type: toColumnMappingType(column.suggestedType),
                })),
            embeddedTables: [],
            linkedTables: [],
            primaryKeyColumns: table.primaryKeyColumns,
            sourceTableName: table.sourceTableName,
            sourceTableSchema: table.sourceTableSchema,
        }))
        .filter((table) => table.primaryKeyColumns.length > 0 && table.columns.length > 0);

    return {
        tables,
    };
}

function toConnectRequest(values: SetupConnectFormValues) {
    const tableNames = parseTableNames(values.tableNames);

    return {
        connectionString: values.connectionString,
        provider: values.provider,
        tableNames: tableNames.length ? tableNames : null,
    };
}

function isTableUsable(table: CdcSinkSourceTable) {
    return table.isCdcEnabled && !table.unsupportedReason;
}

function getTableKey(table: CdcSinkSourceTable) {
    return table.sourceTableSchema ? `${table.sourceTableSchema}.${table.sourceTableName}` : table.sourceTableName;
}

function getMappedTableKey(table: CdcSinkConfiguration["tables"][number]) {
    return table.sourceTableSchema ? `${table.sourceTableSchema}.${table.sourceTableName}` : table.sourceTableName;
}

function getTableLabel(table: CdcSinkSourceTable) {
    return table.sourceTableSchema ? `${table.sourceTableSchema}.${table.sourceTableName}` : table.sourceTableName;
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

function parseTableNames(value: SetupConnectFormValues["tableNames"]) {
    return value.map((table) => table.name.trim()).filter(Boolean);
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
    connectionString: z.string().trim().min(1, "Connection string is required."),
    maxRows: z.number().min(1, "Use at least one row.").nullable(),
    provider: z.string().trim().min(1, "Provider is required."),
    tableNames: z.array(
        z.object({
            name: z.string(),
        }),
    ),
});

type SetupConnectFormValues = z.infer<typeof setupConnectSchema>;

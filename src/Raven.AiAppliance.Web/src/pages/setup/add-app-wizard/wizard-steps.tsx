import {
    Bot,
    CheckCircle2,
    Database,
    DatabaseZap,
    OctagonAlert,
    Loader2,
    Play,
    Search,
    SlidersHorizontal,
    TestTube2,
    Upload,
    WandSparkles,
} from "lucide-react";
import type { ReactNode } from "react";
import { useWatch, type Control } from "react-hook-form";
import type {
    CdcSinkConfiguration,
    DiscoverResponse,
    ProvisionResponse,
    TestMappingResponse,
} from "@/api/generated/server-api";
import { FormInput } from "@/components/form/form-input";
import { FormSelect } from "@/components/form/form-select";
import { FormStringList } from "@/components/form/form-string-list";
import { FormTextarea } from "@/components/form/form-textarea";
import { Alert, AlertDescription, AlertTitle } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { cn } from "@/lib/utils";
import {
    DATA_SOURCE_OPTIONS,
    DESCRIPTION_MAX_LENGTH,
    getMappedTableKey,
    getPrimaryKeyLabel,
    getTableLabel,
    isTableUsable,
    MAPPING_MODE_OPTIONS,
    PROVIDER_OPTIONS,
    type SetupWizardMessage,
    type SetupWizardFormValues,
} from "@/pages/setup/add-app-wizard/wizard-model";

type StepProps = {
    control: Control<SetupWizardFormValues>;
    isWorking: boolean;
    message?: SetupWizardMessage;
};

type ConnectionStepProps = StepProps & {
    onVerifyConnection: () => void;
};

type VerifySchemaStepProps = StepProps & {
    onDiscoverSchema: () => void;
    onVerifyConnection: () => void;
    schema: DiscoverResponse | null;
};

type MapSchemaStepProps = StepProps & {
    mappedConfiguration: CdcSinkConfiguration | null;
    onPrepareMapping: () => void;
    schema: DiscoverResponse | null;
};

type PreviewStepProps = StepProps & {
    mappedConfiguration: CdcSinkConfiguration | null;
    onRunPreview: () => void;
    schema: DiscoverResponse | null;
    testResult: TestMappingResponse | null;
};

type LoadProgressStepProps = {
    mappedConfiguration: CdcSinkConfiguration | null;
    message?: SetupWizardMessage;
    provisionResult: ProvisionResponse | null;
};

export function ChooseDataSourceStep({ message }: { message?: SetupWizardMessage }) {
    return (
        <StepSection
            title="Choose data source"
            description="Where is the data this application will work with?"
            message={message}
        >
            <div className="grid gap-3 md:grid-cols-2">
                {DATA_SOURCE_OPTIONS.map((option) => {
                    const isSelected = option.id === "external";
                    const Icon = option.id === "external" ? Database : DatabaseZap;

                    return (
                        <button
                            key={option.id}
                            type="button"
                            disabled={option.disabled}
                            aria-pressed={isSelected}
                            className={cn(
                                "min-h-28 rounded-lg border bg-background p-4 text-left transition-colors",
                                "hover:bg-accent hover:text-accent-foreground",
                                isSelected && "border-foreground bg-accent text-accent-foreground",
                                option.disabled && "cursor-not-allowed opacity-55 hover:bg-background",
                            )}
                        >
                            <Icon className="mb-5 size-5" aria-hidden="true" />
                            <span className="block text-sm font-semibold">{option.label}</span>
                            <span className="mt-2 block text-xs leading-5 text-muted-foreground">
                                {option.description}
                            </span>
                        </button>
                    );
                })}
            </div>
        </StepSection>
    );
}

export function BasicConfigurationStep({ control, message }: StepProps) {
    const description = useWatch({
        control,
        name: "description",
    });

    return (
        <StepSection title="Configure basic settings" message={message}>
            <div className="grid gap-5">
                <FormInput control={control} name="appName" label="Application name" placeholder="e.g. AcmeShop" />
                <div className="grid gap-1">
                    <FormTextarea
                        control={control}
                        name="description"
                        label="Description (optional)"
                        placeholder="What does this application do? Helps you tell it apart from other apps."
                        maxLength={DESCRIPTION_MAX_LENGTH}
                    />
                    <p className="justify-self-end text-xs text-muted-foreground">
                        {(description ?? "").length}/{DESCRIPTION_MAX_LENGTH}
                    </p>
                </div>
            </div>
        </StepSection>
    );
}

export function ConnectSourceStep({ control, isWorking, message, onVerifyConnection }: ConnectionStepProps) {
    return (
        <StepSection
            title="Connect to your source database"
            description="Enter the external database connection details."
            message={message}
        >
            <div className="grid gap-5">
                <FormSelect
                    control={control}
                    name="provider"
                    label="Database type"
                    options={PROVIDER_OPTIONS}
                    disabled={isWorking}
                />
                <FormTextarea
                    control={control}
                    name="connectionString"
                    label="Connection string"
                    placeholder="Host=localhost;Database=shop;Username=..."
                    textareaClassName="font-mono text-xs"
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
                <div className="flex flex-wrap items-center gap-3">
                    <Button type="button" variant="secondary" onClick={onVerifyConnection} disabled={isWorking}>
                        <TestTube2 className="size-4" aria-hidden="true" />
                        Verify connection
                    </Button>
                </div>
            </div>
        </StepSection>
    );
}

export function VerifySchemaStep({
    control,
    isWorking,
    message,
    onDiscoverSchema,
    onVerifyConnection,
    schema,
}: VerifySchemaStepProps) {
    return (
        <StepSection
            title="Verify your schema"
            description="Fetch existing tables from the linked source."
            message={message}
        >
            <div className="grid gap-4">
                <div className="flex flex-wrap justify-end gap-2">
                    <Button type="button" variant="secondary" onClick={onDiscoverSchema} disabled={isWorking}>
                        <Search className="size-4" aria-hidden="true" />
                        Discover tables
                    </Button>
                    <Button type="button" variant="secondary" onClick={onVerifyConnection} disabled={isWorking}>
                        <TestTube2 className="size-4" aria-hidden="true" />
                        Verify source
                    </Button>
                </div>
                <SchemaTable schema={schema} />
                <FormInput control={control} name="maxRows" label="Preview rows" type="number" min={1} />
            </div>
        </StepSection>
    );
}

export function MapSchemaStep({
    mappedConfiguration,
    message,
    onPrepareMapping,
    schema,
    isWorking,
}: MapSchemaStepProps) {
    return (
        <StepSection
            title="Map your schema"
            description="Choose how source tables become RavenDB documents."
            message={message}
        >
            <div className="grid gap-5">
                <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                    {MAPPING_MODE_OPTIONS.map((option) => {
                        const Icon = getMappingModeIcon(option.id);
                        const isSelected = option.id === "auto";

                        return (
                            <button
                                key={option.id}
                                type="button"
                                disabled={option.disabled}
                                aria-pressed={isSelected}
                                className={cn(
                                    "min-h-32 rounded-lg border bg-background p-4 text-left transition-colors",
                                    "hover:bg-accent hover:text-accent-foreground",
                                    isSelected && "border-foreground bg-accent text-accent-foreground",
                                    option.disabled && "cursor-not-allowed opacity-55 hover:bg-background",
                                )}
                            >
                                <Icon className="mb-5 size-5" aria-hidden="true" />
                                <span className="block text-sm font-semibold">{option.label}</span>
                                <span className="mt-2 block text-xs leading-5 text-muted-foreground">
                                    {option.description}
                                </span>
                            </button>
                        );
                    })}
                </div>

                <div className="flex justify-end">
                    <Button
                        type="button"
                        variant="secondary"
                        onClick={onPrepareMapping}
                        disabled={isWorking || !schema}
                    >
                        <WandSparkles className="size-4" aria-hidden="true" />
                        Generate auto mapping
                    </Button>
                </div>

                <MappingTable configuration={mappedConfiguration} />
            </div>
        </StepSection>
    );
}

export function PreviewStep({
    control,
    isWorking,
    mappedConfiguration,
    message,
    onRunPreview,
    schema,
    testResult,
}: PreviewStepProps) {
    const appName = useWatch({
        control,
        name: "appName",
    });
    const description = useWatch({
        control,
        name: "description",
    });
    const provider = useWatch({
        control,
        name: "provider",
    });
    const providerLabel = PROVIDER_OPTIONS.find((option) => option.value === provider)?.label ?? provider;

    return (
        <StepSection title="Preview" description="Review the source, schema, and generated mapping." message={message}>
            <div className="grid gap-5">
                <div className="grid gap-3 md:grid-cols-3">
                    <SummaryPanel label="Application" value={appName || "Untitled"} />
                    <SummaryPanel label="Source" value={providerLabel} />
                    <SummaryPanel label="Mapped tables" value={String(mappedConfiguration?.tables?.length ?? 0)} />
                </div>

                {description && (
                    <div className="rounded-lg border bg-background p-4">
                        <p className="text-xs font-medium text-muted-foreground">Description</p>
                        <p className="mt-2 text-sm">{description}</p>
                    </div>
                )}

                <SchemaTable schema={schema} compact />
                <MappingTable configuration={mappedConfiguration} />

                <div className="flex justify-end">
                    <Button
                        type="button"
                        variant="secondary"
                        onClick={onRunPreview}
                        disabled={isWorking || !mappedConfiguration}
                    >
                        <Play className="size-4" aria-hidden="true" />
                        Run preview
                    </Button>
                </div>

                <MappingPreviewResult result={testResult} />
            </div>
        </StepSection>
    );
}

export function LoadProgressStep({ mappedConfiguration, message, provisionResult }: LoadProgressStepProps) {
    const tables = mappedConfiguration?.tables ?? [];

    return (
        <StepSection
            title="Load in progress"
            description="The application was created and the initial CDC load has started."
            message={message}
        >
            <div className="grid gap-5">
                <div className="rounded-lg border bg-background p-4">
                    <div className="flex items-center gap-3">
                        <div className="flex size-9 items-center justify-center rounded-full bg-muted">
                            <Loader2 className="size-5 animate-spin" aria-hidden="true" />
                        </div>
                        <div>
                            <p className="text-sm font-semibold">{provisionResult?.slug ?? "Preparing app"}</p>
                            <p className="text-xs text-muted-foreground">
                                Estimated progress until backend status exists.
                            </p>
                        </div>
                    </div>
                    <div className="mt-4 h-2 rounded-full bg-muted">
                        <div className="h-2 w-1/3 rounded-full bg-foreground" />
                    </div>
                </div>

                <div className="overflow-hidden rounded-lg border">
                    <Table>
                        <TableHeader>
                            <TableRow>
                                <TableHead>Collection</TableHead>
                                <TableHead>Source table</TableHead>
                                <TableHead>Status</TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {tables.map((table, index) => (
                                <TableRow key={getMappedTableKey(table)}>
                                    <TableCell>{table.collectionName}</TableCell>
                                    <TableCell className="text-muted-foreground">{getMappedTableKey(table)}</TableCell>
                                    <TableCell>{index === 0 ? "Loading" : "Queued"}</TableCell>
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                </div>
            </div>
        </StepSection>
    );
}

function StepSection({
    children,
    description,
    message,
    title,
}: {
    children: ReactNode;
    description?: string;
    message?: SetupWizardMessage;
    title: string;
}) {
    return (
        <section className="grid gap-5">
            <div>
                <h2 className="text-2xl font-semibold tracking-normal">{title}</h2>
                {description && <p className="mt-3 text-sm text-muted-foreground">{description}</p>}
            </div>
            {children}
            <StepMessageAlert message={message} />
        </section>
    );
}

function StepMessageAlert({ message }: { message?: SetupWizardMessage }) {
    if (!message) {
        return null;
    }

    const Icon = message.type === "success" ? CheckCircle2 : OctagonAlert;

    return (
        <Alert
            variant={message.type === "error" ? "destructive" : "default"}
            className={cn(
                message.type === "success" &&
                    "border-emerald-700/30 bg-emerald-950/20 text-foreground dark:bg-emerald-950/60",
            )}
        >
            <Icon className="size-4" aria-hidden="true" />
            <AlertTitle>{message.title}</AlertTitle>
            {message.description && <AlertDescription>{message.description}</AlertDescription>}
        </Alert>
    );
}

function SchemaTable({ compact = false, schema }: { compact?: boolean; schema: DiscoverResponse | null }) {
    if (!schema) {
        return (
            <div className="rounded-lg border bg-background px-3 py-8 text-center text-sm text-muted-foreground">
                No tables discovered yet.
            </div>
        );
    }

    const tables = compact ? schema.tables.slice(0, 5) : schema.tables;

    return (
        <div className="grid gap-3">
            <MessageList messages={schema.errors} tone="destructive" />
            <div className="overflow-hidden rounded-lg border">
                <Table>
                    <TableHeader>
                        <TableRow>
                            <TableHead>Table name</TableHead>
                            <TableHead>Primary key</TableHead>
                            <TableHead>Columns count</TableHead>
                            {!compact && <TableHead>Status</TableHead>}
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        {tables.map((table) => (
                            <TableRow key={getTableLabel(table)}>
                                <TableCell>{getTableLabel(table)}</TableCell>
                                <TableCell className="text-muted-foreground">{getPrimaryKeyLabel(table)}</TableCell>
                                <TableCell className="text-muted-foreground">{table.columns.length}</TableCell>
                                {!compact && (
                                    <TableCell className="text-muted-foreground">
                                        {isTableUsable(table) ? "Ready" : table.unsupportedReason || "Unsupported"}
                                    </TableCell>
                                )}
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            </div>
        </div>
    );
}

function MappingTable({ configuration }: { configuration: CdcSinkConfiguration | null }) {
    if (!configuration) {
        return (
            <div className="rounded-lg border bg-background px-3 py-8 text-center text-sm text-muted-foreground">
                No mapping generated yet.
            </div>
        );
    }

    return (
        <div className="overflow-hidden rounded-lg border">
            <Table>
                <TableHeader>
                    <TableRow>
                        <TableHead>Collection</TableHead>
                        <TableHead>Source table</TableHead>
                        <TableHead>Fields</TableHead>
                    </TableRow>
                </TableHeader>
                <TableBody>
                    {(configuration.tables ?? []).map((table) => (
                        <TableRow key={getMappedTableKey(table)}>
                            <TableCell>{table.collectionName ?? ""}</TableCell>
                            <TableCell className="text-muted-foreground">{getMappedTableKey(table)}</TableCell>
                            <TableCell className="text-muted-foreground">{table.columns?.length ?? 0}</TableCell>
                        </TableRow>
                    ))}
                </TableBody>
            </Table>
        </div>
    );
}

function MappingPreviewResult({ result }: { result: TestMappingResponse | null }) {
    if (!result) {
        return (
            <div className="rounded-lg border bg-background px-3 py-8 text-center text-sm text-muted-foreground">
                Preview has not been run yet.
            </div>
        );
    }

    return (
        <div className="grid gap-3">
            <MessageList messages={[...result.errors, ...result.warnings]} tone="destructive" />
            {result.results.length === 0 ? (
                <div className="rounded-lg border bg-background px-3 py-8 text-center text-sm text-muted-foreground">
                    No preview rows returned.
                </div>
            ) : (
                result.results.map((row, index) => (
                    <pre key={index} className="max-h-64 overflow-auto rounded-lg border bg-background p-3 text-xs">
                        {row.error || row.document || row.sourceRow || "Empty result"}
                    </pre>
                ))
            )}
        </div>
    );
}

function SummaryPanel({ label, value }: { label: string; value: string }) {
    return (
        <div className="rounded-lg border bg-background p-4">
            <p className="text-xs font-medium text-muted-foreground">{label}</p>
            <p className="mt-2 truncate text-sm font-semibold">{value}</p>
        </div>
    );
}

function MessageList({ messages, tone = "muted" }: { messages: string[]; tone?: "destructive" | "muted" }) {
    const visibleMessages = messages.filter(Boolean);

    if (visibleMessages.length === 0) {
        return null;
    }

    return (
        <ul className={cn("grid gap-1 text-sm", tone === "destructive" ? "text-destructive" : "text-muted-foreground")}>
            {visibleMessages.map((message, index) => (
                <li key={index}>{message}</li>
            ))}
        </ul>
    );
}

function getMappingModeIcon(optionId: (typeof MAPPING_MODE_OPTIONS)[number]["id"]) {
    switch (optionId) {
        case "auto":
            return WandSparkles;
        case "ai-suggest":
            return Bot;
        case "manual":
            return SlidersHorizontal;
        case "import":
            return Upload;
    }
}

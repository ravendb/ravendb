import { useMemo, useState } from "react";
import type { RowSelectionState } from "@tanstack/react-table";
import { useFormContext, useFormState } from "react-hook-form";
import { CheckIcon, PlusIcon, SearchIcon, TriangleAlertIcon } from "lucide-react";
import type { DiscoverResponse, DiscoverTableResponse } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { InputGroup, InputGroupAddon, InputGroupInput } from "@/components/shadcn/ui/input-group";
import { Tabs, TabsList, TabsTrigger } from "@/components/shadcn/ui/tabs";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { getTableKey, isTableSupported } from "@/pages/setup/add-app-wizard/discover-utils";
import { ImportedConfigAlert } from "@/pages/setup/add-app-wizard/imported-config-alert";
import { DefineSchemasSheet } from "@/pages/setup/add-app-wizard/steps/verify/define-schemas-sheet";
import { NeedsConfigTablesTable } from "@/pages/setup/add-app-wizard/steps/verify/needs-config-tables-table";
import { useDiscoverTablesMutation } from "@/pages/setup/add-app-wizard/steps/verify/use-discover-tables";
import { VerifiedTablesTable } from "@/pages/setup/add-app-wizard/steps/verify/verified-tables-table";
import { WizardErrorList } from "@/components/form/wizard/wizard-error-list";
import {
    DiscoverLoadingSkeleton,
    MessageList,
    NoTablesFound,
} from "@/pages/setup/add-app-wizard/steps/verify/verify-schema-states";

type VerifyTab = "verified" | "needs-configuration";

export function VerifySchemaStep() {
    const { control, setValue, getValues } = useFormContext<AppFormData>();
    // formState off the context does not re-render this step; only its own subscription does.
    const { errors } = useFormState({ control, name: "verifySchema.tables" });
    const tablesError = errors.verifySchema?.tables;
    const discoverResult = useSetupWizardStore((state) => state.discoverResult);
    const discoverSchemas = useSetupWizardStore((state) => state.discoverSchemas);
    const isLocked = useSetupWizardStore((state) => state.importState) === "locked";
    const discoverMutation = useDiscoverTablesMutation();

    const [activeTab, setActiveTab] = useState<VerifyTab>("verified");
    const [search, setSearch] = useState("");
    const [isSchemasSheetOpen, setIsSchemasSheetOpen] = useState(false);
    const [rowSelection, setRowSelection] = useState<RowSelectionState>(() =>
        Object.fromEntries(getValues("verifySchema.tables").map((table) => [getTableKey(table), true])),
    );

    // These lists are passed to the table components as react-table `data`; memoizing keeps a
    // stable reference between renders so react-table does not recompute its row models endlessly.
    const allTables = useMemo(() => discoverResult?.tables ?? [], [discoverResult]);
    const verifiedTables = useMemo(
        () => allTables.filter((table) => isTableSupported(discoverResult, table)),
        [allTables, discoverResult],
    );
    // When discovery failed, neither list shows tables - only the errors above.
    const needsConfigTables = useMemo(
        () => allTables.filter((table) => discoverResult?.success && !isTableSupported(discoverResult, table)),
        [allTables, discoverResult],
    );

    const syncSelectionToForm = (selection: RowSelectionState, tables: DiscoverTableResponse[]) => {
        setValue(
            "verifySchema.tables",
            tables
                .filter((table) => selection[getTableKey(table)])
                .map((table) => ({
                    sourceTableName: table.sourceTableName,
                    sourceTableSchema: table.sourceTableSchema,
                })),
            { shouldValidate: true },
        );
    };

    const handleRowSelectionChange = (selection: RowSelectionState) => {
        setRowSelection(selection);
        syncSelectionToForm(selection, verifiedTables);
    };

    const handleSchemasSave = async (schemas: string[]) => {
        let result: DiscoverResponse;
        try {
            result = await discoverMutation.mutateAsync(schemas);
        } catch {
            // The mutation already surfaced the error as a toast; keep the sheet open.
            return;
        }

        // Keep only selections that still exist in the re-discovered schema.
        const nextVerifiedTables = result.tables.filter((table) => isTableSupported(result, table));
        const nextSelection: RowSelectionState = Object.fromEntries(
            nextVerifiedTables
                .filter((table) => rowSelection[getTableKey(table)])
                .map((table) => [getTableKey(table), true]),
        );
        setRowSelection(nextSelection);
        syncSelectionToForm(nextSelection, nextVerifiedTables);
        setIsSchemasSheetOpen(false);
    };

    // The tabs are hidden when every table is verified, so fall back to the verified tab
    // even if "needs-configuration" was active before (e.g. after a re-discovery).
    const currentTab: VerifyTab = needsConfigTables.length > 0 ? activeTab : "verified";

    return (
        <div className="flex min-h-0 flex-1 flex-col gap-4">
            <ImportedConfigAlert />
            <WizardErrorList errors={discoverResult?.errors} />
            <MessageList messages={discoverResult?.warnings} tone="warning" />

            {discoverMutation.isPending ? (
                <DiscoverLoadingSkeleton />
            ) : allTables.length > 0 ? (
                <>
                    <div className="flex items-center gap-2">
                        <InputGroup className="max-w-sm">
                            <InputGroupAddon>
                                <SearchIcon aria-hidden="true" />
                            </InputGroupAddon>
                            <InputGroupInput
                                value={search}
                                onChange={(event) => setSearch(event.target.value)}
                                placeholder="Search by table name..."
                                type="search"
                            />
                        </InputGroup>
                        <Button
                            type="button"
                            variant="outline"
                            className="ml-auto"
                            onClick={() => setIsSchemasSheetOpen(true)}
                            disabled={isLocked}
                        >
                            <PlusIcon aria-hidden="true" />
                            Customize schemas
                        </Button>
                    </div>

                    {needsConfigTables.length > 0 && (
                        <Tabs value={currentTab} onValueChange={(value) => setActiveTab(value as VerifyTab)}>
                            <TabsList>
                                <TabsTrigger value="verified">
                                    <CheckIcon className="size-3.5" aria-hidden="true" />
                                    Verified
                                    <Badge variant="secondary" className="rounded-full font-mono tabular-nums">
                                        {verifiedTables.length}
                                    </Badge>
                                </TabsTrigger>
                                <TabsTrigger value="needs-configuration">
                                    <TriangleAlertIcon className="size-3.5" aria-hidden="true" />
                                    Needs configuration
                                    <Badge variant="secondary" className="rounded-full font-mono tabular-nums">
                                        {needsConfigTables.length}
                                    </Badge>
                                </TabsTrigger>
                            </TabsList>
                        </Tabs>
                    )}

                    {currentTab === "needs-configuration" && (
                        <div className="flex items-center gap-2 rounded-md border border-amber-300 bg-amber-50 px-3 py-1.5 text-sm text-amber-800 dark:border-amber-900/50 dark:bg-amber-950/40 dark:text-amber-200">
                            <TriangleAlertIcon className="size-4 shrink-0" aria-hidden="true" />
                            {needsConfigTables.length === 1
                                ? "1 discovered table needs configuration before it can be selected."
                                : `${needsConfigTables.length} discovered tables need configuration before they can be selected.`}
                        </div>
                    )}

                    {currentTab === "verified" ? (
                        <VerifiedTablesTable
                            tables={verifiedTables}
                            totalTableCount={allTables.length}
                            search={search}
                            rowSelection={rowSelection}
                            onRowSelectionChange={handleRowSelectionChange}
                            disabled={isLocked}
                        />
                    ) : (
                        <NeedsConfigTablesTable tables={needsConfigTables} search={search} />
                    )}
                </>
            ) : discoverResult?.success ? (
                <NoTablesFound schemas={discoverSchemas} onCustomizeSchemas={() => setIsSchemasSheetOpen(true)} />
            ) : null}

            {tablesError && <Alert variant="destructive">{tablesError.message}</Alert>}

            <DefineSchemasSheet
                isOpen={isSchemasSheetOpen}
                onOpenChange={setIsSchemasSheetOpen}
                initialSchemas={discoverSchemas}
                isDiscovering={discoverMutation.isPending}
                onSave={handleSchemasSave}
            />
        </div>
    );
}

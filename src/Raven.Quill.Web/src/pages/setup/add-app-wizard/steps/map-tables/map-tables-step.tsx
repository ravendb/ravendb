import { useFormContext, useFormState } from "react-hook-form";
import { RefreshCwIcon } from "lucide-react";
import { toast } from "sonner";
import AceEditor from "@/components/ace-editor/ace-editor";
import { WizardErrorAlert } from "@/components/form/wizard/wizard-error-alert";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Field, FieldLabel } from "@/components/shadcn/ui/field";
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from "@/components/shadcn/ui/resizable";
import { Switch } from "@/components/shadcn/ui/switch";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { MapTablesSuggestionProgress } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-suggestion-progress";
import {
    parseRawTablesToForm,
    serializeFormTablesToRaw,
    tryParseRawTablesToForm,
} from "@/pages/setup/add-app-wizard/steps/map-tables/raw-tables";
import { TableEditor } from "@/pages/setup/add-app-wizard/steps/map-tables/table-editor";
import { TablesExplorer } from "@/pages/setup/add-app-wizard/steps/map-tables/tables-explorer";
import { UnmappedTablesAlert } from "@/pages/setup/add-app-wizard/steps/map-tables/unmapped-tables-alert";
import { useApplyMapTables } from "@/pages/setup/add-app-wizard/steps/map-tables/use-apply-map-tables";
import { useSuggestedMapTables } from "@/pages/setup/add-app-wizard/steps/map-tables/use-suggested-map-tables";
import { useVerifyMapTablesState } from "@/pages/setup/add-app-wizard/steps/map-tables/use-verify-map-tables";
import { VerifyCdcButton } from "@/pages/setup/add-app-wizard/steps/verify/verify-cdc-button";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";

const VERIFY_TABLES_LABELS = {
    idle: "Verify tables",
    verifying: "Verifying tables...",
    verified: "Tables verified",
};

export function MapTablesStep({ isBusy }: WizardBodyComponentProps) {
    const { control, getValues, setValue } = useFormContext<AppFormData>();
    const { errors } = useFormState({ control, name: "mapTables.tables" });
    const applyMapTables = useApplyMapTables();
    const verifyTables = useVerifyMapTablesState();
    const {
        isSuggesting,
        startedAt: suggestionStartedAt,
        error: suggestionError,
        retry: retrySuggestion,
    } = useSuggestedMapTables();

    const isRawView = useSetupWizardStore((state) => state.isMapTablesRawView);
    const rawContent = useSetupWizardStore((state) => state.mapTablesRawContent);
    const isRawContentValid = useSetupWizardStore((state) => state.isMapTablesRawContentValid);
    const openRawView = useSetupWizardStore((state) => state.openMapTablesRawView);
    const closeRawView = useSetupWizardStore((state) => state.closeMapTablesRawView);
    const setRawContent = useSetupWizardStore((state) => state.setMapTablesRawContent);

    const tablesError = errors.mapTables?.tables;
    const tablesErrorMessage = tablesError?.message ?? tablesError?.root?.message;

    // Invalid raw JSON means the form still holds the previous parse, so what a verify would run
    // against is not what the editor shows.
    const isFormBehindEditor = isRawView && !isRawContentValid;

    const handleToggleRawView = (checked: boolean) => {
        if (checked) {
            openRawView(serializeFormTablesToRaw(getValues("mapTables.tables")));
            return;
        }

        try {
            applyMapTables(parseRawTablesToForm(rawContent));
            closeRawView();
        } catch (error) {
            toast.error(error instanceof Error ? error.message : "The raw configuration could not be applied.");
        }
    };

    // Keep the form in sync with valid edits so the wizard's validation, "Next", and preview all
    // work off form state. Invalid intermediate JSON stays in the editor until it parses again.
    const handleRawChange = (value: string) => {
        const tables = tryParseRawTablesToForm(value);

        setRawContent(value, tables !== null);

        if (tables) {
            setValue("mapTables.tables", tables);
        }
    };

    if (isSuggesting) {
        return <MapTablesSuggestionProgress startedAt={suggestionStartedAt} />;
    }

    if (suggestionError) {
        return (
            <div className="grid gap-3">
                <WizardErrorAlert error={suggestionError} />
                <Button type="button" variant="outline" className="justify-self-start" onClick={retrySuggestion}>
                    <RefreshCwIcon aria-hidden="true" />
                    Try again
                </Button>
            </div>
        );
    }

    return (
        <div className="flex min-h-0 flex-1 flex-col gap-3">
            <div className="flex shrink-0 items-center justify-end gap-4">
                {/* Runs the dry run Next runs, so the operator can check that the (AI-)suggested tables
                    exist and are capturable before leaving the step. Without an enabled table there is
                    nothing to verify, and validation blocks Next anyway, so the button stays out instead
                    of showing up dead. */}
                {verifyTables.selectedTables.length > 0 && (
                    <>
                        {/* While the form is behind the editor, verifying would answer for a mapping the
                            operator is no longer looking at - and an earlier pass must not read as
                            "verified" against the editor's content either. */}
                        <VerifyCdcButton
                            disabled={isBusy || isFormBehindEditor}
                            state={isFormBehindEditor ? { ...verifyTables, isVerified: false } : verifyTables}
                            labels={VERIFY_TABLES_LABELS}
                        />
                        <div className="h-4 w-px bg-border" />
                    </>
                )}
                <Field orientation="horizontal">
                    <Switch id="map-tables-raw-view" checked={isRawView} onCheckedChange={handleToggleRawView} />
                    <FieldLabel htmlFor="map-tables-raw-view">Raw JSON</FieldLabel>
                </Field>
            </div>

            {isRawView ? (
                <AceEditor
                    mode="json"
                    value={rawContent}
                    onChange={handleRawChange}
                    isFillHeight
                    className="min-h-80"
                    actions={[{ component: <AceEditor.FormatAction /> }, { component: <AceEditor.FullScreenAction /> }]}
                />
            ) : (
                <>
                    <UnmappedTablesAlert />
                    <ResizablePanelGroup
                        orientation="horizontal"
                        className="min-h-80 flex-1 rounded-lg border bg-background"
                    >
                        <ResizablePanel defaultSize="30%" minSize="180px" maxSize="50%" className="min-w-0">
                            <TablesExplorer />
                        </ResizablePanel>
                        <ResizableHandle />
                        <ResizablePanel className="min-w-0">
                            <TableEditor />
                        </ResizablePanel>
                    </ResizablePanelGroup>
                    {tablesErrorMessage && <Alert variant="destructive">{tablesErrorMessage}</Alert>}
                </>
            )}

            {verifyTables.error && <WizardErrorAlert error={verifyTables.error} className="shrink-0" />}
        </div>
    );
}

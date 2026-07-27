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

export function MapTablesStep() {
    const { control, getValues, setValue } = useFormContext<AppFormData>();
    const { errors } = useFormState({ control, name: "mapTables.tables" });
    const applyMapTables = useApplyMapTables();
    const { isSuggesting, error: suggestionError, retry: retrySuggestion } = useSuggestedMapTables();

    const isRawView = useSetupWizardStore((state) => state.isMapTablesRawView);
    const rawContent = useSetupWizardStore((state) => state.mapTablesRawContent);
    const openRawView = useSetupWizardStore((state) => state.openMapTablesRawView);
    const closeRawView = useSetupWizardStore((state) => state.closeMapTablesRawView);
    const setRawContent = useSetupWizardStore((state) => state.setMapTablesRawContent);

    const tablesError = errors.mapTables?.tables;
    const tablesErrorMessage = tablesError?.message ?? tablesError?.root?.message;

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
        setRawContent(value);

        const tables = tryParseRawTablesToForm(value);

        if (tables) {
            setValue("mapTables.tables", tables);
        }
    };

    if (isSuggesting) {
        return <MapTablesSuggestionProgress />;
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
            <Field orientation="horizontal" className="justify-self-end">
                <Switch id="map-tables-raw-view" checked={isRawView} onCheckedChange={handleToggleRawView} />
                <FieldLabel htmlFor="map-tables-raw-view">Raw JSON</FieldLabel>
            </Field>

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
        </div>
    );
}

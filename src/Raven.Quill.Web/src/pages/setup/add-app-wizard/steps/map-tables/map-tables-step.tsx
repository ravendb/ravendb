import { useFormContext, useFormState } from "react-hook-form";
import { toast } from "sonner";
import AceEditor from "@/components/ace-editor/ace-editor";
import { Alert } from "@/components/shadcn/ui/alert";
import { Field, FieldLabel } from "@/components/shadcn/ui/field";
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from "@/components/shadcn/ui/resizable";
import { Switch } from "@/components/shadcn/ui/switch";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import {
    parseRawTablesToForm,
    serializeFormTablesToRaw,
    tryParseRawTablesToForm,
} from "@/pages/setup/add-app-wizard/steps/map-tables/raw-tables";
import { TableEditor } from "@/pages/setup/add-app-wizard/steps/map-tables/table-editor";
import { TablesExplorer } from "@/pages/setup/add-app-wizard/steps/map-tables/tables-explorer";

// Matches the h-[34rem] explorer/editor panel so the raw editor fills the same area.
const RAW_EDITOR_HEIGHT_PX = 544;

export function MapTablesStep() {
    const { control, getValues, setValue } = useFormContext<AppFormData>();
    const { errors } = useFormState({ control, name: "mapTables.tables" });

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
            const tables = parseRawTablesToForm(rawContent);
            setValue("mapTables.tables", tables, { shouldDirty: true, shouldValidate: true });
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
            setValue("mapTables.tables", tables, { shouldDirty: true });
        }
    };

    return (
        <div className="grid gap-3">
            <Field orientation="horizontal" className="justify-self-end">
                <Switch id="map-tables-raw-view" checked={isRawView} onCheckedChange={handleToggleRawView} />
                <FieldLabel htmlFor="map-tables-raw-view">Raw JSON</FieldLabel>
            </Field>

            {isRawView ? (
                <AceEditor
                    mode="json"
                    value={rawContent}
                    onChange={handleRawChange}
                    height={`${RAW_EDITOR_HEIGHT_PX}px`}
                    maxHeight={Infinity}
                    actions={[{ component: <AceEditor.FormatAction /> }, { component: <AceEditor.FullScreenAction /> }]}
                />
            ) : (
                <>
                    <ResizablePanelGroup orientation="horizontal" className="h-[34rem] rounded-lg border bg-background">
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

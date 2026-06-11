import { useFormContext, useFormState } from "react-hook-form";
import { Alert } from "@/components/shadcn/ui/alert";
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from "@/components/shadcn/ui/resizable";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { TableEditor } from "@/pages/setup/add-app-wizard/steps/map-tables/table-editor";
import { TablesExplorer } from "@/pages/setup/add-app-wizard/steps/map-tables/tables-explorer";

export function MapTablesStep() {
    const { control } = useFormContext<AppFormData>();
    const { errors } = useFormState({ control, name: "mapTables.tables" });

    const tablesError = errors.mapTables?.tables;
    const tablesErrorMessage = tablesError?.message ?? tablesError?.root?.message;

    return (
        <div className="grid gap-3">
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
        </div>
    );
}

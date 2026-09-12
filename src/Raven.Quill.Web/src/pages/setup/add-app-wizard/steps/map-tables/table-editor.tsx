import { Fragment } from "react";
import { useFormContext, useWatch, type FieldPath } from "react-hook-form";
import {
    Breadcrumb,
    BreadcrumbItem,
    BreadcrumbLink,
    BreadcrumbList,
    BreadcrumbPage,
    BreadcrumbSeparator,
} from "@/components/shadcn/ui/breadcrumb";
import { Alert } from "@/components/shadcn/ui/alert";
import { Label } from "@/components/shadcn/ui/label";
import { Switch } from "@/components/shadcn/ui/switch";
import { Text } from "@/components/typography";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { EmbeddedTableEditor } from "@/pages/setup/add-app-wizard/steps/map-tables/embedded-table-editor";
import { LinkedTableEditor } from "@/pages/setup/add-app-wizard/steps/map-tables/linked-table-editor";
import {
    castToRootTablePath,
    type MapActiveTable,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import { RootTableEditor } from "@/pages/setup/add-app-wizard/steps/map-tables/root-table-editor";
import { useTableActions } from "@/pages/setup/add-app-wizard/steps/map-tables/use-table-actions";
import { useTableBreadcrumbs } from "@/pages/setup/add-app-wizard/steps/map-tables/use-table-breadcrumbs";

export function TableEditor() {
    const mapActiveTable = useSetupWizardStore((state) => state.mapActiveTable);

    if (!mapActiveTable) {
        return <EmptyEditorMessage />;
    }

    return <ActiveTableEditor key={mapActiveTable.path} activeTable={mapActiveTable} />;
}

function ActiveTableEditor({ activeTable }: { activeTable: MapActiveTable }) {
    const { control } = useFormContext<AppFormData>();
    const setMapActiveTable = useSetupWizardStore((state) => state.setMapActiveTable);
    const tableActions = useTableActions();
    const breadcrumbItems = useTableBreadcrumbs(activeTable.path);

    const rootTablePath = castToRootTablePath(activeTable.path.split(".").slice(0, 3).join("."));
    const isRootTableDisabled = Boolean(useWatch({ control, name: `${rootTablePath}.disabled` }));
    const activeTableValue = useWatch({ control, name: activeTable.path as FieldPath<AppFormData> });

    // The selection can briefly point at a removed index, e.g. right after the mapping is regenerated.
    if (!activeTableValue) {
        return <EmptyEditorMessage />;
    }

    return (
        <div className="flex h-full min-h-0 flex-col">
            <div className="flex items-center justify-between gap-3 border-b px-3 py-2">
                <Breadcrumb>
                    <BreadcrumbList>
                        {breadcrumbItems.map((item, idx) => (
                            <Fragment key={item.path}>
                                {idx > 0 && <BreadcrumbSeparator />}
                                <BreadcrumbItem>
                                    {item.isActive ? (
                                        <BreadcrumbPage>{item.label}</BreadcrumbPage>
                                    ) : (
                                        <BreadcrumbLink asChild>
                                            <button type="button" onClick={() => setMapActiveTable(item)}>
                                                {item.label}
                                            </button>
                                        </BreadcrumbLink>
                                    )}
                                </BreadcrumbItem>
                            </Fragment>
                        ))}
                    </BreadcrumbList>
                </Breadcrumb>
                {activeTable.type === "root" && (
                    <div className="flex shrink-0 items-center gap-2">
                        <Label htmlFor="root-table-enabled-switch" className="text-muted-foreground">
                            Enabled
                        </Label>
                        <Switch
                            id="root-table-enabled-switch"
                            checked={!isRootTableDisabled}
                            onCheckedChange={() => tableActions.toggleRootTableDisabled(rootTablePath)}
                        />
                    </div>
                )}
            </div>
            <div className="min-h-0 flex-1 overflow-y-auto p-4">
                {isRootTableDisabled && (
                    <Alert className="mb-4 flex flex-row items-center justify-between gap-3">
                        This table is disabled and will be skipped.
                    </Alert>
                )}
                <fieldset disabled={isRootTableDisabled} className={cn(isRootTableDisabled && "opacity-60")}>
                    {activeTable.type === "root" && <RootTableEditor path={activeTable.path} />}
                    {activeTable.type === "embedded" && <EmbeddedTableEditor path={activeTable.path} />}
                    {activeTable.type === "linked" && <LinkedTableEditor path={activeTable.path} />}
                </fieldset>
            </div>
        </div>
    );
}

function EmptyEditorMessage() {
    return (
        <Text variant="muted" as="div" className="flex h-full items-center justify-center p-6">
            Select a table to view its configuration.
        </Text>
    );
}

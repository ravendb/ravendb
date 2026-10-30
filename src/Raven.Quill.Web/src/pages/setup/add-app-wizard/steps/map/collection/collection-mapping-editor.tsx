import { type ReactNode, useEffect, useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { CircleAlert } from "lucide-react";
import { FormProvider, useForm, useFormContext, useFormState, useWatch, type FieldPath } from "react-hook-form";
import z from "zod";
import type { CdcSinkTableConfig, ValidationIssue } from "@/api/generated/server-api";
import { CopyableCode } from "@/components/data/copyable-code";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/shadcn/ui/tabs";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { tablesSchema, type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { buildExplorerRows, hasChildTables } from "@/pages/setup/add-app-wizard/steps/map-tables/build-explorer-rows";
import { ExplorerRowItem } from "@/pages/setup/add-app-wizard/steps/map-tables/explorer-rows";
import { mapDtoTableToForm, mapFormTablesToDto } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";
import {
    getRootTablePath,
    type FormRootTable,
    type MapActiveTable,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import { formatTableScripts, getErrorAtPath } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { RootTablesFieldArrayProvider } from "@/pages/setup/add-app-wizard/steps/map-tables/root-tables-field-array";
import { ActiveTableEditor, TableEditor } from "@/pages/setup/add-app-wizard/steps/map-tables/table-editor";
import {
    isSameConfig,
    patchEdit,
    toEditorFieldPath,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-edits";
import type { PlanCollection } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";

const editorSchema = z.object({ mapTables: z.object({ tables: tablesSchema }) });

type EditorForm = z.infer<typeof editorSchema>;

const ROOT_PATH = getRootTablePath(0);
const ROOT_TABLE: MapActiveTable = { type: "root", path: ROOT_PATH };

/**
 * Holds the mapping as the editor form's one table. The form starts from the edit or the plan, so it must
 * be mounted again when either changes underneath it.
 */
export function CollectionMappingEditor({
    title,
    collection,
    builtConfig,
    issues,
    isDisabled,
}: {
    /** Shown on the row of the Fields/JSON switch. */
    title: ReactNode;
    collection: PlanCollection;
    /** The mapping as the plan stores it; a form that matches it again is no edit. */
    builtConfig: CdcSinkTableConfig;
    /** The agent's rejection, or the last save's. */
    issues: ValidationIssue[];
    isDisabled: boolean;
}) {
    const stagePlannerEdit = useSetupWizardStore((state) => state.stagePlannerEdit);
    const [built] = useState(() => formatTableScripts(mapDtoTableToForm(builtConfig)));
    const [builtDto] = useState(() => mapFormTablesToDto([built])[0]);
    const editedConfig = collection.edit?.config;
    const form = useForm<EditorForm>({
        mode: "onChange",
        resolver: zodResolver(editorSchema),
        defaultValues: { mapTables: { tables: [editedConfig ? mapDtoTableToForm(editedConfig) : built] } },
    });
    const { name, area } = collection;

    useEffect(
        () =>
            form.subscribe({
                formState: { values: true },
                callback: ({ values }) => {
                    const table = values.mapTables?.tables?.[0];

                    if (!table) {
                        return;
                    }

                    const config = mapFormTablesToDto([table])[0];
                    const edit = useSetupWizardStore.getState().plannerEdits[name];

                    stagePlannerEdit(
                        name,
                        patchEdit(area, edit, { config: isSameConfig(config, builtDto) ? null : config }),
                    );
                },
            }),
        [form, name, area, builtDto, stagePlannerEdit],
    );

    useEffect(() => {
        void form.trigger().then(() => {
            for (const issue of issues) {
                const path = toEditorFieldPath(issue.path);

                if (path) {
                    form.setError(path as FieldPath<EditorForm>, { type: "server", message: issue.message });
                }
            }
        });
    }, [form, issues]);

    return (
        <FormProvider {...form}>
            <RootTablesFieldArrayProvider>
                <Tabs defaultValue="fields" className="gap-3">
                    <div className="flex flex-wrap items-center justify-between gap-3">
                        {title}
                        <TabsList>
                            <TabsTrigger value="fields">Fields</TabsTrigger>
                            <TabsTrigger value="json">JSON</TabsTrigger>
                        </TabsList>
                    </div>
                    <TabsContent value="fields">
                        <CollectionMappingFields isDisabled={isDisabled} />
                    </TabsContent>
                    <TabsContent value="json">
                        <MappingJson name={name} />
                    </TabsContent>
                </Tabs>
            </RootTablesFieldArrayProvider>
        </FormProvider>
    );
}

/**
 * The table list is shown only once the collection has embedded or linked tables. Without it the editor is
 * pinned to the collection's table, because nothing else could select it again, e.g. after removing the last
 * nested table.
 */
function CollectionMappingFields({ isDisabled }: { isDisabled: boolean }) {
    const { control } = useFormContext<AppFormData>();
    const rootTable = useWatch({ control, name: ROOT_PATH });
    const hasNestedTables = hasChildTables(rootTable);

    return (
        <fieldset
            disabled={isDisabled}
            className={cn(
                "grid min-h-96 grid-cols-1 overflow-hidden rounded-lg border bg-background",
                hasNestedTables && "@3xl:grid-cols-[15rem_minmax(0,1fr)]",
                isDisabled && "opacity-60",
            )}
        >
            {hasNestedTables ? (
                <>
                    <CollectionTablesNav />
                    <div className="min-w-0 border-t @3xl:border-t-0 @3xl:border-l">
                        <TableEditor />
                    </div>
                </>
            ) : (
                <ActiveTableEditor activeTable={ROOT_TABLE} />
            )}
        </fieldset>
    );
}

function CollectionTablesNav() {
    const { control } = useFormContext<AppFormData>();
    const tables = useWatch({ control, name: "mapTables.tables" }) ?? [];
    const expandedPaths = useSetupWizardStore((state) => state.mapExpandedPaths);
    const rows = buildExplorerRows({ tables, expandedPaths: { ...expandedPaths, [ROOT_PATH]: true }, filter: "" });

    return (
        <div className="flex flex-col gap-0.5 p-2">
            {rows.map((row) =>
                row.type === "root" ? (
                    <RootTableRow key={row.rowKey} table={row.table} />
                ) : row.type === "schema" ? null : (
                    <ExplorerRowItem key={row.rowKey} row={row} />
                ),
            )}
        </div>
    );
}

/** The collection itself cannot be removed from here, unlike the tables nested in it. */
function RootTableRow({ table }: { table: FormRootTable }) {
    const { control } = useFormContext<AppFormData>();
    const { errors } = useFormState({ control, name: ROOT_PATH });
    const isActive = useSetupWizardStore((state) => state.mapActiveTable?.path === ROOT_PATH);
    const setMapActiveTable = useSetupWizardStore((state) => state.setMapActiveTable);
    const hasError = Boolean(getErrorAtPath(errors, ROOT_PATH));

    return (
        <button
            type="button"
            className={cn(
                "flex h-7 min-w-0 items-center gap-1 rounded-md px-1.5 text-left text-sm",
                isActive ? "bg-accent text-accent-foreground" : "hover:bg-accent/50",
            )}
            onClick={() => setMapActiveTable({ type: "root", path: ROOT_PATH })}
        >
            <span className="truncate">{table.sourceTableName || "Unassigned table"}</span>
            {hasError && <CircleAlert className="size-3.5 shrink-0 text-destructive" aria-label="Has errors" />}
        </button>
    );
}

function MappingJson({ name }: { name: string }) {
    const { control } = useFormContext<AppFormData>();
    const table = useWatch({ control, name: ROOT_PATH });

    return (
        <CopyableCode
            code={JSON.stringify(mapFormTablesToDto([table])[0], null, 2)}
            copyLabel={`Copy the ${name} mapping`}
            language="javascript"
        />
    );
}

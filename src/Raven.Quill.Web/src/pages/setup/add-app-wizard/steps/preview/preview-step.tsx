import { useFormContext, useWatch } from "react-hook-form";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { FormSelect } from "@/components/form/form-select";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { FormInput } from "@/components/form/form-input";
import { ConfirmDialog } from "@/components/shadcn/ui/confirm-dialog";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { Alert, AlertDescription, AlertTitle } from "@/components/shadcn/ui/alert";
import { WizardErrorList } from "@/components/form/wizard/wizard-error-list";
import AceEditor from "@/components/ace-editor/ace-editor";
import { CircleAlertIcon, DownloadIcon, MessageSquareWarningIcon } from "lucide-react";
import { buildConfigExport, downloadConfig } from "@/pages/setup/add-app-wizard/config-io";

// Both failure paths report the same thing: the request itself failed, or it came back with errors.
const MAPPING_TEST_ERROR_TITLE = "Testing the mapping failed";

export function PreviewStep() {
    const { control, getValues } = useFormContext<AppFormData>();

    const tables = useWatch({
        control,
        name: "mapTables.tables",
    });

    const dataSource = useWatch({
        control,
        name: "dataSource.source",
    });

    return (
        <>
            {dataSource === "external" && (
                <div className="flex justify-end">
                    <ConfirmDialog
                        variant="warning"
                        trigger={
                            <Button type="button" variant="outline" disabled={tables.length === 0}>
                                <DownloadIcon aria-hidden="true" />
                                Export configuration
                            </Button>
                        }
                        title="Export configuration?"
                        description="The exported file contains the connection string in plain text, including any username and password it holds. Keep it somewhere safe and avoid sharing it."
                        confirmLabel="Export"
                        onConfirm={() => downloadConfig(buildConfigExport(getValues()))}
                    />
                </div>
            )}
            <div className="grid grid-cols-2 gap-4">
                <FormSelect
                    control={control}
                    name="preview.table"
                    options={tables.map((t) => ({ value: getTableLabel(t), label: getTableLabel(t) }))}
                    placeholder="Select a table to preview"
                    label="Table"
                />
                <FormInput control={control} name="preview.maxRows" label="Max Rows" type="number" min={1} max={1000} />
            </div>
            <PreviewResult />
        </>
    );
}

function PreviewResult() {
    const { control } = useFormContext<AppFormData>();

    const tables = useWatch({
        control,
        name: "mapTables.tables",
    });

    const selectedTableLabel = useWatch({
        control,
        name: "preview.table",
    });

    const maxRows = useWatch({
        control,
        name: "preview.maxRows",
    });

    const slug = useWatch({
        control,
        name: "externalConnection.slug",
    });

    const selectedTable = tables?.find((t) => getTableLabel(t) === selectedTableLabel);

    const testMappingQuery = useQuery({
        ...api.queries.setup.testMapping({
            sourceTableName: selectedTable?.sourceTableName ?? "",
            sourceTableSchema: selectedTable?.sourceTableSchema,
            maxRows,
            slug,
        }),
        enabled: !!selectedTable && Boolean(maxRows),
    });

    if (testMappingQuery.isLoading) {
        return <Spinner />;
    }

    if (testMappingQuery.isError) {
        return (
            <Alert variant="destructive" className="mb-4">
                <CircleAlertIcon aria-hidden="true" />
                <AlertTitle>{MAPPING_TEST_ERROR_TITLE}</AlertTitle>
                <AlertDescription>
                    {testMappingQuery.error instanceof Error ? testMappingQuery.error.message : "Unknown error"}
                </AlertDescription>
            </Alert>
        );
    }

    if (!testMappingQuery.data) {
        return null;
    }

    if (testMappingQuery.data.errors?.length) {
        return (
            <WizardErrorList errors={testMappingQuery.data.errors} title={MAPPING_TEST_ERROR_TITLE} className="mb-4" />
        );
    }

    return (
        <div className="grid gap-4">
            {testMappingQuery.data.warnings?.length > 0 && (
                <Alert className="grid gap-2">
                    <MessageSquareWarningIcon color="orange" />
                    {testMappingQuery.data.warnings.map((warning, index) => (
                        <div key={index}>{warning}</div>
                    ))}
                </Alert>
            )}
            {testMappingQuery.data.results.map((row) => (
                <AceEditor
                    key={row.documentId}
                    mode="json"
                    value={JSON.stringify(JSON.parse(String(row.document)), null, 2)}
                    readOnly
                    actions={[{ component: <AceEditor.FullScreenAction /> }, { component: <AceEditor.FormatAction /> }]}
                />
            ))}
        </div>
    );
}

function getTableLabel(table: AppFormData["mapTables"]["tables"][0]) {
    return table.sourceTableSchema ? `${table.sourceTableSchema}.${table.sourceTableName}` : table.sourceTableName;
}

import { useFormContext, useWatch } from "react-hook-form";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { FormSelect } from "@/components/form/form-select";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { FormInput } from "@/components/form/form-input";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { Alert } from "@/components/shadcn/ui/alert";
import AceEditor from "@/components/ace-editor/ace-editor";
import { MessageSquareWarningIcon } from "lucide-react";

export function PreviewStep() {
    const { control } = useFormContext<AppFormData>();

    // TODO handle manual. Or move manual and ai suggested to single step
    const tables = useWatch({
        control,
        name: "mapAiSuggest.tables",
    });

    return (
        <>
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

    // TODO handle manual. Or move manual and ai suggested to single step
    const tables = useWatch({
        control,
        name: "mapAiSuggest.tables",
    });

    const selectedTableLabel = useWatch({
        control,
        name: "preview.table",
    });

    const maxRows = useWatch({
        control,
        name: "preview.maxRows",
    });

    const selectedTable = tables?.find((t) => getTableLabel(t) === selectedTableLabel);

    const testMappingQuery = useQuery({
        ...api.queries.setup.testMapping({
            sourceTableName: selectedTable?.sourceTableName ?? "",
            sourceTableSchema: selectedTable?.sourceTableSchema,
            maxRows,
        }),
        enabled: !!selectedTable && Boolean(maxRows),
    });

    if (testMappingQuery.isLoading) {
        return <Spinner />;
    }

    if (testMappingQuery.isError) {
        return (
            <Alert variant="destructive" className="mb-4">
                Error testing mapping:{" "}
                {testMappingQuery.error instanceof Error ? testMappingQuery.error.message : "Unknown error"}
            </Alert>
        );
    }

    if (!testMappingQuery.data) {
        return null;
    }

    if (testMappingQuery.data.errors?.length) {
        return (
            <Alert variant="destructive" className="mb-4 grid gap-2">
                {testMappingQuery.data.errors.map((error, index) => (
                    <div key={index}>{error}</div>
                ))}
            </Alert>
        );
    }

    return (
        <div className="grid gap-4">
            {testMappingQuery.data.warnings?.length && (
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

function getTableLabel(table: AppFormData["mapAiSuggest"]["tables"][0]) {
    return table.sourceTableSchema ? `${table.sourceTableSchema}.${table.sourceTableName}` : table.sourceTableName;
}

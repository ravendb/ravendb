import AceEditor from "@/components/ace-editor/ace-editor";
import { Alert } from "@/components/shadcn/ui/alert";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { useFormContext, useWatch } from "react-hook-form";

export function MapAiSuggestStep() {
    const { control } = useFormContext<AppFormData>();

    const tables = useWatch({
        control,
        name: "mapAiSuggest.tables",
    });

    return (
        <>
            {tables?.length > 0 ? (
                <AceEditor
                    value={JSON.stringify(tables, null, 2)}
                    readOnly
                    mode="json"
                    actions={[{ component: <AceEditor.FullScreenAction /> }]}
                    height="400px"
                />
            ) : (
                <Alert>Go back and run AI Suggest to generate a draft mapping.</Alert>
            )}
        </>
    );
}

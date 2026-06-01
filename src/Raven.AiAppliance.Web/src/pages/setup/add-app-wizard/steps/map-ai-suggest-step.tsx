import AceEditor from "@/components/ace-editor/ace-editor";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import { Alert } from "@/components/shadcn/ui/alert";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { StepSection } from "@/pages/setup/add-app-wizard/wizard-step-section";
import { useFormContext, useWatch } from "react-hook-form";

export function MapAiSuggestStep(props: WizardBodyComponentProps) {
    const { control } = useFormContext<AppFormData>();
    const tables = useWatch({
        control,
        name: "mapAiSuggest.tables",
    });

    return (
        <StepSection {...props}>
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
        </StepSection>
    );
}

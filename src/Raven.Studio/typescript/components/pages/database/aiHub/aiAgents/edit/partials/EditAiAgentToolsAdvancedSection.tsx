import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import { FormGroup, FormInput, FormLabel, FormSwitch } from "components/common/Form";

export default function EditAiAgentToolsAdvancedSection() {
    const { control } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    return (
        <>
            <h4 className="m-0 mt-3 hstack gap-2">
                Advanced
                <FormSwitch control={control} name="isToolsAdvancedSettings" />
            </h4>
            <div className="mb-1">Set your advanced settings here.</div>
            {formValues.isToolsAdvancedSettings && (
                <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                    <FormGroup>
                        <FormLabel>Max model iterations per call</FormLabel>
                        <FormInput
                            type="number"
                            control={control}
                            name="maxModelIterationsPerCall"
                            placeholder="Default (16)"
                        />
                    </FormGroup>
                </div>
            )}
        </>
    );
}

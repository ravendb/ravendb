import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import { FormGroup, FormInput, FormLabel, FormSwitch } from "components/common/Form";
import { useEffect } from "react";

export default function EditAiAgentToolsAdvancedSection() {
    const { control, watch, setValue } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            if (name === "isToolsAdvancedSettings" && !values.isToolsAdvancedSettings) {
                setValue("maxModelIterationsPerCall", null, { shouldValidate: true });
            }
        });
        return () => unsubscribe();
    }, [setValue, watch]);

    return (
        <>
            <h4 className="m-0 mt-3 hstack gap-2">
                Advanced
                <FormSwitch control={control} name="isToolsAdvancedSettings" />
            </h4>
            <div className="mb-1">Set your advanced settings here.</div>
            <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                <FormGroup>
                    <FormLabel>Max model iterations per call</FormLabel>
                    <FormInput
                        type="number"
                        control={control}
                        name="maxModelIterationsPerCall"
                        placeholder="Default (16)"
                        disabled={!formValues.isToolsAdvancedSettings}
                    />
                </FormGroup>
            </div>
        </>
    );
}

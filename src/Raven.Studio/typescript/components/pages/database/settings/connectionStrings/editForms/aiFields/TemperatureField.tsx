import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { FieldPath, useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { FormCheckbox, FormInput, FormLabel } from "components/common/Form";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useEffect } from "react";
import InputGroup from "react-bootstrap/InputGroup";

type FormData = ConnectionFormData<AiConnection>;

interface TemperatureFieldProps {
    baseName: Extract<FormData["connectorType"], "ollamaSettings" | "azureOpenAiSettings" | "openAiSettings">;
}

export default function TemperatureField({ baseName }: TemperatureFieldProps) {
    const { control, setValue, watch } = useFormContext<FormData>();

    const formValues = useWatch({ control });

    const fieldNameIsSetTemperature = `${baseName}.isSetTemperature` satisfies FieldPath<FormData>;
    const fieldNameTemperature = `${baseName}.temperature` satisfies FieldPath<FormData>;

    // Reset temperature when isSetTemperature is disabled
    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            if (name === fieldNameIsSetTemperature && !values[baseName].isSetTemperature) {
                setValue(fieldNameTemperature, null, { shouldValidate: true });
            }
        });

        return () => unsubscribe();
    }, [setValue, watch]);

    if (formValues.modelType !== "Chat") {
        return null;
    }

    return (
        <div className="mb-2">
            <FormLabel>
                Temperature
                <PopoverWithHoverWrapper
                    message={
                        <>
                            Controls randomness of the model output. Range typically [0.0, 2.0].
                            <br />
                            <br />
                            Higher values (e.g., 1.0+) make output more creative and diverse.
                            <br />
                            Lower values (e.g., 0.2) make it more deterministic.
                        </>
                    }
                >
                    <Icon icon="info" color="info" margin="ms-1" />
                </PopoverWithHoverWrapper>
            </FormLabel>
            <InputGroup>
                <div className="toggle-field-checkbox">
                    <FormCheckbox control={control} name={fieldNameIsSetTemperature} color="primary" />
                </div>
                <FormInput
                    type="number"
                    control={control}
                    name={fieldNameTemperature}
                    placeholder="e.g. 0.4"
                    disabled={!formValues[baseName].isSetTemperature}
                />
            </InputGroup>
        </div>
    );
}

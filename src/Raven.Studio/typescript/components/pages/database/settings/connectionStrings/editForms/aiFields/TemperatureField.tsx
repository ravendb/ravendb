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
                            Controls the randomness and creativity of the model&apos;s output. Valid values typically
                            range from <code>0.0</code> to <code>2.0</code>:
                            <ul className="mb-0">
                                <li className="mt-1">
                                    Higher values (e.g., <code>1.0</code> or above) produce more diverse and creative
                                    responses.
                                </li>
                                <li className="mt-1">
                                    Lower values (e.g., <code>0.2</code>) result in more focused, consistent, and
                                    deterministic output.
                                </li>
                                <li className="mt-1">
                                    If not explicitly set, <br />
                                    Ollama defaults to a temperature of <code>0.8</code>.
                                </li>
                            </ul>
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

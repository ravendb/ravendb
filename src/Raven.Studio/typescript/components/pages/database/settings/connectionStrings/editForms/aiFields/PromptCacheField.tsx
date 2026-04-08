import { FormLabel, FormSelect } from "components/common/Form";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { SelectOption } from "components/common/select/Select";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { FieldPath, useFormContext, useWatch } from "react-hook-form";

type FormData = ConnectionFormData<AiConnection>;

interface PromptCacheFieldProps {
    baseName: Extract<FormData["connectorType"], "azureOpenAiSettings" | "googleSettings" | "openAiSettings">;
}

export default function PromptCacheField({ baseName }: PromptCacheFieldProps) {
    const { control } = useFormContext<FormData>();

    const modelType = useWatch({ control, name: "modelType" });

    if (modelType !== "Chat") {
        return null;
    }

    const fieldName = `${baseName}.enablePromptCache` satisfies FieldPath<FormData>;

    return (
        <div className="mb-2">
            <FormLabel>
                Prompt Cache Key
                <PopoverWithHoverWrapper
                    message={
                        <>
                            Controls whether RavenDB sends <code>prompt_cache_key</code> with chat completion requests.
                            <ul className="mb-0">
                                <li className="mt-1">
                                    <strong>Default:</strong> use the server&apos;s provider-specific behavior.
                                </li>
                                <li className="mt-1">
                                    <strong>True:</strong> always send the field.
                                </li>
                                <li className="mt-1">
                                    <strong>False:</strong> never send the field.
                                </li>
                            </ul>
                        </>
                    }
                >
                    <Icon icon="info" color="info" margin="ms-1" />
                </PopoverWithHoverWrapper>
            </FormLabel>
            <FormSelect control={control} name={fieldName} options={promptCacheOptions} />
        </div>
    );
}

const promptCacheOptions: SelectOption<boolean>[] = [
    { label: "Default", value: null },
    { label: "True", value: true },
    { label: "False", value: false },
];

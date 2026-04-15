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
    serverDefaultValue: boolean;
}

export default function PromptCacheField({ baseName, serverDefaultValue }: PromptCacheFieldProps) {
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
                            Controls whether RavenDB includes the <code>prompt_cache_key</code> field in chat completion
                            requests sent to the AI provider.
                            <br />
                            <br />
                            This allows the provider to reuse cached prompt prefixes instead of reprocessing the entire
                            conversation history across turns in the same conversation, reducing latency and cost.
                            <br />
                            <br />
                            <ul className="mb-0">
                                <li>
                                    <strong>Default:</strong> <code>{serverDefaultValue ? "True" : "False"}</code>
                                    <br />
                                    {serverDefaultValue
                                        ? "The server sends the cache key by default."
                                        : "The server does not send the cache key by default."}
                                </li>
                                <li className="mt-1">
                                    <strong>True:</strong> Always send the cache key.
                                </li>
                                <li className="mt-1">
                                    <strong>False:</strong> Never send the cache key.
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

const promptCacheOptions: SelectOption<boolean | null>[] = [
    { label: "Default", value: null },
    { label: "True", value: true },
    { label: "False", value: false },
];

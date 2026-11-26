import { FormLabel, FormSelect } from "components/common/Form";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { SelectOption } from "components/common/select/Select";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { ReactNode } from "react";
import { FieldPath, useFormContext, useWatch } from "react-hook-form";

type FormData = ConnectionFormData<AiConnection>;
type OpenAiChatApiType = Raven.Client.Documents.Operations.AI.OpenAiChatApiType;

interface ChatApiFieldProps {
    baseName: Extract<FormData["connectorType"], "openAiSettings" | "azureOpenAiSettings" | "ollamaSettings">;
    defaultDescription: ReactNode;
}

export default function ChatApiField({ baseName, defaultDescription }: ChatApiFieldProps) {
    const { control } = useFormContext<FormData>();

    const modelType = useWatch({ control, name: "modelType" });

    if (modelType !== "Chat") {
        return null;
    }

    const fieldName = `${baseName}.chatApi` satisfies FieldPath<FormData>;

    return (
        <div className="mb-2">
            <FormLabel>
                Chat API
                <PopoverWithHoverWrapper
                    message={
                        <>
                            The API that RavenDB sends chat requests to.
                            <ul className="mb-0">
                                <li className="mt-1">
                                    <strong>Default:</strong> {defaultDescription}
                                </li>
                                <li className="mt-1">
                                    <strong>Responses:</strong> the <code>/responses</code> API. Reasoning models keep
                                    the configured reasoning effort while they call tools.
                                </li>
                                <li className="mt-1">
                                    <strong>Chat Completions:</strong> the <code>/chat/completions</code> API, supported
                                    by most OpenAI-compatible providers. Newer OpenAI models cannot combine reasoning
                                    with tools there, so RavenDB turns reasoning off for them when tools are used.
                                </li>
                            </ul>
                        </>
                    }
                >
                    <Icon icon="info" color="info" margin="ms-1" />
                </PopoverWithHoverWrapper>
            </FormLabel>
            <FormSelect control={control} name={fieldName} options={CHAT_API_OPTIONS} />
        </div>
    );
}

const CHAT_API_OPTIONS: SelectOption<OpenAiChatApiType | null>[] = [
    { label: "Default", value: null },
    { label: "Responses", value: "Responses" },
    { label: "Chat Completions", value: "ChatCompletions" },
];

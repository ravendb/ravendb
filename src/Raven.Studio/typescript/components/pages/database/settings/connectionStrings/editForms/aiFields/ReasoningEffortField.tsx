import { FormLabel, FormSelectAutocomplete } from "components/common/Form";
import { Icon } from "components/common/Icon";
import OptionalLabel from "components/common/OptionalLabel";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { SelectOption } from "components/common/select/Select";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { FieldPath, useFormContext, useWatch } from "react-hook-form";

type FormData = ConnectionFormData<AiConnection>;

interface ReasoningEffortFieldProps {
    baseName: Extract<FormData["connectorType"], "openAiSettings" | "azureOpenAiSettings" | "ollamaSettings">;
}

export default function ReasoningEffortField({ baseName }: ReasoningEffortFieldProps) {
    const { control } = useFormContext<FormData>();

    const modelType = useWatch({ control, name: "modelType" });

    if (modelType !== "Chat") {
        return null;
    }

    const fieldName = `${baseName}.reasoningEffort` satisfies FieldPath<FormData>;

    return (
        <div className="mb-2">
            <FormLabel>
                Reasoning effort <OptionalLabel />
                <PopoverWithHoverWrapper
                    message={
                        <>
                            <p>
                                The reasoning effort to send to the provider, controlling the reasoning depth of
                                supported models (such as the GPT-5 family). Lower values reduce internal reasoning,
                                which may improve latency and reduce variability in responses.
                            </p>
                            <p>
                                Typical values are <code>none</code>, <code>minimal</code>, <code>low</code>,{" "}
                                <code>medium</code>, <code>high</code>, <code>xhigh</code> and <code>max</code>. Model
                                families accept different subsets, for example <code>minimal</code> is rejected by{" "}
                                <code>gpt-5.1</code> and later, which take <code>none</code> instead.
                            </p>
                            {baseName === "azureOpenAiSettings" && (
                                <p>Azure OpenAI receives the reasoning effort only through the Responses API.</p>
                            )}
                            <p className="mb-0">
                                The value is sent as supplied and is not validated by RavenDB. Providers match it
                                exactly, e.g. OpenAI accepts lowercase values only. If not set, the field is omitted and
                                the model applies its own default.
                            </p>
                        </>
                    }
                >
                    <Icon icon="info" color="info" margin="ms-1" />
                </PopoverWithHoverWrapper>
            </FormLabel>
            <FormSelectAutocomplete
                control={control}
                name={fieldName}
                placeholder="Select a reasoning effort or enter a new one (leave empty for model default)"
                options={REASONING_EFFORT_OPTIONS}
                isClearable
            />
        </div>
    );
}

const REASONING_EFFORT_OPTIONS: SelectOption[] = ["none", "minimal", "low", "medium", "high", "xhigh", "max"].map(
    (x) => ({ label: x, value: x })
);

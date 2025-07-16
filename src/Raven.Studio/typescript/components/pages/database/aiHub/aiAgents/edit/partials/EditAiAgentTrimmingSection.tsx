import { FormAceEditor, FormDurationPicker, FormGroup, FormInput, FormLabel } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import ClickableCard from "components/common/ClickableCard";
import OptionalLabel from "components/common/OptionalLabel";
import ReactAce from "react-ace/lib/ace";
import { useRef } from "react";
import AceEditor from "components/common/ace/AceEditor";

export default function EditAiAgentTrimmingSection() {
    const { control, setValue } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const handleSetTrimmingMethod = (method: "Tokens" | "Truncate") => {
        if (formValues.trimming.method === method) {
            setValue("trimming.method", null);
        } else {
            setValue("trimming.method", method);
        }
    };

    return (
        <>
            <h3 className="m-0 mt-3">Configure chat trimming</h3>
            <div className="mb-1">
                Define configuration options for reducing the size of the AI agents chat history.
            </div>
            <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                <FormGroup>
                    <FormLabel>
                        Trimming method <OptionalLabel />
                    </FormLabel>
                    <div className="d-flex gap-2">
                        <ClickableCard
                            icon="tasks-list"
                            title="Summarization by tokens"
                            description="Summarizes chat messages into a compact prompt when token count exceeds a threshold."
                            className="w-50"
                            isSelected={formValues.trimming.method === "Tokens"}
                            onClick={() => handleSetTrimmingMethod("Tokens")}
                        />
                        <ClickableCard
                            icon="collapse-vertical"
                            title="Truncate chat"
                            description="Truncates older chat messages when the number of messages exceeds a maximum length."
                            className="w-50"
                            isSelected={formValues.trimming.method === "Truncate"}
                            onClick={() => handleSetTrimmingMethod("Truncate")}
                        />
                    </div>
                </FormGroup>
                {formValues.trimming.method === "Tokens" && <TokensFields />}
                {formValues.trimming.method === "Truncate" && <TruncateFields />}
            </div>
        </>
    );
}

function TokensFields() {
    const { control } = useFormContext<EditAiAgentFormData>();

    const summarizationTaskBeginningPromptRef = useRef<ReactAce>(null);
    const summarizationTaskEndPromptRef = useRef<ReactAce>(null);

    return (
        <>
            <FormGroup>
                <FormLabel>Summarization task beginning prompt</FormLabel>
                <FormAceEditor
                    aceRef={summarizationTaskBeginningPromptRef}
                    mode="text"
                    control={control}
                    name="trimming.summarizationTaskBeginningPrompt"
                    wrapEnabled
                    setOptions={{
                        indentedSoftWrap: false,
                    }}
                    actions={[{ component: <AceEditor.FullScreenAction /> }]}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>Summarization task end prompt</FormLabel>
                <FormAceEditor
                    aceRef={summarizationTaskEndPromptRef}
                    mode="text"
                    control={control}
                    name="trimming.summarizationTaskEndPrompt"
                    wrapEnabled
                    setOptions={{
                        indentedSoftWrap: false,
                    }}
                    actions={[{ component: <AceEditor.FullScreenAction /> }]}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>
                    Result prefix <OptionalLabel />
                </FormLabel>
                <FormInput
                    type="text"
                    control={control}
                    name="trimming.resultPrefix"
                    placeholder={`Default ("Summary of previous conversation: ")`}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>
                    Max tokens before summarization <OptionalLabel />
                </FormLabel>
                <FormInput
                    type="number"
                    control={control}
                    name="trimming.maxTokensBeforeSummarization"
                    placeholder={`Default (${defaultMaxTokensBeforeSummarization})`}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>
                    Max tokens after summarization <OptionalLabel />
                </FormLabel>
                <FormInput
                    type="number"
                    control={control}
                    name="trimming.maxTokensAfterSummarization"
                    placeholder={`Default (${defaultMaxTokensAfterSummarization})`}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>
                    History duration <OptionalLabel />
                </FormLabel>
                <FormDurationPicker control={control} name="trimming.historyExpirationInSeconds" showDays isFlexGrow />
            </FormGroup>
        </>
    );
}

function TruncateFields() {
    const { control } = useFormContext<EditAiAgentFormData>();

    return (
        <>
            <FormGroup>
                <FormLabel>
                    Messages length before truncate <OptionalLabel />
                </FormLabel>
                <FormInput
                    type="number"
                    control={control}
                    name="trimming.messagesLengthBeforeTruncate"
                    placeholder={`Default (${defaultMessagesLengthBeforeTruncate})`}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>
                    Messages length after truncate <OptionalLabel />
                </FormLabel>
                <FormInput
                    type="number"
                    control={control}
                    name="trimming.messagesLengthAfterTruncate"
                    placeholder={`Default (${defaultMessagesLengthAfterTruncate})`}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>
                    History duration <OptionalLabel />
                </FormLabel>
                <FormDurationPicker control={control} name="trimming.historyExpirationInSeconds" showDays isFlexGrow />
            </FormGroup>
        </>
    );
}

const defaultMessagesLengthBeforeTruncate = 500;
const defaultMessagesLengthAfterTruncate = defaultMessagesLengthBeforeTruncate / 2;
const defaultMaxTokensBeforeSummarization = 32 * 1024;
const defaultMaxTokensAfterSummarization = 1024;

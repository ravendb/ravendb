import { FormDurationPicker, FormGroup, FormInput, FormLabel, FormSwitch, FormErrorIcon } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import { AiAgentTrimmingMethod, EditAiAgentFormData } from "../utils/editAiAgentValidation";
import ClickableCard from "components/common/ClickableCard";
import Button from "react-bootstrap/Button";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import useBoolean from "components/hooks/useBoolean";
import CollapseButton from "components/common/CollapseButton";
import Collapse from "react-bootstrap/Collapse";

export default function EditAiAgentTrimmingSection() {
    const { control, setValue } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const { value: isPanelOpen, setValue: setIsPanelOpen, toggle: toggleIsPanelOpen } = useBoolean(true);

    const handleSetTrimmingMethod = (method: AiAgentTrimmingMethod) => {
        if (formValues.trimming.method === method) {
            setValue("trimming.method", null);
        } else {
            setValue("trimming.method", method);
        }
    };

    return (
        <>
            <div className="hstack mt-3">
                <h3 className="m-0">
                    Configure chat trimming
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                You can configure trimming of long conversations by summarizing older messages.
                                <br />
                                <br />
                                If you &quot;enable history&quot;, the original chat content prior to trimming will be
                                stored in dedicated documents in the <code>@conversations-history</code> collection.
                                <br />
                                <br />
                                Note: if there are any open action tools that have not yet received a response, trimming
                                will be delayed until those actions are completed.
                            </>
                        }
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </h3>
                <FormErrorIcon control={control} paths={["trimming"]} onError={() => setIsPanelOpen(true)} />
                <CollapseButton isExpanded={isPanelOpen} toggle={toggleIsPanelOpen} />
            </div>
            <div className="mb-1">You can configure trimming of long conversations by summarizing older messages.</div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <div>
                    <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                        <FormGroup>
                            <FormLabel className="hstack justify-content-between">
                                <div>Trimming method</div>
                                {formValues.trimming.method != null && (
                                    <Button variant="link" size="sm" onClick={() => setValue("trimming.method", null)}>
                                        Clear selection
                                    </Button>
                                )}
                            </FormLabel>
                            <div className="d-flex">
                                <ClickableCard
                                    icon="tasks-list"
                                    title="Summarize chat"
                                    description="Summarize the chat conversation into a compact prompt when the total number of tokens used exceeds the configured threshold."
                                    className="flex-grow-1"
                                    isSelected={formValues.trimming.method === "Tokens"}
                                    onClick={() => handleSetTrimmingMethod("Tokens")}
                                />
                                {/* maybe add it in RC2
                        <ClickableCard
                            icon="collapse-vertical"
                            title="Truncate chat"
                            description="Remove older messages when the number of chat messages exceeds the configured maximum."
                            className="w-50"
                            isSelected={formValues.trimming.method === "Truncate"}
                            onClick={() => handleSetTrimmingMethod("Truncate")}
                        /> */}
                            </div>
                        </FormGroup>
                        {formValues.trimming.method === "Tokens" && <TokensFields />}
                        {/* {formValues.trimming.method === "Truncate" && <TruncateFields />} */}
                    </div>
                </div>
            </Collapse>
        </>
    );
}

function TokensFields() {
    const { control } = useFormContext<EditAiAgentFormData>();

    return (
        <>
            <FormGroup>
                <FormLabel>
                    Max tokens before summarization
                    <PopoverWithHoverWrapper message="Summarization will be triggered when the total number of tokens used in the conversation exceeds this limit.">
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
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
                    Max tokens after summarization
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                The maximum number of tokens to retain in the conversation after summarization.
                                <br />
                                Messages exceeding this limit will be removed, starting from the oldest.
                            </>
                        }
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput
                    type="number"
                    control={control}
                    name="trimming.maxTokensAfterSummarization"
                    placeholder={`Default (${defaultMaxTokensAfterSummarization})`}
                />
            </FormGroup>
            <HistoryFields />
        </>
    );
}

// function TruncateFields() {
//     const { control } = useFormContext<EditAiAgentFormData>();

//     return (
//         <>
//             <FormGroup>
//                 <FormLabel>
//                     Messages length before truncate
//                     <PopoverWithHoverWrapper message="Truncation is triggered when this number of messages is exceeded.">
//                         <Icon icon="info-new" />
//                     </PopoverWithHoverWrapper>
//                 </FormLabel>
//                 <FormInput
//                     type="number"
//                     control={control}
//                     name="trimming.messagesLengthBeforeTruncate"
//                     placeholder={`Default (${defaultMessagesLengthBeforeTruncate})`}
//                 />
//             </FormGroup>
//             <FormGroup>
//                 <FormLabel>
//                     Messages length after truncate
//                     <PopoverWithHoverWrapper
//                         message={
//                             <>
//                                 The number of most recent messages to keep after truncation.
//                                 <br />
//                                 Older messages beyond this number will be discarded.
//                             </>
//                         }
//                     >
//                         <Icon icon="info-new" />
//                     </PopoverWithHoverWrapper>
//                 </FormLabel>
//                 <FormInput
//                     type="number"
//                     control={control}
//                     name="trimming.messagesLengthAfterTruncate"
//                     placeholder={`Default (${defaultMessagesLengthAfterTruncate})`}
//                 />
//             </FormGroup>
//             <HistoryFields />
//         </>
//     );
// }

function HistoryFields() {
    const { control } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    return (
        <>
            <FormGroup marginClass="mb-0">
                <FormSwitch control={control} name="trimming.isEnableHistory">
                    Enable history
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                Toggle on to keep the original chat content before trimming in a dedicated document in
                                the <code>@conversations-history</code> collection.
                            </>
                        }
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </FormSwitch>
            </FormGroup>
            {formValues.trimming.isEnableHistory && (
                <FormGroup marginClass="mt-1">
                    <FormSwitch control={control} name="trimming.isSetHistoryExpiration">
                        Set history expiration
                        <PopoverWithHoverWrapper message="Toggle on to set how long history documents are retained.">
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </FormSwitch>
                    {formValues.trimming.isSetHistoryExpiration && (
                        <FormDurationPicker
                            control={control}
                            name="trimming.historyExpirationInSeconds"
                            showDays
                            isFlexGrow
                        />
                    )}
                </FormGroup>
            )}
        </>
    );
}

// const defaultMessagesLengthBeforeTruncate = 500;
// const defaultMessagesLengthAfterTruncate = defaultMessagesLengthBeforeTruncate / 2;
const defaultMaxTokensBeforeSummarization = 32 * 1024;
const defaultMaxTokensAfterSummarization = 1024;

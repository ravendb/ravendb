import { FormDurationPicker, FormGroup, FormInput, FormLabel, FormSwitch } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import ClickableCard from "components/common/ClickableCard";
import OptionalLabel from "components/common/OptionalLabel";
import Accordion from "react-bootstrap/Accordion";
import useUniqueId from "components/hooks/useUniqueId";

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

    const formValues = useWatch({
        control,
    });

    const advancedSettingsId = useUniqueId("tokens-advanced");

    const isAdvancedOpen =
        formValues.trimming.isEnableHistory ||
        formValues.trimming.summarizationTaskBeginningPrompt ||
        formValues.trimming.summarizationTaskEndPrompt ||
        formValues.trimming.resultPrefix;

    return (
        <>
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
            <Accordion
                className="trimming-advanced border border-secondary rounded-2 panel-bg-2"
                defaultActiveKey={isAdvancedOpen ? advancedSettingsId : undefined}
            >
                <Accordion.Item eventKey={advancedSettingsId} className="panel-bg-2">
                    <Accordion.Header>Advanced settings</Accordion.Header>
                    <Accordion.Collapse eventKey={advancedSettingsId} mountOnEnter unmountOnExit>
                        <Accordion.Body className="panel-bg-2 rounded-2 py-0">
                            <hr className="mt-0 mb-2" />
                            <FormGroup>
                                <FormLabel>
                                    Summarization task beginning prompt <OptionalLabel />
                                </FormLabel>
                                <FormInput
                                    type="textarea"
                                    as="textarea"
                                    control={control}
                                    name="trimming.summarizationTaskBeginningPrompt"
                                    rows={4}
                                />
                            </FormGroup>
                            <FormGroup>
                                <FormLabel>
                                    Summarization task end prompt <OptionalLabel />
                                </FormLabel>
                                <FormInput
                                    type="textarea"
                                    as="textarea"
                                    control={control}
                                    name="trimming.summarizationTaskEndPrompt"
                                    rows={4}
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
                            <HistoryFields />
                        </Accordion.Body>
                    </Accordion.Collapse>
                </Accordion.Item>
            </Accordion>
        </>
    );
}

function TruncateFields() {
    const { control } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const isAdvancedOpen = formValues.trimming.isEnableHistory;

    const advancedSettingsId = useUniqueId("truncate-advanced");

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
            <Accordion
                className="trimming-advanced border border-secondary rounded-2 panel-bg-2"
                defaultActiveKey={isAdvancedOpen ? advancedSettingsId : undefined}
            >
                <Accordion.Item eventKey={advancedSettingsId} className="panel-bg-2">
                    <Accordion.Header>Advanced settings</Accordion.Header>
                    <Accordion.Collapse eventKey={advancedSettingsId} mountOnEnter unmountOnExit>
                        <Accordion.Body className="panel-bg-2 rounded-2 py-0">
                            <hr className="mt-0 mb-2" />
                            <HistoryFields />
                        </Accordion.Body>
                    </Accordion.Collapse>
                </Accordion.Item>
            </Accordion>
        </>
    );
}

function HistoryFields() {
    const { control } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    return (
        <>
            <FormGroup>
                <FormSwitch control={control} name="trimming.isEnableHistory">
                    Enable history
                </FormSwitch>
            </FormGroup>
            {formValues.trimming.isEnableHistory && (
                <FormGroup>
                    <FormSwitch control={control} name="trimming.isSetHistoryExpiration">
                        Set history expiration
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

const defaultMessagesLengthBeforeTruncate = 500;
const defaultMessagesLengthAfterTruncate = defaultMessagesLengthBeforeTruncate / 2;
const defaultMaxTokensBeforeSummarization = 32 * 1024;
const defaultMaxTokensAfterSummarization = 1024;

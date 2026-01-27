import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { aiAssistantSelectors } from "components/common/shell/aiAssistantSlice";
import ChatbotAskAiAttachedContext from "components/shell/chatbot/partials/askAi/ChatbotAskAiAttachedContext";
import { chatbotActions, chatbotSelectors } from "components/shell/chatbot/store/chatbotSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import moment from "moment";
import ProgressBar from "react-bootstrap/ProgressBar";
import { useForm, useWatch } from "react-hook-form";

export default function ChatbotAskAiPromptPanel() {
    const dispatch = useAppDispatch();
    const consentStatus = useAppSelector(aiAssistantSelectors.consentStatus);
    const isConsentSuccess = consentStatus.data === "Success";

    const { control, formState, handleSubmit, reset } = useForm({
        defaultValues: {
            prompt: "",
        },
    });

    const formValues = useWatch({
        control,
    });

    const handleSend = async () => {
        if (!formValues.prompt.trim()) {
            return;
        }

        reset();
        await dispatch(
            chatbotActions.runChat({
                message: formValues.prompt,
            })
        ).unwrap();
    };

    const isPromptDisabled = formState.isSubmitting || !isConsentSuccess;

    return (
        <>
            <div className="prompt-wrapper">
                <div className="prompt-wrapper-inner">
                    <AttachedContext />
                    <FormInput
                        type="textarea"
                        as="textarea"
                        control={control}
                        name="prompt"
                        placeholder="Ask anything"
                        className="prompt-textarea"
                        disabled={isPromptDisabled}
                        onKeyDown={(e) => {
                            if (e.key === "Enter" && !e.shiftKey) {
                                e.preventDefault();
                                handleSubmit(handleSend)();
                            }
                        }}
                    />
                    {formValues.prompt.trim() && (
                        <div className="hstack justify-content-end">
                            <ButtonWithSpinner
                                variant="secondary"
                                icon="arrow-thin-top"
                                onClick={handleSubmit(handleSend)}
                                className="rounded-pill p-0"
                                style={{ width: "30px", height: "30px" }}
                                isSpinning={isPromptDisabled}
                                title={isPromptDisabled ? "Please wait" : "Submit"}
                            />
                        </div>
                    )}
                </div>
            </div>
            <p className="chatbot-disclaimer mb-0 pt-1 small text-center text-muted">
                Responses are AI-generated and may require verification.
            </p>
            <Usage />
        </>
    );
}

function AttachedContext() {
    const attachedContexts = useAppSelector(chatbotSelectors.attachedContexts);
    return <ChatbotAskAiAttachedContext attachedContexts={attachedContexts} isReadOnly={false} />;
}

const USAGE_THRESHOLD = 60;

function Usage() {
    const usage = useAppSelector(aiAssistantSelectors.usage);
    const isVisible =
        usage.status === "success" && usage.data?.Status === "Success" && usage.data?.UsagePercentage > USAGE_THRESHOLD;

    if (!isVisible) {
        return null;
    }

    const roundedUsage = Math.round(usage.data.UsagePercentage);
    const resetsFormattedDate = moment().add(1, "month").startOf("month").format("MMM D, YYYY");

    return (
        <PopoverWithHoverWrapper
            message={
                <div style={{ width: "200px" }}>
                    <div className="hstack gap-2">
                        <strong>{roundedUsage}% used</strong>
                        <ProgressBar
                            now={roundedUsage}
                            className="tokens-usage-progress-bar flex-grow"
                            style={{ height: "8px" }}
                        />
                    </div>
                    <div className="small">Resets {resetsFormattedDate}</div>
                </div>
            }
        >
            <div className="text-center small">
                <Icon icon="info-new" /> You&apos;ve used {roundedUsage}% of your usage limit.
            </div>
        </PopoverWithHoverWrapper>
    );
}

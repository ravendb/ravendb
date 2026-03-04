import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { aiAssistantSelectors } from "components/common/shell/aiAssistantSlice";
import ChatbotAskAiAttachedContext from "components/shell/chatbot/partials/askAi/ChatbotAskAiAttachedContext";
import {
    chatbotActions,
    chatbotRequestSizeLimitBytes,
    chatbotRequestSizeWarningBytes,
    estimateChatbotRunChatRequestSize,
    chatbotSelectors,
} from "components/shell/chatbot/store/chatbotSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import genUtils from "common/generalUtils";
import moment from "moment";
import ProgressBar from "react-bootstrap/ProgressBar";
import { useForm, useWatch } from "react-hook-form";
import classNames from "classnames";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { clusterSelectors } from "components/common/shell/clusterSlice";

export default function ChatbotAskAiPromptPanel() {
    const dispatch = useAppDispatch();
    const consentStatus = useAppSelector(aiAssistantSelectors.consentStatus);
    const conversationId = useAppSelector(chatbotSelectors.conversationId);
    const attachedContexts = useAppSelector(chatbotSelectors.attachedContexts);
    const buildVersion = useAppSelector(clusterSelectors.serverVersion)?.BuildVersion;

    const isConsentSuccess = consentStatus.data === "Success";

    const { control, formState, handleSubmit, reset } = useForm({
        defaultValues: {
            prompt: "",
        },
    });

    const formValues = useWatch({
        control,
    });

    const prompt = formValues.prompt ?? "";
    const hasPrompt = Boolean(prompt.trim());

    const estimatedRequestSizeInBytes = estimateChatbotRunChatRequestSize({
        ravenVersion: buildVersion,
        message: prompt,
        conversationId,
        attachedContexts: attachedContexts.filter((context) => context.state === "included"),
    });

    const isRequestSizeExceeded = hasPrompt && estimatedRequestSizeInBytes > chatbotRequestSizeLimitBytes;

    const handleSend = async () => {
        if (!hasPrompt || isRequestSizeExceeded) {
            return;
        }

        reset();
        await dispatch(
            chatbotActions.runChat({
                message: prompt,
            })
        ).unwrap();
    };

    const isInputDisabled = formState.isSubmitting || !isConsentSuccess;
    const isSubmitDisabled = isInputDisabled || isRequestSizeExceeded;

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
                        disabled={isInputDisabled}
                        onKeyDown={(e) => {
                            if (e.key === "Enter" && !e.shiftKey) {
                                e.preventDefault();
                                handleSubmit(handleSend)();
                            }
                        }}
                    />
                    {hasPrompt && (
                        <div className="hstack justify-content-end align-items-center gap-2">
                            <RequestSizeIndicator estimatedRequestSizeInBytes={estimatedRequestSizeInBytes} />
                            <ConditionalPopover
                                conditions={[
                                    {
                                        isActive: isRequestSizeExceeded,
                                        message: "Request body is too large. Please remove some attached context.",
                                    },
                                    {
                                        isActive: formState.isSubmitting,
                                        message: "Your request is being processed. Please wait for the response.",
                                    },
                                ]}
                            >
                                <ButtonWithSpinner
                                    variant="secondary"
                                    icon="arrow-thin-top"
                                    onClick={handleSubmit(handleSend)}
                                    className="rounded-pill p-0"
                                    style={{ width: "30px", height: "30px" }}
                                    isSpinning={formState.isSubmitting}
                                    disabled={isSubmitDisabled}
                                    title="Send"
                                />
                            </ConditionalPopover>
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

interface RequestSizeIndicatorProps {
    estimatedRequestSizeInBytes: number;
}

function RequestSizeIndicator({ estimatedRequestSizeInBytes }: RequestSizeIndicatorProps) {
    const sizeState = getRequestSizeState(estimatedRequestSizeInBytes);
    const progress = Math.min(estimatedRequestSizeInBytes / chatbotRequestSizeLimitBytes, 1);
    const sizeText = genUtils.formatBytesToSize(estimatedRequestSizeInBytes, 2);
    const limitText = genUtils.formatBytesToSize(chatbotRequestSizeLimitBytes, 0);

    if (sizeState === "ok") {
        return null;
    }

    return (
        <PopoverWithHoverWrapper
            message={
                <div className="small">
                    <div className="fw-bold">Request body size</div>
                    <div>
                        {sizeText} / {limitText}
                    </div>
                    <div className="small mt-2">Includes server metadata overhead.</div>
                </div>
            }
            inline={false}
            wrapperClassName="hstack"
        >
            <div className="chatbot-request-size-wrapper">
                <div className={classNames("chatbot-request-size-indicator", sizeState)}>
                    <svg viewBox="0 0 36 36" width="12" height="12">
                        <circle className="track" cx="18" cy="18" r="15.9155" />
                        <circle
                            className="value"
                            cx="18"
                            cy="18"
                            r="15.9155"
                            style={{ strokeDasharray: `${progress * 100}, 100` }}
                        />
                    </svg>
                </div>
                <div className="chatbot-request-size-value text-body">{sizeText}</div>
            </div>
        </PopoverWithHoverWrapper>
    );
}

type RequestSizeState = "ok" | "warning" | "error";

function getRequestSizeState(estimatedRequestSizeInBytes: number): RequestSizeState {
    if (estimatedRequestSizeInBytes > chatbotRequestSizeLimitBytes) {
        return "error";
    }
    if (estimatedRequestSizeInBytes > chatbotRequestSizeWarningBytes) {
        return "warning";
    }
    return "ok";
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

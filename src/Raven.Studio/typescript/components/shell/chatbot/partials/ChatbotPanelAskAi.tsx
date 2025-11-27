import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormInput } from "components/common/Form";
import { useAppDispatch, useAppSelector } from "components/store";
import { useForm, useWatch } from "react-hook-form";
import { chatbotSelectors, chatbotActions } from "../store/chatbotSlice";
import ChatbotMessages from "./ChatbotMessages";
import { aiAssistantSelectors } from "components/common/shell/aiAssistantSlice";
import AiAssistantConsentStatusChecker from "components/common/aiAssistant/AiAssistantConsentStatusChecker";
import ChatbotAskAiAttachedContext from "./askAi/ChatbotAskAiAttachedContext";
import ChatbotAskAiCommonActions from "./askAi/ChatbotAskAiCommonActions";

export default function ChatbotPanelAskAi() {
    const dispatch = useAppDispatch();
    const consentStatus = useAppSelector(aiAssistantSelectors.consentStatus);
    const lastRunData = useAppSelector(chatbotSelectors.lastRunData);

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
        reset();
        await dispatch(
            chatbotActions.runChat({
                message: formValues.prompt,
            })
        ).unwrap();
    };

    const onConsentGiven = () => {
        if (lastRunData) {
            dispatch(chatbotActions.retryRunChat());
        }
    };

    const isPromptDisabled = formState.isSubmitting || !isConsentSuccess;

    return (
        <div className="vstack flex-grow py-2 h-100">
            <div className="overflow-y-auto">
                <AiAssistantConsentStatusChecker className="p-2 flex-grow" onConsentGiven={onConsentGiven} />
                <ChatbotAskAiCommonActions />
            </div>
            <ChatbotMessages />
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
                    {formValues.prompt && (
                        <div className="hstack justify-content-end">
                            <ButtonWithSpinner
                                variant="secondary"
                                icon="arrow-up"
                                onClick={handleSubmit(handleSend)}
                                className="rounded-pill p-0"
                                style={{ width: "30px", height: "30px" }}
                                isSpinning={isPromptDisabled}
                            />
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}

function AttachedContext() {
    const attachedContexts = useAppSelector(chatbotSelectors.attachedContexts);
    return <ChatbotAskAiAttachedContext attachedContexts={attachedContexts} isReadOnly={false} />;
}

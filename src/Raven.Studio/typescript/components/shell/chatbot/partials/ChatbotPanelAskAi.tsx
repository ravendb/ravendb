import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormInput } from "components/common/Form";
import { useAppDispatch, useAppSelector } from "components/store";
import { useForm, useWatch } from "react-hook-form";
import { chatbotSelectors, chatbotActions } from "../store/chatbotSlice";
import ChatbotMessages from "./ChatbotMessages";
import router from "plugins/router";
import { aiAssistantSelectors } from "components/common/shell/aiAssistantSlice";
import AiAssistantConsentStatusChecker from "components/common/aiAssistant/AiAssistantConsentStatusChecker";

export default function ChatbotPanelAskAi() {
    const dispatch = useAppDispatch();
    const messages = useAppSelector(chatbotSelectors.messages);
    const consentStatus = useAppSelector(aiAssistantSelectors.consentStatus);
    const lastRunChatData = useAppSelector(chatbotSelectors.lastRunChatData);

    const isConsentSuccess = consentStatus.data === "Success";

    const { control, handleSubmit, formState, reset } = useForm({
        defaultValues: {
            prompt: "",
        },
    });

    const formValues = useWatch({
        control,
    });

    const handleSend = async () => {
        const viewTitle = router.activeInstruction().config.title;
        await dispatch(
            chatbotActions.runChat({
                data: { View: viewTitle, Message: formValues.prompt },
            })
        ).unwrap();
        reset();
    };

    const onConsentGiven = () => {
        if (lastRunChatData) {
            dispatch(chatbotActions.retryRunChat());
        }
    };

    const isPromptDisabled = formState.isSubmitting || !isConsentSuccess;

    return (
        <div className="vstack flex-grow py-2 h-100">
            <AiAssistantConsentStatusChecker className="p-2 flex-grow" onConsentGiven={onConsentGiven} />
            <ChatbotMessages messages={messages} />
            <div className="position-relative flex-shrink-0 px-2 pt-2">
                <FormInput
                    type="textarea"
                    as="textarea"
                    control={control}
                    name="prompt"
                    placeholder="Ask the agent anything"
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
                    <ButtonWithSpinner
                        variant="secondary"
                        icon="arrow-up"
                        onClick={handleSubmit(handleSend)}
                        className="position-absolute rounded-pill"
                        style={{ right: "20px", bottom: "10px", zIndex: 5 }}
                        isSpinning={isPromptDisabled}
                    />
                )}
            </div>
        </div>
    );
}

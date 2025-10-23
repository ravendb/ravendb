import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormInput } from "components/common/Form";
import { useAppDispatch, useAppSelector } from "components/store";
import { useForm, useWatch } from "react-hook-form";
import { chatbotSelectors, chatbotActions } from "../store/chatbotSlice";
import ChatbotMessages from "./ChatbotMessages";

export default function ChatbotPanelAskAi() {
    const dispatch = useAppDispatch();
    const messages = useAppSelector(chatbotSelectors.messages);

    const { control, handleSubmit, formState, reset } = useForm({
        defaultValues: {
            prompt: "",
        },
    });

    const formValues = useWatch({
        control,
    });

    const handleSend = async () => {
        await dispatch(chatbotActions.runChat({ message: formValues.prompt, isContinuation: true })).unwrap();
        reset();
    };

    return (
        <div className="vstack flex-grow p-2">
            <ChatbotMessages messages={messages} />
            <div className="position-relative">
                <FormInput
                    type="textarea"
                    as="textarea"
                    control={control}
                    name="prompt"
                    placeholder="Ask the agent anything"
                    className="prompt-textarea"
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
                        style={{ right: "10px", bottom: "10px", zIndex: 5 }}
                        isSpinning={formState.isSubmitting}
                    />
                )}
            </div>
        </div>
    );
}

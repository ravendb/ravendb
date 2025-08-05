import classNames from "classnames";
import AceEditor from "components/common/ace/AceEditor";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormInput } from "components/common/Form";
import { useAppDispatch, useAppSelector } from "components/store";
import { useRef, useEffect } from "react";
import Spinner from "react-bootstrap/Spinner";
import { useFormContext, useWatch } from "react-hook-form";
import AiAgentMessages from "../../partials/AiAgentMessages";
import AiAgentParametersField from "../../partials/AiAgentParametersField";
import { AiAgentToolCall } from "../../utils/aiAgentsTypes";
import { chatAiAgentSelectors, chatAiAgentActions } from "../store/chatAiAgentSlice";
import { ChatAiAgentFormData } from "../utils/chatAiAgentValidation";
import ChatAiAgentPersistenceSection from "./ChatAiAgentPersistenceSection";

interface ChatAiAgentFormBodyProps {
    height: number;
    handleSend: () => Promise<void>;
    runChat: (toolCallParameters?: AiAgentToolCall[]) => Promise<void>;
    isHistory: boolean;
}

export default function ChatAiAgentFormBody({ height, handleSend, runChat, isHistory }: ChatAiAgentFormBodyProps) {
    const dispatch = useAppDispatch();

    const messagesPanelRef = useRef<HTMLDivElement>(null);

    const hasScroll = useAppSelector(chatAiAgentSelectors.hasScroll);
    const messages = useAppSelector(chatAiAgentSelectors.messages);
    const config = useAppSelector(chatAiAgentSelectors.config);
    const isRawData = useAppSelector(chatAiAgentSelectors.isRawData);
    const document = useAppSelector(chatAiAgentSelectors.document);
    const isLoading = useAppSelector(chatAiAgentSelectors.isLoading);
    const isWaitingForActionToolSubmit = useAppSelector(chatAiAgentSelectors.isWaitingForActionToolSubmit);

    const { control, handleSubmit } = useFormContext<ChatAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    // Scroll to the bottom of the test panel when new messages are added and set hasScroll
    useEffect(() => {
        if (!messagesPanelRef.current) {
            return;
        }

        dispatch(
            chatAiAgentActions.hasScrollSet(
                messagesPanelRef.current.scrollHeight > messagesPanelRef.current.clientHeight
            )
        );

        if (messagesPanelRef.current) {
            messagesPanelRef.current.scrollTo({
                top: messagesPanelRef.current.scrollHeight,
                behavior: "smooth",
            });
        }
    }, [messages.length]);

    return (
        <>
            <div
                ref={messagesPanelRef}
                className={classNames(
                    "overflow-auto ps-2 flex-grow-1 position-relative d-flex justify-content-center",
                    { "pe-2": !hasScroll }
                )}
                style={{ height: height - promptHeightInPx }}
            >
                <div className="w-100" style={{ maxWidth: "800px" }}>
                    {messages.length === 0 && (
                        <div className="h-100 vstack justify-content-center">
                            <ChatAiAgentPersistenceSection />
                            <AiAgentParametersField control={control} name="parameters" value={formValues.parameters} />
                        </div>
                    )}
                    {!isRawData && messages.length > 0 && (
                        <AiAgentMessages
                            messages={messages}
                            toolQueries={config.data?.Queries}
                            toolActions={config.data?.Actions}
                            handleSaveParameters={runChat}
                            setIsWaitingForActionToolSubmit={(value: boolean) =>
                                dispatch(chatAiAgentActions.isWaitingForActionToolSubmitSet(value))
                            }
                        />
                    )}
                    {isRawData && document.data && (
                        <AceEditor
                            mode="json"
                            value={JSON.stringify(document.data, null, 2)}
                            height={`${height - promptHeightInPx}px`}
                            readOnly
                        />
                    )}
                    {isLoading && (
                        <div className="position-absolute top-50 start-50 translate-middle">
                            <Spinner animation="border" />
                        </div>
                    )}
                </div>
            </div>
            {!isHistory && (
                <div className="d-flex justify-content-center mt-3 px-3 pb-3">
                    <div className="w-100" style={{ maxWidth: "800px" }}>
                        <div className="position-relative">
                            <FormInput
                                type="textarea"
                                as="textarea"
                                control={control}
                                name="prompt"
                                placeholder="Ask the agent anything"
                                className="rounded-2"
                                rows={3}
                                onKeyDown={(e) => {
                                    if (e.key === "Enter" && !e.shiftKey) {
                                        e.preventDefault();
                                        handleSubmit(handleSend)();
                                    }
                                }}
                                disabled={isLoading || isWaitingForActionToolSubmit}
                            />
                            {formValues.prompt && (
                                <ButtonWithSpinner
                                    type="submit"
                                    variant="secondary"
                                    icon="arrow-up"
                                    isSpinning={isLoading}
                                    disabled={isLoading || isWaitingForActionToolSubmit}
                                    className="position-absolute rounded-pill"
                                    style={{ right: "10px", bottom: "10px", zIndex: 5 }}
                                />
                            )}
                        </div>
                    </div>
                </div>
            )}
        </>
    );
}

const promptHeightInPx = 150;

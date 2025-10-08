import classNames from "classnames";
import AceEditor from "components/common/ace/AceEditor";
import { FormInput } from "components/common/Form";
import { useAppDispatch, useAppSelector } from "components/store";
import { useRef, useEffect } from "react";
import Spinner from "react-bootstrap/Spinner";
import { useFormContext, useWatch, UseFieldArrayReturn } from "react-hook-form";
import AiAgentMessages from "../../partials/AiAgentMessages";
import AiAgentParametersField from "../../partials/AiAgentParametersField";
import { AiAgentToolCall } from "../../utils/aiAgentsTypes";
import { chatAiAgentSelectors, chatAiAgentActions } from "../store/chatAiAgentSlice";
import { ChatAiAgentFormData } from "../utils/chatAiAgentValidation";
import ChatAiAgentPersistenceSection from "./ChatAiAgentPersistenceSection";
import RichAlert from "components/common/RichAlert";
import Button from "react-bootstrap/Button";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "components/hooks/useAppUrls";
import "./ChatAiAgentFormBody.scss";
import ChatAiAgentPromptActions from "./ChatAiAgentPromptActions";

interface ChatAiAgentFormBodyProps {
    height: number;
    handleSend: () => Promise<void>;
    runChat: (toolCallParameters?: AiAgentToolCall[]) => Promise<void>;
    isHistory: boolean;
    promptsFieldsArray: UseFieldArrayReturn<ChatAiAgentFormData, "prompts", "id">;
}

export default function ChatAiAgentFormBody({
    height,
    handleSend,
    runChat,
    isHistory,
    promptsFieldsArray,
}: ChatAiAgentFormBodyProps) {
    const dispatch = useAppDispatch();

    const messagesPanelRef = useRef<HTMLDivElement>(null);

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const conversationId = useAppSelector(chatAiAgentSelectors.conversationId);
    const hasScroll = useAppSelector(chatAiAgentSelectors.hasScroll);
    const messages = useAppSelector(chatAiAgentSelectors.messages);
    const config = useAppSelector(chatAiAgentSelectors.config);
    const isRawData = useAppSelector(chatAiAgentSelectors.isRawData);
    const document = useAppSelector(chatAiAgentSelectors.document);
    const isLoading = useAppSelector(chatAiAgentSelectors.isLoading);
    const isWaitingForActionToolSubmit = useAppSelector(chatAiAgentSelectors.isWaitingForActionToolSubmit);
    const isDocumentDeleted = useAppSelector(chatAiAgentSelectors.isDocumentDeleted);
    const isDocumentChanged = useAppSelector(chatAiAgentSelectors.isDocumentChanged);
    const activePromptIndex = useAppSelector(chatAiAgentSelectors.activePromptIndex);

    const { appUrl } = useAppUrls();

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

    const handleRefreshDocument = async () => {
        await dispatch(chatAiAgentActions.getDocument({ databaseName, id: conversationId })).unwrap();
        dispatch(chatAiAgentActions.isDocumentChangedSet(false));
    };

    const isPromptDisabled = isLoading || isWaitingForActionToolSubmit || isDocumentDeleted || isDocumentChanged;

    return (
        <>
            <div
                ref={messagesPanelRef}
                className={classNames(
                    "ai-agents overflow-auto ps-2 flex-grow-1 position-relative d-flex justify-content-center",
                    { "pe-2": !hasScroll }
                )}
                style={{ height: height - promptHeightInPx }}
            >
                <div className="w-100" style={{ maxWidth: "800px" }}>
                    {messages.length === 0 && (
                        <div className="h-100 vstack justify-content-center">
                            <ChatAiAgentPersistenceSection />
                            <hr />
                            <AiAgentParametersField
                                control={control}
                                name="parameters"
                                value={formValues.parameters}
                                isTest={false}
                            />
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
                            parametersFromUser={document.data?.Parameters}
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
                <div className="d-flex justify-content-center pb-4">
                    <div className="w-100" style={{ maxWidth: "800px" }}>
                        {isDocumentChanged && !isDocumentDeleted && (
                            <RichAlert variant="warning" className="p-1 mb-2">
                                This document has been updated. To see the latest version in chat,{" "}
                                <Button variant="link" className="p-0 text-warning" onClick={handleRefreshDocument}>
                                    click here to refresh
                                </Button>
                                .
                            </RichAlert>
                        )}
                        {isDocumentDeleted && (
                            <RichAlert variant="warning" className="p-1 mb-2">
                                This document has been deleted,{" "}
                                <a
                                    href={appUrl.forDocuments("@conversations", databaseName)}
                                    className="text-warning text-decoration-none"
                                >
                                    click here to see the recent conversations
                                </a>
                                .
                            </RichAlert>
                        )}
                        <div className="position-relative gradient-top">
                            <FormInput
                                type="textarea"
                                as="textarea"
                                control={control}
                                name={`prompts.${activePromptIndex}.text`}
                                placeholder="Ask the agent anything"
                                className="rounded-2"
                                rows={4}
                                onKeyDown={(e) => {
                                    if (e.key === "Enter" && !e.shiftKey) {
                                        e.preventDefault();
                                        handleSubmit(handleSend)();
                                    }
                                }}
                                disabled={isPromptDisabled}
                                style={{ zIndex: 5 }}
                                key={promptsFieldsArray.fields[activePromptIndex].id}
                                isHideErrorMessage
                            />
                            <ChatAiAgentPromptActions
                                promptsFieldsArray={promptsFieldsArray}
                                isPromptDisabled={isPromptDisabled}
                                isLoading={isLoading}
                            />
                        </div>
                    </div>
                </div>
            )}
        </>
    );
}

const promptHeightInPx = 150;

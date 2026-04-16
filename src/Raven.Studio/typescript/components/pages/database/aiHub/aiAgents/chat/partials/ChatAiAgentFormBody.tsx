import classNames from "classnames";
import AceEditor from "components/common/ace/AceEditor";
import { FormInput } from "components/common/Form";
import { chatAiAgentAttachmentsUtils } from "components/pages/database/aiHub/aiAgents/chat/utils/chatAiAgentAttachmentsUtils";
import { useAppDispatch, useAppSelector } from "components/store";
import { ClipboardEvent, useRef, useEffect } from "react";
import Spinner from "react-bootstrap/Spinner";
import { useFormContext, useWatch, useFieldArray } from "react-hook-form";
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
import ChatAiAgentPromptAttachments from "components/pages/database/aiHub/aiAgents/chat/partials/ChatAiAgentPromptAttachments";
import ChatAiAgentAttachmentsDropzone from "components/pages/database/aiHub/aiAgents/chat/partials/ChatAiAgentAttachmentsDropzone";

interface ChatAiAgentFormBodyProps {
    height: number;
    handleSend: () => Promise<void>;
    runChat: (toolCallParameters?: AiAgentToolCall[]) => Promise<void>;
    isHistory: boolean;
}

export default function ChatAiAgentFormBody({ height, handleSend, runChat, isHistory }: ChatAiAgentFormBodyProps) {
    const dispatch = useAppDispatch();

    const messagesPanelRef = useRef<HTMLDivElement>(null);

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const conversationId = useAppSelector(chatAiAgentSelectors.conversationId);
    const messages = useAppSelector(chatAiAgentSelectors.messages);
    const config = useAppSelector(chatAiAgentSelectors.config);
    const isRawData = useAppSelector(chatAiAgentSelectors.isRawData);
    const document = useAppSelector(chatAiAgentSelectors.document);
    const isLoading = useAppSelector(chatAiAgentSelectors.isLoading);
    const isActionToolSubmitRequired = useAppSelector(chatAiAgentSelectors.isActionToolSubmitRequired);
    const isDocumentDeleted = useAppSelector(chatAiAgentSelectors.isDocumentDeleted);
    const isDocumentChanged = useAppSelector(chatAiAgentSelectors.isDocumentChanged);
    const activePromptIndex = useAppSelector(chatAiAgentSelectors.activePromptIndex);

    const { appUrl } = useAppUrls();

    const { control, handleSubmit, formState } = useFormContext<ChatAiAgentFormData>();

    const formParameters = useWatch({
        control,
        name: "parameters",
    });

    const promptsFieldsArray = useFieldArray({
        control,
        name: "prompts",
    });

    const attachmentsFieldsArray = useFieldArray({
        control,
        name: "attachments",
    });

    // Scroll to the bottom of the test panel when new messages are added
    useEffect(() => {
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

    const isPromptDisabled =
        isLoading || isActionToolSubmitRequired || isDocumentDeleted || isDocumentChanged || config.data?.Disabled;
    const hasPromptErrors = !!formState.errors.prompts;

    const handlePromptPaste = (event: ClipboardEvent<HTMLTextAreaElement>) => {
        const files = chatAiAgentAttachmentsUtils.getLocalFilesFromClipboardData(event.clipboardData);
        if (!files.length) {
            return;
        }

        event.preventDefault();

        const { attachments, invalidFiles } = chatAiAgentAttachmentsUtils.prepareConversationLocalFiles(
            files,
            attachmentsFieldsArray.fields.map((x) => x.name),
            document.data?.["@metadata"]?.["@attachments"]?.map((attachment) => attachment.Name) ?? []
        );

        if (attachments.length) {
            attachmentsFieldsArray.append(attachments);
        }

        chatAiAgentAttachmentsUtils.reportValidationErrors(invalidFiles);
    };

    return (
        <>
            <div
                ref={messagesPanelRef}
                className="ai-agents overflow-auto flex-grow-1 position-relative d-flex justify-content-center chat-ai-agent-messages-panel"
                style={{ height: height - promptHeightInPx }}
            >
                <div className="w-100" style={{ maxWidth: "800px" }}>
                    {messages.length === 0 && (
                        <div className="h-100 vstack justify-content-center">
                            <ChatAiAgentPersistenceSection />
                            <hr />
                            <AiAgentParametersField
                                control={control}
                                value={formParameters}
                                panelClassName="panel-bg-1"
                            />
                        </div>
                    )}
                    {!isRawData && messages.length > 0 && (
                        <AiAgentMessages
                            mode="chat"
                            messages={messages}
                            handleSaveParameters={runChat}
                            parametersFromUser={document.data?.Parameters}
                            documentId={conversationId}
                            openActionCalls={document.data?.OpenActionCalls}
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
                <div className="d-flex justify-content-center pb-4 chat-ai-agent-bottom-section">
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
                        <ChatAiAgentAttachmentsDropzone attachmentsFieldsArray={attachmentsFieldsArray} />
                        <div
                            className={classNames("prompt-wrapper", {
                                "border-danger": hasPromptErrors,
                            })}
                        >
                            <ChatAiAgentPromptAttachments attachmentsFieldsArray={attachmentsFieldsArray} />
                            <FormInput
                                type="textarea"
                                as="textarea"
                                control={control}
                                name={`prompts.${activePromptIndex}.text`}
                                placeholder="Ask the agent anything"
                                className="prompt-textarea"
                                onKeyDown={(e) => {
                                    if (e.key === "Enter" && !e.shiftKey) {
                                        e.preventDefault();
                                        handleSubmit(handleSend)();
                                    }
                                }}
                                onPaste={handlePromptPaste}
                                disabled={isPromptDisabled}
                                key={promptsFieldsArray.fields[activePromptIndex].id}
                            />
                            <ChatAiAgentPromptActions
                                promptsFieldsArray={promptsFieldsArray}
                                isPromptDisabled={isPromptDisabled}
                                isLoading={isLoading}
                                hasPromptErrors={hasPromptErrors}
                                attachmentsFieldsArray={attachmentsFieldsArray}
                            />
                        </div>
                    </div>
                </div>
            )}
        </>
    );
}

const promptHeightInPx = 150;

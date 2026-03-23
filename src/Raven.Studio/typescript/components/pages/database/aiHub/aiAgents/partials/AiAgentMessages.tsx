import "./AiAgentMessages.scss";
import AceEditor from "components/common/ace/AceEditor";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import { useEffect, useRef } from "react";
import ReactAce from "react-ace";
import { Control, SubmitHandler, useFieldArray, useForm } from "react-hook-form";
import { FormAceEditor, FormGroup, FormLabel } from "components/common/Form";
import Button from "react-bootstrap/Button";
import { AiAgentMessage, AiAgentToolCall } from "../utils/aiAgentsTypes";
import Badge from "react-bootstrap/Badge";
import genUtils from "common/generalUtils";
import AiTokensUsagePopoverBody from "components/common/AiTokensUsagePopoverBody";
import { aceEditorUtils } from "components/common/ace/aceEditorUtils";
import { AiAgentSubmittedActionTool } from "components/pages/database/aiHub/aiAgents/partials/aiAgentMessages/AiAgentSubmittedActionTool";
import { AiAgentToolTranscript } from "components/pages/database/aiHub/aiAgents/partials/aiAgentMessages/AiAgentToolTranscript";
import { AiAgentMessagesAttachments } from "components/pages/database/aiHub/aiAgents/partials/aiAgentMessages/AiAgentMessagesAttachments";

interface AiAgentMessagesProps {
    messages: AiAgentMessage[];
    handleSaveParameters: (toolCallParameters: AiAgentToolCall[]) => void;
    setIsWaitingForActionToolSubmit: (isWaiting: boolean) => void;
    parametersFromUser?: Record<string, string>;
    documentId?: string;
}

export default function AiAgentMessages({
    messages,
    handleSaveParameters,
    setIsWaitingForActionToolSubmit,
    parametersFromUser,
    documentId,
}: AiAgentMessagesProps) {
    return (
        <div className="w-100 vstack gap-2 ai-agent-messages pb-4">
            {messages.map((message) => (
                <AiAgentMessageComponent
                    key={message.id}
                    message={message}
                    allMessages={messages}
                    handleSaveParameters={handleSaveParameters}
                    setIsWaitingForActionToolSubmit={setIsWaitingForActionToolSubmit}
                    parametersFromUser={parametersFromUser}
                    documentId={documentId}
                />
            ))}
        </div>
    );
}

interface AiAgentMessageProps {
    message: AiAgentMessage;
    allMessages: AiAgentMessage[];
    handleSaveParameters: (toolCallParameters: AiAgentToolCall[]) => void;
    setIsWaitingForActionToolSubmit: (isWaiting: boolean) => void;
    parametersFromUser?: Record<string, string>;
    documentId: string;
}

function AiAgentMessageComponent({
    message,
    allMessages,
    handleSaveParameters,
    setIsWaitingForActionToolSubmit,
    parametersFromUser,
    documentId,
}: AiAgentMessageProps) {
    return (
        <div>
            {message.role === "system" && <SystemMessage message={message} />}
            {message.role === "user" && <UserMessage message={message} documentId={documentId} />}
            {message.role === "assistant" && (
                <AgentMessage
                    agentMessage={message}
                    allMessages={allMessages}
                    handleSaveParameters={handleSaveParameters}
                    setIsWaitingForActionToolSubmit={setIsWaitingForActionToolSubmit}
                    parametersFromUser={parametersFromUser}
                />
            )}
            {message.role === "submitted-action-tool" && (
                <AiAgentSubmittedActionTool content={message.content} toolName={message.toolName} />
            )}
        </div>
    );
}

interface SystemMessageProps {
    message: AiAgentMessage;
}

function SystemMessage({ message }: SystemMessageProps) {
    return (
        <div className="text-muted">
            <div className="text-center md-label">{message.date}</div>
            <div className="mt-2 p-2 border-start border-secondary d-flex vstack">
                <small>
                    <Icon icon="system" size="xs" />
                    System prompt
                </small>
                <small className="mt-2 overflow-auto" style={{ maxHeight: "200px", whiteSpace: "pre-wrap" }}>
                    {message.content}
                </small>
            </div>
        </div>
    );
}

interface UserMessageProps {
    message: AiAgentMessage;
    documentId: string;
}

function UserMessage({ message, documentId }: UserMessageProps) {
    const getMessageContent = (): string | { type: "text"; text: string }[] => {
        try {
            return JSON.parse(message.content);
        } catch {
            return message.content;
        }
    };

    const messageContent = getMessageContent();
    const isContentString = typeof messageContent === "string";
    const isContentArray = Array.isArray(messageContent);

    return (
        <div className="pt-3">
            <div className="md-label text-center">{message.date}</div>
            <div className="hstack justify-content-end user-message">
                <div
                    className="text-emphasis text-end bg-faded-primary p-2 border-radius-xs border border-primary"
                    style={{ maxWidth: "75%" }}
                >
                    <div className="overflow-auto" style={{ maxHeight: "200px", whiteSpace: "pre-wrap" }}>
                        <AiAgentMessagesAttachments attachments={message.attachments} documentId={documentId} />
                        {isContentString && messageContent}
                        {isContentArray && (
                            <div className="vstack gap-2 align-items-start">
                                {messageContent.map((x, idx) => (
                                    <div key={idx} className="vstack gap-1 align-items-start">
                                        <Badge bg="primary" pill style={{ fontSize: "12px" }}>
                                            Prompt #{idx + 1}
                                        </Badge>
                                        <div className="text-start">{x.text}</div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}

interface AgentMessageProps {
    agentMessage: AiAgentMessage;
    allMessages: AiAgentMessage[];
    handleSaveParameters?: (parameters: AiAgentToolCall[]) => void;
    setIsWaitingForActionToolSubmit: (isWaiting: boolean) => void;
    parametersFromUser?: Record<string, string>;
}

function AgentMessage({
    agentMessage,
    allMessages,
    handleSaveParameters,
    setIsWaitingForActionToolSubmit,
    parametersFromUser,
}: AgentMessageProps) {
    const aceRef = useRef<ReactAce>(null);

    const { control, handleSubmit, formState } = useForm<{ parameters: AiAgentToolCall[] }>({
        defaultValues: {
            parameters:
                agentMessage.toolCalls?.map((x) => ({
                    id: x.id,
                    name: x.name,
                    arguments: "",
                })) ?? [],
        },
    });

    const parametersFieldsArray = useFieldArray({
        control,
        name: "parameters",
    });

    const handleSave: SubmitHandler<{ parameters: AiAgentToolCall[] }> = (formData) => {
        handleSaveParameters?.(formData.parameters);
    };

    const agentMessageIndex = allMessages.findIndex((x) => x.id === agentMessage.id);
    const isLastItem = agentMessageIndex === allMessages.length - 1;
    const hasActionTool = agentMessage.toolCalls?.some((x) => x.type === "action");

    const isRequireParameters =
        isLastItem && hasActionTool && agentMessage.toolCalls?.length > 0 && !formState.isSubmitted;

    useEffect(() => {
        setIsWaitingForActionToolSubmit(isRequireParameters);
    }, [isRequireParameters]);

    const contentMode = aceEditorUtils.getAceEditorMode(agentMessage.content);

    return (
        <div>
            <div className="hstack justify-content-between mb-1">
                <div className="hstack gap-1">
                    <strong>
                        <Icon icon="sparkles" />
                        AI Agent
                    </strong>
                    <small className="text-muted">{agentMessage.date}</small>
                </div>
                {agentMessage.usage && (
                    <small className="text-muted">
                        <PopoverWithHoverWrapper
                            message={
                                <AiTokensUsagePopoverBody
                                    prompt={agentMessage.usage.PromptTokens}
                                    completion={agentMessage.usage.CompletionTokens}
                                    cached={agentMessage.usage.CachedTokens}
                                    reasoning={agentMessage.usage.ReasoningTokens}
                                    total={agentMessage.usage.TotalTokens}
                                />
                            }
                            placement="left"
                        >
                            <Icon icon="info" />
                        </PopoverWithHoverWrapper>
                        Tokens used: {genUtils.formatAiTokens(agentMessage.usage.TotalTokens)}
                    </small>
                )}
            </div>
            {agentMessage.state === "success" && (
                <div>
                    {agentMessage.content && (
                        <div className="mt-2">
                            <AceEditor
                                aceRef={aceRef}
                                defaultValue={agentMessage.content}
                                readOnly
                                mode={contentMode}
                                actions={[
                                    { component: <AceEditor.FullScreenAction /> },
                                    { component: <AceEditor.FormatAction /> },
                                    { component: <AceEditor.ToggleNewLinesAction /> },
                                ]}
                                height={aceEditorUtils.getAceEditorHeight(agentMessage.content)}
                                wrapEnabled={contentMode === "text" ? true : false}
                                setOptions={{
                                    indentedSoftWrap: contentMode === "text" ? true : false,
                                }}
                            />
                        </div>
                    )}
                    {agentMessage.toolCalls?.length > 0 && (
                        <div className="vstack gap-2">
                            {agentMessage.toolCalls.map((toolCall) => (
                                <AiAgentToolTranscript
                                    key={toolCall.id}
                                    toolCall={toolCall}
                                    parametersFromUser={parametersFromUser}
                                />
                            ))}
                        </div>
                    )}
                </div>
            )}
            {isRequireParameters && (
                <div className="hstack justify-content-end mt-2">
                    <div className="text-end bg-faded-primary p-2 border-radius-xs border border-primary text-reset w-100">
                        {parametersFieldsArray.fields.map((field, idx) => (
                            <ActionToolParameterField key={field.id} idx={idx} name={field.name} control={control} />
                        ))}
                        <Button variant="primary" className="rounded-pill" onClick={handleSubmit(handleSave)}>
                            <Icon icon="check" />
                            Submit
                        </Button>
                    </div>
                </div>
            )}
        </div>
    );
}

interface ActionToolParameterFieldProps {
    idx: number;
    name: string;
    control: Control<{ parameters: AiAgentToolCall[] }>;
}

function ActionToolParameterField({ idx, name, control }: ActionToolParameterFieldProps) {
    const aceRef = useRef<ReactAce>(null);

    return (
        <FormGroup>
            <FormLabel>
                Enter a response after completing action for <strong>{name}</strong>
            </FormLabel>
            <FormAceEditor
                aceRef={aceRef}
                control={control}
                name={`parameters.${idx}.arguments`}
                mode="text"
                height="150px"
                actions={[{ component: <AceEditor.FullScreenAction /> }, { component: <AceEditor.FormatAction /> }]}
                placeholder={idx === 0 ? actionToolParameterFieldPlaceholder : ""}
            />
        </FormGroup>
    );
}

const actionToolParameterFieldPlaceholder = `Provide a free-text response to the LLM after completing the requested action, e.g.:
The issue has been forwarded to the support team.`;

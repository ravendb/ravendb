import "./AiAgentMessages.scss";
import AceEditor from "components/common/ace/AceEditor";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import { useRef } from "react";
import ReactAce from "react-ace";
import { Control, SubmitHandler, useFieldArray, useForm } from "react-hook-form";
import { FormAceEditor, FormGroup, FormLabel } from "components/common/Form";
import { AiAgentMessage, AiAgentToolCall } from "../utils/aiAgentsTypes";
import Badge from "react-bootstrap/Badge";
import genUtils from "common/generalUtils";
import AiTokensUsagePopoverBody from "components/common/AiTokensUsagePopoverBody";
import { aceEditorUtils } from "components/common/ace/aceEditorUtils";
import { AiAgentSubmittedActionTool } from "components/pages/database/aiHub/aiAgents/partials/aiAgentMessages/AiAgentSubmittedActionTool";
import { AiAgentToolTranscript } from "components/pages/database/aiHub/aiAgents/partials/aiAgentMessages/AiAgentToolTranscript";
import { AiAgentMessagesAttachments } from "components/pages/database/aiHub/aiAgents/partials/aiAgentMessages/AiAgentMessagesAttachments";
import {
    AiAgentMessagesContextValue,
    AiAgentMessagesProvider,
    useAiAgentMessagesContext,
} from "components/pages/database/aiHub/aiAgents/partials/aiAgentMessages/AiAgentMessagesContext";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import InnerForm from "components/common/InnerForm";
import { tryHandleSubmit } from "components/utils/common";
import AiAgentSummary from "components/pages/database/aiHub/aiAgents/partials/aiAgentMessages/AiAgentSummary";

type AiAgentMessagesProps = AiAgentMessagesContextValue & {
    messages: AiAgentMessage[];
};

export default function AiAgentMessages(props: AiAgentMessagesProps) {
    const { messages, ...contextValue } = props;

    // Used as key for OpenActionCalls component to re-render it when openActionCalls change
    const openActionCallsIds = Object.keys(props.openActionCalls ?? {});

    return (
        <AiAgentMessagesProvider value={contextValue}>
            <div className="w-100 vstack gap-2 ai-agent-messages pb-4">
                {messages.map((message) => (
                    <AiAgentMessageComponent key={message.id} message={message} />
                ))}
                {openActionCallsIds.length > 0 && <OpenActionCalls key={openActionCallsIds.join(";")} />}
            </div>
        </AiAgentMessagesProvider>
    );
}

interface AiAgentMessageProps {
    message: AiAgentMessage;
}

function AiAgentMessageComponent({ message }: AiAgentMessageProps) {
    return (
        <div>
            {message.role === "system" && <SystemMessage message={message} />}
            {message.role === "user" && <UserMessage message={message} />}
            {message.role === "assistant-summary" && <AiAgentSummary agentMessage={message} />}
            {message.role === "assistant" && <AgentMessage agentMessage={message} />}
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
}

function UserMessage({ message }: UserMessageProps) {
    const { documentId } = useAiAgentMessagesContext();

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
}

function AgentMessage({ agentMessage }: AgentMessageProps) {
    const aceRef = useRef<ReactAce>(null);
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
                                    { component: <AceEditor.AutoResizeHeightAction /> },
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
                                <AiAgentToolTranscript key={toolCall.id} toolCall={toolCall} />
                            ))}
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}

function OpenActionCalls() {
    const { openActionCalls, handleSaveParameters } = useAiAgentMessagesContext();

    const openActionCallsValues = Object.values(openActionCalls ?? {});

    const { control, handleSubmit, formState } = useForm<{ parameters: AiAgentToolCall[] }>({
        defaultValues: {
            parameters: openActionCallsValues.map((x) => ({
                id: x.ToolId,
                name: x.Name,
                arguments: "",
            })),
        },
    });

    const parametersFieldsArray = useFieldArray({
        control,
        name: "parameters",
    });

    const handleSave: SubmitHandler<{ parameters: AiAgentToolCall[] }> = async (formData) => {
        return tryHandleSubmit(async () => {
            await handleSaveParameters(formData.parameters);
        });
    };

    return (
        <InnerForm onSubmit={handleSubmit(handleSave)}>
            <div className="hstack justify-content-end mt-2">
                <div className="text-end bg-faded-primary p-2 border-radius-xs border border-primary text-reset w-100">
                    {parametersFieldsArray.fields.map((field, idx) => (
                        <ActionToolParameterField key={field.id} idx={idx} name={field.name} control={control} />
                    ))}
                    <ButtonWithSpinner
                        variant="primary"
                        className="rounded-pill ms-auto"
                        onClick={handleSubmit(handleSave)}
                        isSpinning={formState.isSubmitting}
                        icon="check"
                    >
                        Submit
                    </ButtonWithSpinner>
                </div>
            </div>
        </InnerForm>
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

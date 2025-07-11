import "./AiAgentMessages.scss";
import AceEditor from "components/common/ace/AceEditor";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import { Fragment, useEffect, useRef } from "react";
import ReactAce from "react-ace";
import Spinner from "react-bootstrap/Spinner";
import useUniqueId from "components/hooks/useUniqueId";
import Accordion from "react-bootstrap/Accordion";
import IconName from "typings/server/icons";
import { SubmitHandler, useFieldArray, useForm } from "react-hook-form";
import { FormAceEditor, FormGroup, FormLabel } from "components/common/Form";
import Button from "react-bootstrap/Button";
import { AiAgentMessage, AiAgentToolCall } from "../utils/aiAgentsTypes";

type ToolQuery = Raven.Client.Documents.Operations.AI.Agents.AiAgentToolQuery;
type ToolAction = Raven.Client.Documents.Operations.AI.Agents.AiAgentToolAction;

interface AiAgentMessagesProps {
    messages: AiAgentMessage[];
    toolQueries: ToolQuery[];
    toolActions: ToolAction[];
    handleSaveParameters: (parameters: AiAgentToolCall[]) => void;
}

export default function AiAgentMessages({
    messages,
    toolQueries,
    toolActions,
    handleSaveParameters,
}: AiAgentMessagesProps) {
    return (
        <div className="w-100 vstack gap-2 ai-agent-messages">
            {messages.map((message, idx) => (
                <Fragment key={message.id}>
                    {message.role === "user" && (
                        <UserMessage message={message} idx={idx} toolQueries={toolQueries} toolActions={toolActions} />
                    )}
                    {message.role === "assistant" && (
                        <AgentMessage
                            agentMessage={message}
                            allMessages={messages}
                            toolQueries={toolQueries}
                            toolActions={toolActions}
                            handleSaveParameters={handleSaveParameters}
                        />
                    )}
                </Fragment>
            ))}
        </div>
    );
}

interface UserMessageProps {
    message: AiAgentMessage;
    idx: number;
    toolQueries: ToolQuery[];
    toolActions: ToolAction[];
}

function UserMessage({ message, idx, toolQueries, toolActions }: UserMessageProps) {
    return (
        <div>
            {idx === 0 && <div className="text-muted text-center">{message.date}</div>}
            <div className="hstack justify-content-end user-message">
                <div
                    className="text-end bg-faded-primary p-2 rounded-3 border border-primary text-reset"
                    style={{ maxWidth: "75%" }}
                >
                    <div>{message.content}</div>
                    {message.toolCalls?.length > 0 && (
                        <div className="vstack gap-2">
                            {message.toolCalls.map((toolCall) => (
                                <TranscriptTool
                                    key={toolCall.id}
                                    toolCall={toolCall}
                                    toolQueries={toolQueries}
                                    toolActions={toolActions}
                                />
                            ))}
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}

interface AgentMessageProps {
    agentMessage: AiAgentMessage;
    allMessages: AiAgentMessage[];
    toolQueries: ToolQuery[];
    toolActions: ToolAction[];
    handleSaveParameters?: (parameters: AiAgentToolCall[]) => void;
}

function AgentMessage({
    agentMessage,
    allMessages,
    toolQueries,
    toolActions,
    handleSaveParameters,
}: AgentMessageProps) {
    const aceRef = useRef<ReactAce>(null);

    const { control, handleSubmit, reset, formState } = useForm<{ parameters: AiAgentToolCall[] }>({
        defaultValues: {
            parameters:
                agentMessage.toolCalls?.map((x) => ({
                    id: x.id,
                    name: x.name,
                    arguments: x.arguments,
                })) ?? [],
        },
    });

    const parametersFieldsArray = useFieldArray({
        control,
        name: "parameters",
    });

    // TODO: this is a workaround to reset the form when the tool calls change
    useEffect(() => {
        reset({
            parameters:
                agentMessage.toolCalls?.map((x) => ({
                    id: x.id,
                    name: x.name,
                    arguments: x.arguments,
                })) ?? [],
        });
    }, [agentMessage.toolCalls?.length]);

    const handleSave: SubmitHandler<{ parameters: AiAgentToolCall[] }> = (formData) => {
        handleSaveParameters?.(formData.parameters);
    };

    const agentMessageIndex = allMessages.findIndex((x) => x.id === agentMessage.id);
    const transcript = agentMessage.transcript ?? [];

    const isLastItem = agentMessageIndex === allMessages.length - 1;
    const isRequireParameters = isLastItem && agentMessage.toolCalls?.length > 0 && !formState.isSubmitted;

    return (
        <div>
            <div className="hstack justify-content-between mb-2">
                <div className="hstack gap-2">
                    <div className="agent-icon-wrapper">
                        <Icon icon="sparkles" margin="m-0" />
                    </div>
                    <strong>AI Agent</strong>
                    <div className="text-muted">{agentMessage.date}</div>
                </div>
                {agentMessage.usage && (
                    <div className="hstack text-muted">
                        <PopoverWithHoverWrapper
                            message={
                                <div>
                                    <div className="hstack justify-content-between gap-3">
                                        <span>Prompt tokens</span>
                                        <span>{agentMessage.usage.PromptTokens}</span>
                                    </div>
                                    <div className="hstack justify-content-between gap-3">
                                        <span>Completion tokens</span>
                                        <span>{agentMessage.usage.CompletionTokens}</span>
                                    </div>
                                    <div className="hstack justify-content-between gap-3">
                                        <span>Cached tokens</span>
                                        <span>{agentMessage.usage.CachedTokens}</span>
                                    </div>
                                    <hr className="my-1" />
                                    <div className="hstack justify-content-between gap-3">
                                        <span>Tokens usage</span>
                                        <span>{agentMessage.usage.TotalTokens}</span>
                                    </div>
                                </div>
                            }
                        >
                            <Icon icon="info" />
                        </PopoverWithHoverWrapper>
                        Tokens usage: {agentMessage.usage.TotalTokens}
                    </div>
                )}
            </div>
            {agentMessage.state === "loading" && (
                <div className="hstack">
                    <Spinner size="sm" className="me-1" />
                    <span>Thinking...</span>
                </div>
            )}
            {agentMessage.state === "error" && <div className="text-danger">Error</div>}
            {agentMessage.state === "success" && (
                <div>
                    <Transcript transcript={transcript} toolQueries={toolQueries} toolActions={toolActions} />
                    {agentMessage.content && (
                        <div className="mt-2">
                            <AceEditor
                                aceRef={aceRef}
                                value={agentMessage.content}
                                readOnly
                                mode="json"
                                actions={[{ component: <AceEditor.FullScreenAction /> }]}
                                height={getAgentAceEditorHeight(agentMessage.content)}
                            />
                        </div>
                    )}
                </div>
            )}
            {isRequireParameters && (
                <div className="hstack justify-content-end mt-2">
                    <div
                        className="text-end bg-faded-primary p-2 rounded-3 border border-primary text-reset"
                        style={{ maxWidth: "75%" }}
                    >
                        {parametersFieldsArray.fields.map((field, idx) => (
                            <FormGroup key={field.id}>
                                <FormLabel>
                                    Define parameters for <strong>{field.name}</strong> tool call
                                </FormLabel>
                                <FormAceEditor
                                    control={control}
                                    name={`parameters.${idx}.arguments`}
                                    mode="json"
                                    height="100px"
                                />
                            </FormGroup>
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

interface TranscriptProps {
    transcript: AiAgentMessage[];
    toolQueries: ToolQuery[];
    toolActions: ToolAction[];
}

function Transcript({ transcript, toolQueries, toolActions }: TranscriptProps) {
    const id = useUniqueId("transcript");

    const getTitle = (message: AiAgentMessage) => {
        if (message.role === "system") {
            return "System Role was set.";
        }
        if (message.role === "user") {
            return "User Role input.";
        }
        if (message.role === "assistant") {
            return "Assistant Role response.";
        }
        if (message.role === "tool") {
            return "Tool Role response.";
        }

        return message.role;
    };

    return (
        <Accordion className="transcript border border-secondary rounded-2 panel-bg-2">
            <Accordion.Item eventKey={id} className="panel-bg-2">
                <Accordion.Header>Transcript</Accordion.Header>
                <Accordion.Body className="panel-bg-2 rounded-2">
                    <div className="vstack gap-2">
                        {transcript.map((message) => (
                            <div key={message.id}>
                                <div>{getTitle(message)}</div>
                                {message.content && (
                                    <div className="border border-secondary rounded-2 p-2 well mt-1">
                                        {message.content}
                                    </div>
                                )}
                                {message.toolCalls?.length > 0 && (
                                    <div className="vstack gap-2">
                                        {message.toolCalls.map((toolCall) => (
                                            <TranscriptTool
                                                key={toolCall.id}
                                                toolCall={toolCall}
                                                toolQueries={toolQueries}
                                                toolActions={toolActions}
                                            />
                                        ))}
                                    </div>
                                )}
                            </div>
                        ))}
                    </div>
                </Accordion.Body>
            </Accordion.Item>
        </Accordion>
    );
}

interface TranscriptToolProps {
    toolCall: AiAgentToolCall;
    toolQueries: ToolQuery[];
    toolActions: ToolAction[];
}

function TranscriptTool({ toolCall, toolQueries, toolActions }: TranscriptToolProps) {
    const id = useUniqueId("tool-call");

    const toolQuery = toolQueries?.find((x) => x.Name === toolCall.name);
    const toolAction = toolActions?.find((x) => x.Name === toolCall.name);

    const icon: IconName = toolQuery ? "query" : "force";

    return (
        <Accordion className="transcript-tool border border-secondary rounded-2 panel-bg-3">
            <Accordion.Item eventKey={id} className="panel-bg-3">
                <Accordion.Header>
                    <div className="hstack gap-2">
                        <div className="p-1 rounded-2 bg-faded-primary border border-primary">
                            <Icon icon={icon} color="primary" margin="m-0" />
                        </div>
                        <div className="text-truncate">Tool call: {toolCall.name}</div>
                    </div>
                </Accordion.Header>
                <Accordion.Body className="panel-bg-3 rounded-2">
                    <TranscriptToolBody tool={toolQuery ?? toolAction} toolCall={toolCall} />
                </Accordion.Body>
            </Accordion.Item>
        </Accordion>
    );
}

interface TranscriptToolBodyProps {
    tool: ToolQuery | ToolAction;
    toolCall: AiAgentToolCall;
}

function TranscriptToolBody({ tool, toolCall }: TranscriptToolBodyProps) {
    return (
        <div>
            <small className="text-muted">Description</small>
            <div>{tool.Description}</div>
            <hr className="my-1" />
            {tool.ParametersSampleObject && (
                <div>
                    <small className="text-muted">Parameters</small>
                    <AceEditor value={tool.ParametersSampleObject} readOnly mode="json" height="100px" />
                </div>
            )}
            {tool.ParametersSchema && (
                <div>
                    <small className="text-muted">Parameters schema</small>
                    <AceEditor value={tool.ParametersSchema} readOnly mode="json" height="100px" />
                </div>
            )}
            {"Query" in tool && tool.Query && (
                <div>
                    <small className="text-muted">Query</small>
                    <AceEditor value={tool.Query} readOnly mode="text" height="100px" />
                </div>
            )}
            {toolCall.arguments && (
                <div>
                    <small className="text-muted">Arguments</small>
                    <AceEditor value={toolCall.arguments} readOnly mode="text" height="100px" />
                </div>
            )}
        </div>
    );
}

function getAgentAceEditorHeight(content: string): `${number}px` {
    if (!content) {
        return "100px";
    }

    const lineHeight = 26;
    const lineCount = content.split("\n").length;

    if (lineCount <= 12) {
        return `${lineCount * lineHeight}px`;
    }

    return "320px";
}

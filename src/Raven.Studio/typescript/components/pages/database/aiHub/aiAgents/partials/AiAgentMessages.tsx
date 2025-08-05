import "./AiAgentMessages.scss";
import AceEditor from "components/common/ace/AceEditor";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import { useEffect, useMemo, useRef } from "react";
import ReactAce from "react-ace";
import useUniqueId from "components/hooks/useUniqueId";
import Accordion from "react-bootstrap/Accordion";
import IconName from "typings/server/icons";
import { Control, SubmitHandler, useFieldArray, useForm } from "react-hook-form";
import { FormAceEditor, FormGroup, FormLabel } from "components/common/Form";
import Button from "react-bootstrap/Button";
import { AiAgentMessage, AiAgentToolCall } from "../utils/aiAgentsTypes";
import { useDocumentColumnsProvider } from "components/common/virtualTable/columnProviders/useDocumentColumnsProvider";
import { getCoreRowModel, getFilteredRowModel, getSortedRowModel, useReactTable } from "@tanstack/react-table";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import document from "models/database/documents/document";
import Badge from "react-bootstrap/Badge";
import { aiAgentsUtils } from "../utils/aiAgentsUtils";
import useRqlLanguageService from "components/hooks/useRqlLanguageService";
import genUtils from "common/generalUtils";

type ToolQuery = Raven.Client.Documents.Operations.AI.Agents.AiAgentToolQuery;
type ToolAction = Raven.Client.Documents.Operations.AI.Agents.AiAgentToolAction;

interface AiAgentMessagesProps {
    messages: AiAgentMessage[];
    toolQueries: ToolQuery[];
    toolActions: ToolAction[];
    handleSaveParameters: (toolCallParameters: AiAgentToolCall[]) => void;
    setIsWaitingForActionToolSubmit: (isWaiting: boolean) => void;
}

export default function AiAgentMessages({
    messages,
    toolQueries,
    toolActions,
    handleSaveParameters,
    setIsWaitingForActionToolSubmit,
}: AiAgentMessagesProps) {
    return (
        <div className="w-100 vstack gap-2 ai-agent-messages pb-1">
            {messages.map((message) => (
                <AiAgentMessage
                    key={message.id}
                    message={message}
                    allMessages={messages}
                    toolQueries={toolQueries}
                    toolActions={toolActions}
                    handleSaveParameters={handleSaveParameters}
                    setIsWaitingForActionToolSubmit={setIsWaitingForActionToolSubmit}
                />
            ))}
        </div>
    );
}

interface AiAgentMessageProps {
    message: AiAgentMessage;
    allMessages: AiAgentMessage[];
    toolQueries: ToolQuery[];
    toolActions: ToolAction[];
    handleSaveParameters: (toolCallParameters: AiAgentToolCall[]) => void;
    setIsWaitingForActionToolSubmit: (isWaiting: boolean) => void;
}

function AiAgentMessage({
    message,
    allMessages,
    toolQueries,
    toolActions,
    handleSaveParameters,
    setIsWaitingForActionToolSubmit,
}: AiAgentMessageProps) {
    const toolName = allMessages
        .find((x) => x.toolCalls?.some((y) => y.id === message.toolCallId))
        ?.toolCalls.find((x) => x.id === message.toolCallId)?.name;

    const isActionTool = !!(toolName && toolActions.some((x) => x.Name === toolName));

    return (
        <div>
            {message.role === "system" && <SystemMessage message={message} />}
            {isActionTool && <ToolMessage message={message} type="action" />}
            {message.role === "user" && (
                <UserMessage message={message} toolQueries={toolQueries} toolActions={toolActions} />
            )}
            {message.role === "assistant" && (
                <AgentMessage
                    agentMessage={message}
                    allMessages={allMessages}
                    toolQueries={toolQueries}
                    toolActions={toolActions}
                    handleSaveParameters={handleSaveParameters}
                    setIsWaitingForActionToolSubmit={setIsWaitingForActionToolSubmit}
                />
            )}
        </div>
    );
}

interface ToolMessageProps {
    message: AiAgentMessage;
    type: "action" | "query";
}

function ToolMessage({ message, type }: ToolMessageProps) {
    const aceRef = useRef<ReactAce>(null);

    const toolName = message.toolName;

    const isTable = message.content.startsWith("[") && message.content.endsWith("]") && message.content.length > 2;
    const tableData = useMemo(
        () => (isTable ? JSON.parse(message.content).map((x: any) => new document(x)) : []),
        [message.content, isTable]
    );
    const contentMode = getAceEditorMode(message.content);

    const { columnDefs } = useDocumentColumnsProvider({
        documents: tableData,
        availableWidth: window.innerWidth,
        hasCheckbox: false,
        hasPreview: false,
        hasFlags: true,
    });

    const table = useReactTable({
        data: tableData,
        columns: columnDefs,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
    });

    return (
        <div className="bg-faded-primary p-2 rounded-3 border border-primary text-reset w-100">
            {type === "query" && <div>Query tool result</div>}
            {type === "action" && toolName && (
                <div className="hstack justify-content-between mb-1">
                    <div>
                        Response from action tool: <strong>{toolName}</strong>
                    </div>
                    <Badge bg="primary" pill>
                        <Icon icon="check" /> Submitted
                    </Badge>{" "}
                </div>
            )}
            {isTable ? (
                <VirtualTable table={table} heightInPx={300} className="border border-secondary" />
            ) : (
                <AceEditor
                    aceRef={aceRef}
                    value={message.content}
                    readOnly
                    mode={contentMode}
                    height="150px"
                    actions={[{ component: <AceEditor.FullScreenAction /> }, { component: <AceEditor.FormatAction /> }]}
                />
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
            <div className="text-center">{message.date}</div>
            <div className="mt-2 p-2 border-start border-secondary">
                <div>
                    <Icon icon="system" size="xs" />
                    System prompt
                </div>
                <div className="mt-2 overflow-auto" style={{ maxHeight: "200px", whiteSpace: "pre-wrap" }}>
                    {message.content}
                </div>
            </div>
        </div>
    );
}

interface UserMessageProps {
    message: AiAgentMessage;
    toolQueries: ToolQuery[];
    toolActions: ToolAction[];
}

function UserMessage({ message, toolQueries, toolActions }: UserMessageProps) {
    const isMessageWithParameters = message.content.startsWith("AI Agent Parameters:");

    if (isMessageWithParameters) {
        return null;
    }

    return (
        <div>
            <div className="text-muted text-center">{message.date}</div>
            <div className="hstack justify-content-end user-message">
                <div
                    className="text-end bg-faded-primary p-2 rounded-3 border border-primary text-reset"
                    style={{ maxWidth: "75%" }}
                >
                    <div className="overflow-auto" style={{ maxHeight: "200px", whiteSpace: "pre-wrap" }}>
                        {message.content}
                    </div>
                    {message.toolCalls?.length > 0 && (
                        <div className="vstack gap-2">
                            {message.toolCalls.map((toolCall) => (
                                <ToolCall
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
    setIsWaitingForActionToolSubmit: (isWaiting: boolean) => void;
}

function AgentMessage({
    agentMessage,
    allMessages,
    toolQueries,
    toolActions,
    handleSaveParameters,
    setIsWaitingForActionToolSubmit,
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
    const isToolAction = agentMessage.toolCalls?.some((x) => toolActions?.some((y) => y.Name === x.name));

    const isRequireParameters =
        isLastItem && isToolAction && agentMessage.toolCalls?.length > 0 && !formState.isSubmitted;

    useEffect(() => {
        setIsWaitingForActionToolSubmit(isRequireParameters);
    }, [isRequireParameters]);

    const contentMode = getAceEditorMode(agentMessage.content);

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
                                        <span>{genUtils.formatAiTokens(agentMessage.usage.PromptTokens)}</span>
                                    </div>
                                    <div className="hstack justify-content-between gap-3">
                                        <span>Completion tokens</span>
                                        <span>{genUtils.formatAiTokens(agentMessage.usage.CompletionTokens)}</span>
                                    </div>
                                    <div className="hstack justify-content-between gap-3">
                                        <span>Cached tokens</span>
                                        <span>{genUtils.formatAiTokens(agentMessage.usage.CachedTokens)}</span>
                                    </div>
                                    <hr className="my-1" />
                                    <div className="hstack justify-content-between gap-3">
                                        <span>Total tokens used</span>
                                        <span>{genUtils.formatAiTokens(agentMessage.usage.TotalTokens)}</span>
                                    </div>
                                </div>
                            }
                        >
                            <Icon icon="info" />
                        </PopoverWithHoverWrapper>
                        Tokens used: {genUtils.formatAiTokens(agentMessage.usage.TotalTokens)}
                    </div>
                )}
            </div>
            {agentMessage.state === "success" && (
                <div>
                    {agentMessage.content && (
                        <div className="mt-2">
                            <AceEditor
                                aceRef={aceRef}
                                value={agentMessage.content}
                                readOnly
                                mode={contentMode}
                                actions={[
                                    { component: <AceEditor.FullScreenAction /> },
                                    { component: <AceEditor.FormatAction /> },
                                ]}
                                height={getAgentAceEditorHeight(agentMessage.content)}
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
                                <ToolCall
                                    key={toolCall.id}
                                    toolCall={toolCall}
                                    toolQueries={toolQueries}
                                    toolActions={toolActions}
                                />
                            ))}
                        </div>
                    )}
                </div>
            )}
            {isRequireParameters && (
                <div className="hstack justify-content-end mt-2">
                    <div className="text-end bg-faded-primary p-2 rounded-3 border border-primary text-reset w-100">
                        {parametersFieldsArray.fields.map((field, idx) => (
                            <ParameterField key={field.id} idx={idx} name={field.name} control={control} />
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

interface ParameterFieldProps {
    idx: number;
    name: string;
    control: Control<{ parameters: AiAgentToolCall[] }>;
}

function ParameterField({ idx, name, control }: ParameterFieldProps) {
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
                placeholder={idx === 0 ? parameterFieldPlaceholder : ""}
            />
        </FormGroup>
    );
}

const parameterFieldPlaceholder = `Provide a free-text response to the LLM after completing the requested action, e.g.:
The issue has been forwarded to the support team.`;

interface ToolCallProps {
    toolCall: AiAgentToolCall;
    toolQueries: ToolQuery[];
    toolActions: ToolAction[];
}

function ToolCall({ toolCall, toolQueries, toolActions }: ToolCallProps) {
    const id = useUniqueId("tool-call");

    const toolQuery = toolQueries?.find((x) => x.Name === toolCall.name);
    const toolAction = toolActions?.find((x) => x.Name === toolCall.name);

    const icon: IconName = toolQuery ? "query" : "force";
    const label = toolQuery ? "Query tool:" : "Action tool:";

    return (
        <Accordion className="transcript-tool border border-secondary rounded-2 panel-bg-1">
            <Accordion.Item eventKey={id} className="panel-bg-1">
                <Accordion.Header>
                    <div className="hstack gap-2">
                        <div className="p-1 rounded-2 bg-faded-primary border border-primary">
                            <Icon icon={icon} color="primary" margin="m-0" />
                        </div>
                        <div className="text-truncate">
                            {label} {toolCall.name}
                        </div>
                    </div>
                </Accordion.Header>
                <Accordion.Collapse eventKey={id} mountOnEnter unmountOnExit>
                    <Accordion.Body className="panel-bg-1 rounded-2">
                        <ToolCallBody tool={toolQuery ?? toolAction} toolCall={toolCall} />
                    </Accordion.Body>
                </Accordion.Collapse>
            </Accordion.Item>
        </Accordion>
    );
}

interface ToolCallBodyProps {
    tool: ToolQuery | ToolAction;
    toolCall: AiAgentToolCall;
}

function ToolCallBody({ tool, toolCall }: ToolCallBodyProps) {
    const prettifiedArguments = aiAgentsUtils.getPrettifiedContent(toolCall?.arguments);
    const argumentsMode = getAceEditorMode(prettifiedArguments);

    const rqlLanguageService = useRqlLanguageService();

    const id = useUniqueId("tool-call-details");

    return (
        <div className="vstack gap-2">
            {tool && (
                <Accordion className="tool-call-details border border-secondary rounded-2 panel-bg-2">
                    <Accordion.Item eventKey={id} className="panel-bg-2">
                        <Accordion.Header className="p-2">
                            <Icon icon="settings" />
                            See details
                        </Accordion.Header>
                        <Accordion.Collapse eventKey={id} mountOnEnter unmountOnExit>
                            <Accordion.Body className="panel-bg-2 rounded-2 pt-0">
                                {tool.Description && (
                                    <div>
                                        <small className="text-muted">Description</small>
                                        <div>{tool.Description}</div>
                                        <hr className="my-1" />
                                    </div>
                                )}
                                {tool.ParametersSampleObject && (
                                    <div>
                                        <small className="text-muted">Sample parameters object</small>
                                        <AceEditor
                                            value={tool.ParametersSampleObject}
                                            readOnly
                                            mode="json"
                                            height="100px"
                                        />
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
                                        <AceEditor
                                            value={tool.Query}
                                            readOnly
                                            mode="rql"
                                            height="100px"
                                            languageService={rqlLanguageService}
                                        />
                                    </div>
                                )}
                            </Accordion.Body>
                        </Accordion.Collapse>
                    </Accordion.Item>
                </Accordion>
            )}
            <div>
                <small className="text-muted">Parameters filled by LLM</small>
                <AceEditor
                    value={prettifiedArguments}
                    readOnly
                    mode={argumentsMode}
                    height={getAgentAceEditorHeight(prettifiedArguments)}
                />
            </div>
            {toolCall?.queryToolResult && <ToolMessage message={toolCall.queryToolResult} type="query" />}
        </div>
    );
}

function getAgentAceEditorHeight(content: string): `${number}px` {
    if (!content) {
        return "100px";
    }

    const lineHeight = 26;
    const minimumLineCount = 4;
    const lineCount = content.split("\n").length;
    const effectiveLineCount = Math.max(lineCount, minimumLineCount);

    if (effectiveLineCount <= 12) {
        const halfLineHeight = lineHeight / 2; // to show that there is more content
        return `${effectiveLineCount * lineHeight + halfLineHeight}px`;
    }

    return "320px";
}

function getAceEditorMode(content: string): "json" | "text" {
    if (content?.startsWith("{") && content?.endsWith("}")) {
        return "json";
    }

    return "text";
}

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
import queryCriteria from "models/database/query/queryCriteria";
import savedQueriesStorage from "common/storage/savedQueriesStorage";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import AiTokensUsagePopoverBody from "components/common/AiTokensUsagePopoverBody";

type ToolQuery = Raven.Client.Documents.Operations.AI.Agents.AiAgentToolQuery;
type ToolAction = Raven.Client.Documents.Operations.AI.Agents.AiAgentToolAction;

interface AiAgentMessagesProps {
    messages: AiAgentMessage[];
    toolQueries: ToolQuery[];
    toolActions: ToolAction[];
    handleSaveParameters: (toolCallParameters: AiAgentToolCall[]) => void;
    setIsWaitingForActionToolSubmit: (isWaiting: boolean) => void;
    parametersFromUser?: Record<string, string>;
}

export default function AiAgentMessages({
    messages,
    toolQueries,
    toolActions,
    handleSaveParameters,
    setIsWaitingForActionToolSubmit,
    parametersFromUser,
}: AiAgentMessagesProps) {
    return (
        <div className="w-100 vstack gap-2 ai-agent-messages pb-4">
            {messages.map((message) => (
                <AiAgentMessage
                    key={message.id}
                    message={message}
                    allMessages={messages}
                    toolQueries={toolQueries}
                    toolActions={toolActions}
                    handleSaveParameters={handleSaveParameters}
                    setIsWaitingForActionToolSubmit={setIsWaitingForActionToolSubmit}
                    parametersFromUser={parametersFromUser}
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
    parametersFromUser?: Record<string, string>;
}

function AiAgentMessage({
    message,
    allMessages,
    toolQueries,
    toolActions,
    handleSaveParameters,
    setIsWaitingForActionToolSubmit,
    parametersFromUser,
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
                    parametersFromUser={parametersFromUser}
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
        <div className="bg-faded-primary p-2 border-radius-xs border border-primary w-100">
            {type === "query" && <div className="text-emphasis">Query tool result</div>}
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
                    defaultValue={message.content}
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
    toolQueries: ToolQuery[];
    toolActions: ToolAction[];
}

function UserMessage({ message, toolQueries, toolActions }: UserMessageProps) {
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

    const isMessageWithParameters = isContentString && messageContent.startsWith("AI Agent Parameters:");

    if (isMessageWithParameters) {
        return null;
    }

    return (
        <div className="pt-3">
            <div className="md-label text-center">{message.date}</div>
            <div className="hstack justify-content-end user-message">
                <div
                    className="text-emphasis text-end bg-faded-primary p-2 border-radius-xs border border-primary"
                    style={{ maxWidth: "75%" }}
                >
                    <div className="overflow-auto" style={{ maxHeight: "200px", whiteSpace: "pre-wrap" }}>
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
    parametersFromUser?: Record<string, string>;
}

function AgentMessage({
    agentMessage,
    allMessages,
    toolQueries,
    toolActions,
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
    const isToolAction = agentMessage.toolCalls?.some((x) => toolActions?.some((y) => y.Name === x.name));

    const isRequireParameters =
        isLastItem && isToolAction && agentMessage.toolCalls?.length > 0 && !formState.isSubmitted;

    useEffect(() => {
        setIsWaitingForActionToolSubmit(isRequireParameters);
    }, [isRequireParameters]);

    const contentMode = getAceEditorMode(agentMessage.content);

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
    parametersFromUser?: Record<string, string>;
}

function ToolCall({ toolCall, toolQueries, toolActions, parametersFromUser }: ToolCallProps) {
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
                        <div className="p-1 rounded-1 bg-faded-primary border border-primary">
                            <Icon icon={icon} color="primary" margin="m-0" />
                        </div>
                        <div className="text-truncate">
                            {label} {toolCall.name}
                        </div>
                    </div>
                </Accordion.Header>
                <Accordion.Collapse eventKey={id} mountOnEnter unmountOnExit>
                    <Accordion.Body className="panel-bg-1 rounded-2">
                        <ToolCallBody
                            tool={toolQuery ?? toolAction}
                            toolCall={toolCall}
                            parametersFromUser={parametersFromUser}
                        />
                    </Accordion.Body>
                </Accordion.Collapse>
            </Accordion.Item>
        </Accordion>
    );
}

interface ToolCallBodyProps {
    tool: ToolQuery | ToolAction;
    toolCall: AiAgentToolCall;
    parametersFromUser?: Record<string, string>;
}

function ToolCallBody({ tool, toolCall, parametersFromUser }: ToolCallBodyProps) {
    const prettifiedArguments = aiAgentsUtils.getPrettifiedContent(toolCall?.arguments);
    const argumentsMode = getAceEditorMode(prettifiedArguments);

    const id = useUniqueId("tool-call-details");

    return (
        <div className="vstack gap-2">
            {tool && (
                <Accordion className="tool-call-details border border-secondary rounded-2 panel-bg-2">
                    <Accordion.Item eventKey={id} className="panel-bg-2">
                        <Accordion.Header className="p-1">
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
                                {tool.ParametersSchema && (
                                    <div>
                                        <small className="text-muted">Parameters schema</small>
                                        <AceEditor
                                            defaultValue={tool.ParametersSchema}
                                            readOnly
                                            mode="json"
                                            height="100px"
                                        />
                                    </div>
                                )}
                                {"Query" in tool && tool.Query && (
                                    <ToolDetailsQuery
                                        queryText={tool.Query}
                                        parametersFromUser={parametersFromUser}
                                        parametersFromModel={toolCall.arguments}
                                    />
                                )}
                            </Accordion.Body>
                        </Accordion.Collapse>
                    </Accordion.Item>
                </Accordion>
            )}
            <div>
                <small className="text-muted">Parameters filled by LLM</small>
                <AceEditor
                    defaultValue={prettifiedArguments}
                    readOnly
                    mode={argumentsMode}
                    height={getAgentAceEditorHeight(prettifiedArguments)}
                />
            </div>
            {toolCall?.queryToolResult && <ToolMessage message={toolCall.queryToolResult} type="query" />}
        </div>
    );
}

interface Argument {
    key: string;
    value: any;
}

function ToolDetailsQuery({
    queryText,
    parametersFromUser,
    parametersFromModel,
}: {
    queryText: string;
    parametersFromUser?: Record<string, string>;
    parametersFromModel?: string;
}) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const rqlLanguageService = useRqlLanguageService();

    const getLlmParametersForQuery = (matches: string[]): Argument[] => {
        try {
            const parametersObject = JSON.parse(parametersFromModel);
            return matches
                .map((x) => ({ key: x, value: parametersObject[x] }))
                .filter((x) => x.value && !Object.keys(parametersFromUser).includes(x.key));
        } catch {
            return [];
        }
    };

    const getAgentParametersForQuery = (matches: string[]): Argument[] => {
        return matches.map((x) => ({ key: x, value: parametersFromUser?.[x] })).filter((x) => x.value);
    };

    const getArgumentFormattedValue = (value: string): string => {
        if (typeof value === "number") {
            return value;
        }
        return JSON.stringify(value);
    };

    const getQueryWithParameters = (): string => {
        const regexToFind$: RegExp = /\$\w+/g;
        const allMatches = queryText.match(regexToFind$)?.map((x) => x.replace("$", "")) || [];
        const uniqueMatches = [...new Set(allMatches)];

        const llmParametersForQuery = getLlmParametersForQuery(uniqueMatches);
        const agentParametersForQuery = getAgentParametersForQuery(uniqueMatches);

        let resultQuery = "";

        if (llmParametersForQuery.length > 0) {
            resultQuery += `// LLM parameters\n`;
            resultQuery += llmParametersForQuery
                .map((x) => `$${x.key} = ${getArgumentFormattedValue(x.value)}`)
                .join("\n");
            resultQuery += "\n\n";
        }

        if (agentParametersForQuery.length > 0) {
            resultQuery += `// Agent parameters\n`;
            resultQuery += agentParametersForQuery
                .map((x) => `$${x.key} = ${getArgumentFormattedValue(x.value)}`)
                .join("\n");
            resultQuery += "\n\n";
        }

        resultQuery += queryText;

        return resultQuery;
    };

    const queryWithParameters = getQueryWithParameters();

    const linkToQuery = () => {
        const query = queryCriteria.empty();

        query.queryText(queryWithParameters);
        query.recentQuery(true);
        const queryDto = query.toStorageDto();
        savedQueriesStorage.saveAndNavigate(databaseName, queryDto, {
            newWindow: true,
        });
    };

    return (
        <div>
            <div className="d-flex justify-content-between mb-1 align-items-end">
                <small className="text-muted">Query</small>
                <Button
                    variant="info"
                    className="rounded-pill"
                    onClick={linkToQuery}
                    title="Click to test this query in the Studio's Query View"
                    size="sm"
                >
                    <Icon icon="rocket" />
                    Test query
                </Button>
            </div>
            <AceEditor
                defaultValue={queryWithParameters}
                readOnly
                mode="rql"
                height={getAgentAceEditorHeight(queryWithParameters, 200)}
                languageService={rqlLanguageService}
            />
        </div>
    );
}

function getAgentAceEditorHeight(content: string, maxHeightInPx = 320): `${number}px` {
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

    return `${maxHeightInPx}px`;
}

function getAceEditorMode(content: string): "json" | "text" {
    if (content?.startsWith("{") && content?.endsWith("}")) {
        return "json";
    }

    return "text";
}

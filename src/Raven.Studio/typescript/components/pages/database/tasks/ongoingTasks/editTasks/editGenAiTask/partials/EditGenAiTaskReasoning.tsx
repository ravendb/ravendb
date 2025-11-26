import { useReactTable, getCoreRowModel, getSortedRowModel, getFilteredRowModel } from "@tanstack/react-table";
import AceEditor from "components/common/ace/AceEditor";
import { useDocumentColumnsProvider } from "components/common/virtualTable/columnProviders/useDocumentColumnsProvider";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { Icon } from "components/common/Icon";
import { useRef, useMemo, useId } from "react";
import ReactAce from "react-ace";
import Accordion from "react-bootstrap/Accordion";
import AccordionButton from "react-bootstrap/AccordionButton";
import document from "models/database/documents/document";
import { aiAgentsUtils } from "components/pages/database/aiHub/aiAgents/utils/aiAgentsUtils";
import useRqlLanguageService from "components/hooks/useRqlLanguageService";
import Button from "react-bootstrap/Button";
import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../utils/editGenAiTaskValidation";
import useToolQueryDetails from "components/pages/database/aiHub/aiAgents/hooks/useToolQueryDetails";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { DotList } from "components/common/dotList/DotList";
import { aceEditorUtils } from "components/common/ace/aceEditorUtils";

interface Message {
    role: "system" | "user" | "assistant" | "tool";
    content: string;
    tool_call_id?: string;
    tool_calls?: {
        id: string;
        function: {
            name: string;
            arguments: string;
        };
    }[];
    date: string;
}

type ConversationDocument = Omit<
    Raven.Server.Documents.Handlers.AI.Agents.ConversationDocument,
    "Messages" | "Parameters"
> & {
    Messages: Message[];
    Parameters: Record<string, string>;
};

interface EditGenAiTaskReasoningProps {
    conversationDocument: ConversationDocument;
}

export default function EditGenAiTaskReasoning({ conversationDocument }: EditGenAiTaskReasoningProps) {
    if (!conversationDocument) {
        return null;
    }

    const toolMessages = conversationDocument.Messages.filter((message) => message.role === "tool");

    const canShowMessage = (message: Message) => {
        if (message.role !== "system" && message.role !== "user" && message.role !== "assistant") {
            return false;
        }

        if (
            message.role === "user" &&
            typeof message.content === "string" &&
            message.content.startsWith("AI Agent Parameters:")
        ) {
            return false;
        }

        if (message.role === "assistant" && !message.tool_calls?.length) {
            return false;
        }

        return true;
    };

    return (
        <Accordion defaultActiveKey={null} className="reasoning mb-1">
            <Accordion.Item eventKey="advanced-settings" className="border border-secondary rounded-2 panel-bg-2">
                <Accordion.Header
                    as={() => <AccordionButton className="rounded-2 panel-bg-2 fs-5 p-1">Reasoning</AccordionButton>}
                ></Accordion.Header>
                <Accordion.Body className="p-2">
                    <DotList
                        gap={2}
                        dotColor="info"
                        items={conversationDocument.Messages.filter(canShowMessage).map((message) => (
                            <ReasoningMessage
                                key={message.date}
                                message={message}
                                toolMessages={toolMessages}
                                parametersFromUser={conversationDocument.Parameters}
                            />
                        ))}
                    />
                </Accordion.Body>
            </Accordion.Item>
        </Accordion>
    );
}

interface ReasoningMessageProps {
    message: Message;
    toolMessages: Message[];
    parametersFromUser: Record<string, string>;
}

function ReasoningMessage({ message, toolMessages, parametersFromUser }: ReasoningMessageProps) {
    switch (message.role) {
        case "system":
            return <SystemMessage message={message} />;
        case "user":
            return <UserMessage message={message} />;
        case "assistant":
            return (
                <AssistantMessage
                    message={message}
                    parametersFromUser={parametersFromUser}
                    toolMessages={toolMessages}
                />
            );
        default:
            return null;
    }
}

interface SystemMessageProps {
    message: Message;
}

function SystemMessage({ message }: SystemMessageProps) {
    return (
        <div>
            <div>Role: system</div>
            <div
                className="mt-1 p-2 rounded-2 border border-secondary bg-body overflow-auto"
                style={{ maxHeight: "200px", whiteSpace: "pre-wrap" }}
            >
                {message.content}
            </div>
        </div>
    );
}

interface UserMessageProps {
    message: Message;
}

function UserMessage({ message }: UserMessageProps) {
    const isMessageWithParameters = message.content?.startsWith("AI Agent Parameters:");

    if (isMessageWithParameters) {
        return null;
    }

    return (
        <div>
            <div>Role: user</div>
            <div
                className="mt-1 p-2 rounded-2 border border-secondary bg-body overflow-auto"
                style={{ maxHeight: "200px", whiteSpace: "pre-wrap" }}
            >
                {message.content}
            </div>
        </div>
    );
}

interface AssistantMessageProps {
    message: Message;
    toolMessages: Message[];
    parametersFromUser: Record<string, string>;
}

function AssistantMessage({ message, toolMessages, parametersFromUser }: AssistantMessageProps) {
    if (!message.tool_calls?.length) {
        return null;
    }

    return (
        <div>
            <div className="mb-1">Role: assistant</div>
            {message.tool_calls.map((toolCall) => (
                <AssistantToolResponse
                    key={toolCall.id}
                    name={toolCall.function.name}
                    parametersFromModel={toolCall.function.arguments}
                    parametersFromUser={parametersFromUser}
                    toolContent={toolMessages.find((m) => m.tool_call_id === toolCall.id).content}
                />
            ))}
        </div>
    );
}

interface AssistantToolResponseProps {
    name: string;
    toolContent: string;
    parametersFromModel: string;
    parametersFromUser: Record<string, string>;
}

function AssistantToolResponse({
    name,
    parametersFromModel,
    toolContent,
    parametersFromUser,
}: AssistantToolResponseProps) {
    const id = useId();

    return (
        <Accordion className="transcript-tool border border-secondary rounded-2 panel-bg-3">
            <Accordion.Item eventKey={id} className="panel-bg-3">
                <Accordion.Header
                    as={() => (
                        <AccordionButton className="rounded-2 panel-bg-3 fs-5 p-1">
                            <div className="hstack gap-2 fs-5">
                                <div className="p-1 rounded-1 bg-faded-primary border border-primary tool-icon">
                                    <Icon icon="hammer" color="primary" margin="m-0" />
                                </div>
                                <div className="text-truncate">Tool call: {name}</div>
                            </div>
                        </AccordionButton>
                    )}
                ></Accordion.Header>
                <Accordion.Collapse eventKey={id} mountOnEnter unmountOnExit>
                    <Accordion.Body className="rounded-2">
                        <ToolCallBody
                            name={name}
                            toolContent={toolContent}
                            parametersFromModel={parametersFromModel}
                            parametersFromUser={parametersFromUser}
                        />
                    </Accordion.Body>
                </Accordion.Collapse>
            </Accordion.Item>
        </Accordion>
    );
}

interface ToolCallBodyProps {
    name: string;
    toolContent: string;
    parametersFromModel: string;
    parametersFromUser?: Record<string, string>;
}

function ToolCallBody({ name, toolContent, parametersFromUser, parametersFromModel }: ToolCallBodyProps) {
    const prettifiedArguments = aiAgentsUtils.getPrettifiedContent(parametersFromModel);
    const argumentsMode = aceEditorUtils.getAceEditorMode(prettifiedArguments);
    const rqlLanguageService = useRqlLanguageService();

    const { control } = useFormContext<EditGenAiTaskFormData>();
    const formQueryTools = useWatch({
        control,
        name: "queries",
    });

    const formQueryTool = formQueryTools.find((x) => x.name === name);

    const { linkToQuery, queryWithParameters } = useToolQueryDetails({
        queryText: formQueryTool?.query ?? "",
        parametersFromUser,
        parametersFromModel,
    });

    return (
        <div className="vstack gap-2">
            {formQueryTool?.description && (
                <div>
                    <small className="text-muted">Description</small>
                    <div>{formQueryTool.description}</div>
                    <hr className="my-1" />
                </div>
            )}
            <div>
                <div className="d-flex justify-content-between align-items-end">
                    <small className="text-muted">Query</small>
                    <Button
                        variant="info"
                        className="rounded-pill mb-1"
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
                    height={aceEditorUtils.getAceEditorHeight(queryWithParameters, { maxLineCount: 8 })}
                    languageService={rqlLanguageService}
                />
            </div>
            <div>
                <small className="text-muted">Parameters filled by LLM</small>
                <AceEditor
                    defaultValue={prettifiedArguments}
                    readOnly
                    mode={argumentsMode}
                    height={aceEditorUtils.getAceEditorHeight(prettifiedArguments)}
                />
            </div>
            <QueryToolResponseContent content={toolContent} />
        </div>
    );
}

interface QueryToolResponseContentProps {
    content: string;
}

function QueryToolResponseContent({ content }: QueryToolResponseContentProps) {
    const aceRef = useRef<ReactAce>(null);

    const isTable = content.startsWith("[") && content.endsWith("]") && content.length > 2;
    const tableData = useMemo(
        () => (isTable ? JSON.parse(content).map((x: any) => new document(x)) : []),
        [content, isTable]
    );
    const contentMode = aceEditorUtils.getAceEditorMode(content);

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
        <div>
            <div className="text-emphasis">Results: {tableData.length}</div>

            {isTable ? (
                <VirtualTable
                    table={table}
                    heightInPx={virtualTableUtils.getHeightInPx(tableData.length, 300)}
                    className="border border-secondary rounded-2"
                />
            ) : (
                <AceEditor
                    aceRef={aceRef}
                    defaultValue={content}
                    readOnly
                    mode={contentMode}
                    height="150px"
                    actions={[{ component: <AceEditor.FullScreenAction /> }, { component: <AceEditor.FormatAction /> }]}
                />
            )}
        </div>
    );
}

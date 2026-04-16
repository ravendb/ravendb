import AceEditor from "components/common/ace/AceEditor";
import { aceEditorUtils } from "components/common/ace/aceEditorUtils";
import { Icon } from "components/common/Icon";
import useRqlLanguageService from "components/hooks/useRqlLanguageService";
import useToolQueryDetails from "components/pages/database/aiHub/aiAgents/hooks/useToolQueryDetails";
import {
    AiAgentToolCall,
    AiAgentToolCallAction,
    AiAgentToolCallQuery,
    AiAgentToolCallSubAgent,
    AiAgentToolCallUnknown,
} from "components/pages/database/aiHub/aiAgents/utils/aiAgentsTypes";
import { aiAgentsUtils } from "components/pages/database/aiHub/aiAgents/utils/aiAgentsUtils";
import Button from "react-bootstrap/Button";
import Accordion from "react-bootstrap/Accordion";
import assertUnreachable from "components/utils/assertUnreachable";
import { AiAgentToolResponseContent } from "components/pages/database/aiHub/aiAgents/partials/aiAgentMessages/AiAgentToolResponseContent";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAiAgentMessagesContext } from "components/pages/database/aiHub/aiAgents/partials/aiAgentMessages/AiAgentMessagesContext";

interface AiAgentToolTranscriptProps {
    toolCall: AiAgentToolCall;
}

export function AiAgentToolTranscript({ toolCall }: AiAgentToolTranscriptProps) {
    const type = toolCall.type;

    switch (type) {
        case "query":
            return <QueryToolTranscript toolCall={toolCall} />;
        case "action":
            return <ActionToolTranscript toolCall={toolCall} />;
        case "sub-agent":
            return <SubAgentTranscript toolCall={toolCall} />;
        case "unknown":
            return <UnknownToolTranscript toolCall={toolCall} />;
        default:
            assertUnreachable(type);
    }
}

function QueryToolTranscript({ toolCall }: { toolCall: AiAgentToolCallQuery }) {
    const detailsId = toolCall.id + "-details";

    const rqlLanguageService = useRqlLanguageService();
    const { parametersFromUser } = useAiAgentMessagesContext();

    const { linkToQuery, queryWithParameters } = useToolQueryDetails({
        queryText: toolCall.configDetails.Query,
        parametersFromUser,
        parametersFromModel: toolCall.arguments,
    });

    return (
        <Accordion className="transcript-tool border border-secondary rounded-2 panel-bg-1">
            <Accordion.Item eventKey={toolCall.id} className="panel-bg-1">
                <Accordion.Header>
                    <div className="hstack gap-2">
                        <div className="tool-icon bg-faded-primary">
                            <Icon icon="query" color="primary" margin="m-0" />
                        </div>
                        <div className="text-truncate">Query tool: {toolCall.name}</div>
                    </div>
                </Accordion.Header>
                <Accordion.Collapse eventKey={toolCall.id} mountOnEnter unmountOnExit>
                    <Accordion.Body className="panel-bg-1 rounded-2 vstack gap-2">
                        <Accordion className="tool-call-details border border-secondary rounded-2 panel-bg-2">
                            <Accordion.Item eventKey={detailsId} className="panel-bg-2">
                                <Accordion.Header className="p-1">
                                    <Icon icon="settings" />
                                    See details
                                </Accordion.Header>
                                <Accordion.Collapse eventKey={detailsId} mountOnEnter unmountOnExit>
                                    <Accordion.Body className="panel-bg-2 rounded-2 pt-0">
                                        <div>
                                            <small className="text-muted">Description</small>
                                            <div>{toolCall.configDetails.Description}</div>
                                            <hr className="my-1" />
                                        </div>
                                        <SchemaField
                                            parametersSchema={toolCall.configDetails.ParametersSchema}
                                            parametersSampleObject={toolCall.configDetails.ParametersSampleObject}
                                        />
                                        <div className="d-flex justify-content-between mb-1 align-items-end mt-2">
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
                                            height={aceEditorUtils.getAceEditorHeight(queryWithParameters, {
                                                maxLineCount: 8,
                                            })}
                                            languageService={rqlLanguageService}
                                        />
                                    </Accordion.Body>
                                </Accordion.Collapse>
                            </Accordion.Item>
                        </Accordion>
                        <FilledParametersField toolArguments={toolCall.arguments} />
                        <div className="bg-faded-primary p-2 border-radius-xs border border-primary w-100">
                            <div className="text-emphasis">Query tool result</div>
                            <AiAgentToolResponseContent content={toolCall.responseMessage.content} />
                        </div>
                    </Accordion.Body>
                </Accordion.Collapse>
            </Accordion.Item>
        </Accordion>
    );
}

function ActionToolTranscript({ toolCall }: { toolCall: AiAgentToolCallAction }) {
    const detailsId = toolCall.id + "-details";

    return (
        <Accordion className="transcript-tool border border-secondary rounded-2 panel-bg-1">
            <Accordion.Item eventKey={toolCall.id} className="panel-bg-1">
                <Accordion.Header>
                    <div className="hstack gap-2">
                        <div className="tool-icon bg-faded-primary">
                            <Icon icon="force" color="primary" margin="m-0" />
                        </div>
                        <div className="text-truncate">Action tool: {toolCall.name}</div>
                    </div>
                </Accordion.Header>
                <Accordion.Collapse eventKey={toolCall.id} mountOnEnter unmountOnExit>
                    <Accordion.Body className="panel-bg-1 rounded-2 vstack gap-2">
                        <Accordion className="tool-call-details border border-secondary rounded-2 panel-bg-2">
                            <Accordion.Item eventKey={detailsId} className="panel-bg-2">
                                <Accordion.Header className="p-1">
                                    <Icon icon="settings" />
                                    See details
                                </Accordion.Header>
                                <Accordion.Collapse eventKey={detailsId} mountOnEnter unmountOnExit>
                                    <Accordion.Body className="panel-bg-2 rounded-2 pt-0">
                                        <div>
                                            <small className="text-muted">Description</small>
                                            <div>{toolCall.configDetails.Description}</div>
                                            <hr className="my-1" />
                                        </div>
                                        <SchemaField
                                            parametersSchema={toolCall.configDetails.ParametersSchema}
                                            parametersSampleObject={toolCall.configDetails.ParametersSampleObject}
                                        />
                                    </Accordion.Body>
                                </Accordion.Collapse>
                            </Accordion.Item>
                        </Accordion>

                        <FilledParametersField toolArguments={toolCall.arguments} />
                    </Accordion.Body>
                </Accordion.Collapse>
            </Accordion.Item>
        </Accordion>
    );
}

function SubAgentTranscript({ toolCall }: { toolCall: AiAgentToolCallSubAgent }) {
    const subConversationId = toolCall.responseMessage?.subConversationId;
    const aiAgentMessages = useAiAgentMessagesContext();

    const { appUrl } = useAppUrls();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const subAgentConversationLink = subConversationId
        ? appUrl.forChatAiAgent(databaseName, toolCall.name, subConversationId)
        : null;

    const isTest = aiAgentMessages.mode === "test";

    const handleOpenTestSubConversation = () => {
        if (!isTest) {
            return;
        }

        aiAgentMessages.openTestSubConversation(subConversationId);
    };

    return (
        <Accordion className="transcript-tool sub-agent-transcript border border-secondary rounded-2 panel-bg-1">
            <Accordion.Item eventKey={toolCall.id} className="panel-bg-1">
                <Accordion.Header>
                    <div className="hstack gap-2">
                        <div className="tool-icon bg-faded-primary">
                            <Icon icon="user" color="primary" margin="m-0" />
                        </div>
                        <div className="text-truncate">Sub-agent: {toolCall.name}</div>
                    </div>
                </Accordion.Header>
                <Accordion.Collapse eventKey={toolCall.id} mountOnEnter unmountOnExit>
                    <Accordion.Body className="panel-bg-1 rounded-2 sub-agent-transcript__steps">
                        <div className="sub-agent-transcript__step">
                            <div className="tool-icon opacity-0" />
                            <FilledParametersField
                                toolArguments={toolCall.arguments}
                                className="sub-agent-transcript__content"
                            />
                        </div>
                        <div className="sub-agent-transcript__step">
                            <div className="tool-icon bg-faded-primary">
                                <Icon icon="user" color="primary" margin="m-0" />
                            </div>
                            <div className="sub-agent-transcript__content">
                                <div>Sub-conversation created</div>
                                {isTest ? (
                                    <button
                                        type="button"
                                        className="sub-agent-transcript__link btn panel-bg-2 rounded-2 border border-secondary hstack justify-content-between px-2 py-1 text-muted w-100"
                                        title="Open the sub-agent transcript returned by the current test run"
                                        onClick={handleOpenTestSubConversation}
                                    >
                                        <Icon icon="preview" />
                                        <span className="text-truncate">Conversation details</span>
                                    </button>
                                ) : (
                                    <a
                                        href={subAgentConversationLink}
                                        className="sub-agent-transcript__link btn panel-bg-2 rounded-2 border border-secondary hstack justify-content-between px-2 py-1 text-muted"
                                        title={subConversationId}
                                        target="_blank"
                                        rel="noreferrer"
                                    >
                                        <Icon icon="ai-agents" />
                                        <span className="text-truncate">Conversation details</span>
                                        <Icon icon="newtab" margin="ms-auto" />
                                    </a>
                                )}
                            </div>
                        </div>
                        <div className="sub-agent-transcript__step">
                            <div className="tool-icon bg-faded-success border border-success">
                                <Icon icon="check" color="success" margin="m-0" />
                            </div>
                            <div className="sub-agent-transcript__content">
                                <div className="text-truncate">Sub-agent final answer</div>
                                <AiAgentToolResponseContent content={toolCall.responseMessage.content} />
                            </div>
                        </div>
                    </Accordion.Body>
                </Accordion.Collapse>
            </Accordion.Item>
        </Accordion>
    );
}

function UnknownToolTranscript({ toolCall }: { toolCall: AiAgentToolCallUnknown }) {
    return (
        <Accordion className="transcript-tool border border-secondary rounded-2 panel-bg-1">
            <Accordion.Item eventKey={toolCall.id} className="panel-bg-1">
                <Accordion.Header>
                    <div className="hstack gap-2">
                        <div className="tool-icon bg-faded-primary">
                            <Icon icon="studio-config" color="primary" margin="m-0" />
                        </div>
                        <div className="text-truncate">Tool: {toolCall.name}</div>
                    </div>
                </Accordion.Header>
                <Accordion.Collapse eventKey={toolCall.id} mountOnEnter unmountOnExit>
                    <Accordion.Body className="panel-bg-1 rounded-2 vstack gap-2">
                        <FilledParametersField toolArguments={toolCall.arguments} />
                        {toolCall.responseMessage?.content && (
                            <div className="bg-faded-primary p-2 border-radius-xs border border-primary w-100">
                                <div className="text-emphasis">Result</div>
                                <AiAgentToolResponseContent content={toolCall.responseMessage.content} />
                            </div>
                        )}
                    </Accordion.Body>
                </Accordion.Collapse>
            </Accordion.Item>
        </Accordion>
    );
}

interface FilledParametersFieldProps {
    toolArguments: string;
    className?: string;
}

function FilledParametersField({ toolArguments, className }: FilledParametersFieldProps) {
    const prettifiedArguments = aiAgentsUtils.getPrettifiedContent(toolArguments);
    const argumentsMode = aceEditorUtils.getAceEditorMode(prettifiedArguments);

    return (
        <div className={className}>
            <small className="text-muted">Parameters filled by LLM</small>
            <AceEditor
                defaultValue={prettifiedArguments}
                readOnly
                mode={argumentsMode}
                height={aceEditorUtils.getAceEditorHeight(prettifiedArguments)}
                actions={[
                    { component: <AceEditor.FullScreenAction /> },
                    { component: <AceEditor.FormatAction /> },
                    { component: <AceEditor.AutoResizeHeightAction /> },
                ]}
            />
        </div>
    );
}

interface SchemaFieldProps {
    parametersSchema: string;
    parametersSampleObject: string;
}

function SchemaField({ parametersSchema, parametersSampleObject }: SchemaFieldProps) {
    if (parametersSchema) {
        const prettifiedSchema = aiAgentsUtils.getPrettifiedContent(parametersSchema);

        return (
            <div>
                <small className="text-muted">Parameters schema</small>
                <AceEditor
                    defaultValue={prettifiedSchema}
                    readOnly
                    mode="json"
                    height={aceEditorUtils.getAceEditorHeight(prettifiedSchema, { maxLineCount: 6 })}
                />
            </div>
        );
    }

    const prettifiedSampleObject = aiAgentsUtils.getPrettifiedContent(parametersSampleObject);

    return (
        <div>
            <small className="text-muted">Sample parameters object</small>
            <AceEditor
                defaultValue={prettifiedSampleObject}
                readOnly
                mode="json"
                height={aceEditorUtils.getAceEditorHeight(prettifiedSampleObject, { maxLineCount: 6 })}
            />
        </div>
    );
}

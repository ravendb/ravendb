import AceEditor from "components/common/ace/AceEditor";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import moment from "moment";
import { useRef } from "react";
import ReactAce from "react-ace";
import Spinner from "react-bootstrap/Spinner";

export interface AiAgentMessage {
    id: string;
    author: "user" | "agent";
    text?: string;
    date?: Date;
    state?: "loading" | "success" | "error";
    usage?: Raven.Client.Documents.Operations.AI.Agents.AiUsage;
}

interface AiAgentMessagesProps {
    messages: AiAgentMessage[];
}

export default function AiAgentMessages({ messages }: AiAgentMessagesProps) {
    return (
        <div className="w-100 vstack gap-2">
            {messages.map((x, idx) =>
                x.author === "user" ? (
                    <div key={idx} className="hstack justify-content-end">
                        <div
                            className="text-end bg-faded-primary p-2 rounded-3 border border-primary text-reset"
                            style={{ maxWidth: "75%" }}
                        >
                            {x.text}
                        </div>
                    </div>
                ) : (
                    <AgentMessage key={idx} agentMessage={x} />
                )
            )}
        </div>
    );
}

function AgentMessage({ agentMessage }: { agentMessage: AiAgentMessage }) {
    const aceRef = useRef<ReactAce>(null);

    return (
        <div>
            <div className="hstack justify-content-between mb-2">
                <div className="hstack gap-2">
                    <div className="p-1 rounded-2 border">
                        <Icon icon="sparkles" margin="m-0" />
                    </div>
                    <strong>AI Agent</strong>
                    <div className="text-muted">{moment(agentMessage.date).format("HH:mm A")}</div>
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
                <AceEditor
                    aceRef={aceRef}
                    value={agentMessage.text}
                    readOnly
                    mode="json"
                    actions={[{ component: <AceEditor.FullScreenAction /> }]}
                    height={getAgentAceEditorHeight(agentMessage.text)}
                />
            )}
        </div>
    );
}

function getAgentAceEditorHeight(text: string): `${number}px` {
    if (!text) {
        return "100px";
    }

    const lineHeight = 26;
    const lineCount = text.split("\n").length;

    if (lineCount <= 12) {
        return `${lineCount * lineHeight}px`;
    }

    return "320px";
}

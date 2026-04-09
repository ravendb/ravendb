import genUtils from "common/generalUtils";
import AiTokensUsagePopoverBody from "components/common/AiTokensUsagePopoverBody";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import useBoolean from "components/hooks/useBoolean";
import { useResizeObserver } from "components/hooks/useResizeObserver";
import { AiAgentMessage } from "components/pages/database/aiHub/aiAgents/utils/aiAgentsTypes";
import { useRef } from "react";
import Button from "react-bootstrap/Button";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

interface AiAgentSummaryProps {
    agentMessage: AiAgentMessage;
}

export default function AiAgentSummary({ agentMessage }: AiAgentSummaryProps) {
    const content = agentMessage.content;
    const { value: isExpanded, setTrue: expand, setFalse: collapse } = useBoolean(false);
    const contentRef = useRef<HTMLDivElement>(null);
    const { height } = useResizeObserver({ ref: contentRef });

    const canExpand = height > MAX_HEIGHT_PX && !isExpanded;

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
            <div className="assistant-summary" style={{ maxHeight: isExpanded ? "none" : MAX_HEIGHT_PX }}>
                <div className="assistant-summary__content" ref={contentRef}>
                    <ReactMarkdown remarkPlugins={[remarkGfm]}>{content}</ReactMarkdown>
                    {canExpand && (
                        <Button
                            className="assistant-summary__show-more rounded-pill"
                            variant="primary"
                            size="sm"
                            onClick={expand}
                        >
                            <Icon icon="expand-vertical" />
                            Show More
                        </Button>
                    )}
                </div>
                {isExpanded && (
                    <Button className="assistant-summary__show-less" variant="link" size="xs" onClick={collapse}>
                        Show Less
                    </Button>
                )}
            </div>
        </div>
    );
}

const MAX_HEIGHT_PX = 240;

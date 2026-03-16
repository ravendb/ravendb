import { Icon } from "components/common/Icon";
import { AiAgentToolResponseContent } from "components/pages/database/aiHub/aiAgents/partials/aiAgentMessages/AiAgentToolResponseContent";
import Badge from "react-bootstrap/Badge";

interface AiAgentSubmittedActionToolProps {
    content: string;
    toolName: string;
}

export function AiAgentSubmittedActionTool({ content, toolName }: AiAgentSubmittedActionToolProps) {
    return (
        <div className="bg-faded-primary p-2 border-radius-xs border border-primary w-100">
            <div className="hstack justify-content-between mb-1">
                <div>
                    Response from action tool: <strong>{toolName}</strong>
                </div>
                <Badge bg="primary" pill>
                    <Icon icon="check" /> Submitted
                </Badge>
            </div>

            <AiAgentToolResponseContent content={content} />
        </div>
    );
}

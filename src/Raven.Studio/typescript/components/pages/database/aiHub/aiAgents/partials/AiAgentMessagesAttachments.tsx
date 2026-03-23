import { AiAgentMessageAttachment } from "../utils/aiAgentsTypes";
import { Icon } from "components/common/Icon";
import { aiAgentsUtils } from "components/pages/database/aiHub/aiAgents/utils/aiAgentsUtils";

interface AiAgentMessagesAttachmentsProps {
    attachments: AiAgentMessageAttachment[];
}

export function AiAgentMessagesAttachments({ attachments }: AiAgentMessagesAttachmentsProps) {
    if (!attachments?.length) {
        return null;
    }

    return (
        <div className="hstack gap-1 mb-1 flex-wrap">
            {attachments.map((attachment) => (
                <div
                    key={attachment.name}
                    className="border border-secondary rounded-1 hstack gap-1 well small"
                    style={{
                        padding: "1px 4px",
                    }}
                >
                    <Icon
                        icon={aiAgentsUtils.getAttachmentIcon(attachment.contentType)}
                        color="primary"
                        margin="m-0"
                        size="xs"
                    />
                    <span>{attachment.name}</span>
                </div>
            ))}
        </div>
    );
}

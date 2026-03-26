import { Icon } from "components/common/Icon";
import { ChatAiAgentFormData } from "components/pages/database/aiHub/aiAgents/chat/utils/chatAiAgentValidation";
import Button from "react-bootstrap/Button";
import { UseFieldArrayReturn } from "react-hook-form";
import IconName from "typings/server/icons";

interface ChatAiAgentPromptAttachmentsProps {
    attachmentsFieldsArray: UseFieldArrayReturn<ChatAiAgentFormData, "attachments", "id">;
}

export default function ChatAiAgentPromptAttachments({ attachmentsFieldsArray }: ChatAiAgentPromptAttachmentsProps) {
    if (!attachmentsFieldsArray.fields.length) {
        return null;
    }

    return (
        <div className="hstack gap-1 mb-1 overflow-y-auto" style={{ maxHeight: "100px" }}>
            {attachmentsFieldsArray.fields.map((attachment, index) => (
                <div
                    key={attachment.id}
                    className="border border-secondary rounded-1 hstack gap-1 well small"
                    style={{
                        padding: "1px 4px",
                    }}
                >
                    <Icon icon={getFileIcon(attachment.contentType)} color="primary" margin="m-0" size="xs" />
                    <span>{attachment.name}</span>
                    <Button
                        variant="link"
                        className="text-muted p-0 hover-filter"
                        size="xs"
                        onClick={() => attachmentsFieldsArray.remove(index)}
                        title="Remove attachment"
                    >
                        <Icon icon="cancel" margin="m-0" size="xs" />
                    </Button>
                </div>
            ))}
        </div>
    );
}

function getFileIcon(contentType: string): IconName {
    switch (contentType) {
        case "image/png":
        case "image/gif":
        case "image/jpeg":
        case "image/webp":
            return "filesystem";
        default:
            return "document2";
    }
}

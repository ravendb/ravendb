import { AiAgentMessageAttachment } from "../../utils/aiAgentsTypes";
import { Icon } from "components/common/Icon";
import { chatAiAgentAttachmentsUtils } from "components/pages/database/aiHub/aiAgents/chat/utils/chatAiAgentAttachmentsUtils";
import downloader from "common/downloader";
import { useMemo } from "react";
import endpoints from "endpoints";
import Button from "react-bootstrap/Button";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

interface AiAgentMessagesAttachmentsProps {
    attachments: AiAgentMessageAttachment[];
    documentId: string;
}

export function AiAgentMessagesAttachments({ attachments, documentId }: AiAgentMessagesAttachmentsProps) {
    const { appUrl } = useAppUrls();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const downloaderInstance = useMemo(() => new downloader(), []);

    const downloadAttachment = (fileName: string) => {
        const args = {
            id: documentId,
            name: fileName,
        };

        const url = endpoints.databases.attachment.attachments + appUrl.urlEncodeArgs(args);
        downloaderInstance.download(databaseName, url);
    };

    if (!attachments?.length) {
        return null;
    }

    return (
        <div className="hstack gap-1 mb-1 flex-wrap">
            {attachments.map((attachment) => (
                <Button
                    key={attachment.name}
                    className="border border-secondary rounded-1 hstack gap-1 well small"
                    style={{
                        padding: "1px 4px",
                    }}
                    onClick={() => downloadAttachment(attachment.name)}
                    size="xs"
                    title="Download attachment"
                    variant="secondary"
                >
                    <Icon
                        icon={chatAiAgentAttachmentsUtils.getIcon(attachment.contentType)}
                        color="primary"
                        margin="m-0"
                        size="xs"
                    />
                    <span>{attachment.name}</span>
                </Button>
            ))}
        </div>
    );
}

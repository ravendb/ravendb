import { useRef } from "react";
import ReactAce from "react-ace/lib/ace";
import AceEditor from "components/common/ace/AceEditor";
import useDialog from "components/common/Dialog";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { GenAiAiAttachment } from "../utils/editGenAiTaskValidation";
import genUtils from "common/generalUtils";
import Badge from "react-bootstrap/Badge";

export default function EditGenAiTaskAttachmentsButton({ attachments }: { attachments: GenAiAiAttachment[] }) {
    const dialog = useDialog();

    if (!attachments || attachments.length === 0) {
        return null;
    }

    const hasNotFound = attachments.some((attachment) => attachment.Source === "NotFound");

    const showAttachments = async () => {
        await dialog({
            title: (
                <span>
                    <Icon icon="attachment" />
                    Attachments
                </span>
            ),
            message: <AttachmentsModalBody attachments={attachments} />,
            modalSize: "lg",
        });
    };

    return (
        <Button
            variant={hasNotFound ? "warning" : "success"}
            title="Attachments"
            onClick={showAttachments}
            size="xs"
            className="rounded-2"
        >
            <Icon icon="preview" />
            See attachments ({genUtils.formatNumberToStringFixed(attachments.length, 0)})
            {hasNotFound && <Icon icon="warning" className="ms-1" />}
        </Button>
    );
}

function AttachmentsModalBody({ attachments }: { attachments: GenAiAiAttachment[] }) {
    const notFound = attachments.filter((attachment) => attachment.Source === "NotFound");
    const fromUser = attachments.filter((attachment) => attachment.Source === "FromUser");
    const fromDatabase = attachments.filter((attachment) => attachment.Source === "FromDatabase");

    return (
        <div className="vstack gap-2 overflow-auto" style={{ maxHeight: "500px" }}>
            {notFound.length > 0 && (
                <div>
                    <div className="w-fit-content">
                        <Badge bg="warning" className="d-flex align-items-center">
                            <Icon icon="warning" />
                            Not found:
                        </Badge>
                    </div>
                    <div className="vstack gap-2 py-1">
                        {notFound.map((attachment, idx) => (
                            <AttachmentEditor key={idx} attachment={attachment} />
                        ))}
                    </div>
                </div>
            )}
            {fromUser.length > 0 && (
                <div>
                    <div className="w-fit-content">
                        <Badge bg="success" className="d-flex align-items-center">
                            <Icon icon="user" /> From user:
                        </Badge>
                    </div>
                    <div className="vstack gap-2 py-1">
                        {fromUser.map((attachment, idx) => (
                            <AttachmentEditor key={idx} attachment={attachment} />
                        ))}
                    </div>
                </div>
            )}
            {fromDatabase.length > 0 && (
                <div>
                    <div className="w-fit-content">
                        <Badge bg="success" className="d-flex align-items-center">
                            <Icon icon="database" /> From database:
                        </Badge>
                    </div>
                    <div className="vstack gap-2 py-1">
                        {fromDatabase.map((attachment, idx) => (
                            <AttachmentEditor key={idx} attachment={attachment} />
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
}

function AttachmentEditor({ attachment }: { attachment: GenAiAiAttachment }) {
    const aceRef = useRef<ReactAce>(null);

    return (
        <AceEditor
            aceRef={aceRef}
            mode="json"
            value={JSON.stringify(attachment, null, 4)}
            readOnly={true}
            height="168px"
            actions={[{ component: <AceEditor.FullScreenAction /> }, { component: <AceEditor.ToggleNewLinesAction /> }]}
        />
    );
}

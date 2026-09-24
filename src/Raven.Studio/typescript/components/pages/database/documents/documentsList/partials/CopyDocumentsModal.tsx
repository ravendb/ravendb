import copyToClipboard from "common/copyToClipboard";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import Modal from "components/common/Modal";
import Button from "react-bootstrap/Button";

export interface CopyDocumentsModalData {
    title: string;
    text: string;
}

interface CopyDocumentsModalProps extends CopyDocumentsModalData {
    close: () => void;
}

export default function CopyDocumentsModal({ title, text, close }: CopyDocumentsModalProps) {
    return (
        <Modal show onHide={close} size="lg">
            <Modal.Header onCloseClick={close} className="pb-0">
                <div>
                    <Icon icon="copy" />
                    <span>{title}</span>
                </div>
            </Modal.Header>
            <Modal.Body className="pb-3">
                <pre style={{ maxHeight: "60vh" }} className="overflow-auto m-0 mt-3">
                    <Code language="javascript" code={text} isActionsHidden />
                </pre>
            </Modal.Body>
            <Modal.Footer>
                <Button type="button" variant="secondary" onClick={close}>
                    <Icon icon="close" />
                    Close
                </Button>
                <Button
                    type="button"
                    variant="primary"
                    onClick={() => copyToClipboard.copy(text, "Copied to clipboard")}
                >
                    <Icon icon="copy" />
                    Copy to Clipboard
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

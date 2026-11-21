import Button from "react-bootstrap/Button";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import Modal from "components/common/Modal";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface ConnectionTestErrorModalProps {
    error: string;
    onClose: () => void;
}

export default function ConnectionTestErrorModal({ error, onClose }: ConnectionTestErrorModalProps) {
    const commonT = useStudioTranslation("common");
    const [errorSummary] = error.split(/\r?\n/, 1);

    return (
        <Modal show onHide={onClose} contentClassName="modal-border bulge-danger" size="lg">
            <Modal.Header onCloseClick={onClose} className="hstack gap-3">
                <Icon icon="danger" color="danger" className="fs-2" margin="m-0" />
                <h3 className="m-0">{commonT("connectionTestFailed")}</h3>
            </Modal.Header>
            <Modal.Body className="vstack gap-3">
                <div className="text-break">{errorSummary}</div>
                <Code language="csharp" code={error} whiteSpace="pre-wrap" />
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" className="link-muted" onClick={onClose}>
                    {commonT("close")}
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

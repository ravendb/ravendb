import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import Code from "components/common/Code";
import copyToClipboard from "common/copyToClipboard";
import React from "react";
import Modal from "components/common/Modal";

interface ErrorModalProps {
    toggleErrorModal: () => void;
    error: string;
}

export function ErrorModal(props: ErrorModalProps) {
    const { toggleErrorModal, error } = props;
    return (
        <Modal size="xl" show onHide={toggleErrorModal} contentClassName="modal-border bulge-danger">
            <Modal.Header className="hstack gap-1 mb-4" onCloseClick={toggleErrorModal}>
                <Icon icon="warning" color="danger" margin="m-0" />
                <div className="text-center lead">Error</div>
            </Modal.Header>
            <Modal.Body>
                <Code code={error} language="csharp" />
                <div className="text-end mt-3">
                    <Button
                        className="rounded-pill"
                        variant="primary"
                        onClick={() => copyToClipboard.copy(error, "Copied error message to clipboard")}
                    >
                        <Icon icon="copy" /> <span>Copy to clipboard</span>
                    </Button>
                </div>
            </Modal.Body>
        </Modal>
    );
}

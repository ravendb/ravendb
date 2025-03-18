import Button from "react-bootstrap/Button";
import { CloseButton, Modal, ModalBody } from "reactstrap";
import { Icon } from "components/common/Icon";
import Code from "components/common/Code";
import copyToClipboard from "common/copyToClipboard";
import React from "react";

interface ErrorModalProps {
    toggleErrorModal: () => void;
    error: string;
}

export function ErrorModal(props: ErrorModalProps) {
    const { toggleErrorModal, error } = props;
    return (
        <Modal
            size="xl"
            wrapClassName="bs5"
            isOpen
            toggle={toggleErrorModal}
            contentClassName="modal-border bulge-danger"
        >
            <ModalBody>
                <div className="position-absolute m-2 end-0 top-0">
                    <CloseButton onClick={toggleErrorModal} />
                </div>
                <div className="hstack gap-1 mb-4">
                    <Icon icon="warning" color="danger" margin="m-0" />
                    <div className="text-center lead">Error</div>
                </div>
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
            </ModalBody>
        </Modal>
    );
}

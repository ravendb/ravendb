import { Icon } from "components/common/Icon";
import React from "react";
import { Button, Modal, ModalBody, ModalFooter, ModalHeader } from "reactstrap";

interface GatherDebugInfoAbortConfirmProps {
    isOpen: boolean;
    toggle: () => void;
    onConfirm: () => Promise<void>;
}

export default function GatherDebugInfoAbortConfirm({ isOpen, toggle, onConfirm }: GatherDebugInfoAbortConfirmProps) {
    const onSubmit = () => {
        onConfirm();
        toggle();
    };

    return (
        <Modal
            isOpen={isOpen}
            toggle={toggle}
            wrapClassName="bs5"
            centered
            contentClassName="modal-border bulge-warning"
        >
            <ModalHeader>Are you sure?</ModalHeader>
            <ModalBody>
                <div className="text-center lead">Do you want to abort package creation?</div>
            </ModalBody>
            <ModalFooter>
                <Button color="link" onClick={toggle} className="text-muted">
                    Cancel
                </Button>
                <Button color="warning" onClick={onSubmit} className="rounded-pill">
                    <Icon icon="cancel" className="me-1" />
                    Abort
                </Button>
            </ModalFooter>
        </Modal>
    );
}

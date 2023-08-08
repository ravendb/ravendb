import React from "react";
import { Button, Modal, ModalBody, ModalFooter, ModalHeader } from "reactstrap";
import { Icon } from "components/common/Icon";

interface ConfirmResetIndexProps {
    toggle: () => void;
    onConfirm: () => void;
}

export function TombstonesStateForceCleanupConfirm(props: ConfirmResetIndexProps) {
    const { toggle, onConfirm } = props;

    const onSubmit = () => {
        onConfirm();
        toggle();
    };

    return (
        <Modal isOpen toggle={toggle} wrapClassName="bs5" centered contentClassName="modal-border bulge-warning">
            <ModalHeader>Are you sure?</ModalHeader>
            <ModalBody>
                <div className="text-center lead">Do you want to force tombstones cleanup?</div>
            </ModalBody>
            <ModalFooter>
                <Button color="link" onClick={toggle} className="text-muted">
                    Cancel
                </Button>
                <Button color="warning" onClick={onSubmit} className="rounded-pill">
                    <Icon icon="force" />
                    Yes, cleanup
                </Button>
            </ModalFooter>
        </Modal>
    );
}

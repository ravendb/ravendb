import { Icon } from "components/common/Icon";
import React from "react";
import Button from "react-bootstrap/Button";
import Modal from "components/common/Modal";

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
        <Modal show={isOpen} onHide={toggle} contentClassName="modal-border bulge-warning">
            <Modal.Header closeButton={false}>Are you sure?</Modal.Header>
            <Modal.Body>
                <div className="text-center lead">Do you want to abort package creation?</div>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" onClick={toggle} className="link-muted">
                    Cancel
                </Button>
                <Button variant="warning" onClick={onSubmit} className="rounded-pill">
                    <Icon icon="cancel" className="me-1" />
                    Abort
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

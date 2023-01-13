import React from "react";
import { Alert, Button, Modal, ModalBody, ModalFooter, ModalHeader } from "reactstrap";
import { IndexSharedInfo } from "components/models/indexes";

interface ConfirmResetIndexProps {
    toggle: () => void;
    onConfirm: () => void;
    index: IndexSharedInfo;
}

export function ConfirmResetIndex(props: ConfirmResetIndexProps) {
    const { toggle, index, onConfirm } = props;

    const onSubmit = () => {
        onConfirm();
        toggle();
    };

    return (
        <Modal isOpen toggle={toggle} wrapClassName="bs5">
            <ModalHeader toggle={toggle}>Reset index?</ModalHeader>
            <ModalBody>
                You&apos;re resetting index: <br />
                <ul>
                    <li>
                        <strong>{index.name}</strong>
                    </li>
                </ul>
                <Alert color="warning">
                    <small>
                        Clicking <strong>Reset</strong> will remove all existing indexed data.
                    </small>
                    <br />
                    <small>All items matched by the index definition will be re-indexed.</small>
                </Alert>
            </ModalBody>
            <ModalFooter>
                <Button color="secondary" onClick={toggle}>
                    Cancel
                </Button>
                <Button color="danger" onClick={onSubmit}>
                    Reset
                </Button>
            </ModalFooter>
        </Modal>
    );
}

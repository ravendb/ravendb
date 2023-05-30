import React from "react";
import { Alert, Button, Modal, ModalBody, ModalFooter } from "reactstrap";
import { IndexSharedInfo } from "components/models/indexes";
import { Icon } from "components/common/Icon";

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
        <Modal isOpen toggle={toggle} wrapClassName="bs5" centered contentClassName="modal-border bulge-warning">
            <ModalBody className="vstack gap-4 position-relative">
                <div className="text-center">
                    <Icon icon="index" color="warning" addon="reset-index" className="fs-1" margin="m-0" />
                </div>
                <div className="position-absolute m-2 end-0 top-0">
                    <Button close onClick={toggle} />
                </div>
                <div className="text-center lead">
                    You&apos;re about to <span className="text-warning">reset</span> following index
                </div>
                <span className="text-center bg-faded-primary py-1 px-3 w-fit-content rounded-pill mx-auto">
                    <Icon icon="index" />
                    {index.name}
                </span>
                <Alert color="warning">
                    <small>
                        Clicking <strong>Reset</strong> will remove all existing indexed data.
                    </small>
                    <br />
                    <small>All items matched by the index definition will be re-indexed.</small>
                </Alert>
            </ModalBody>
            <ModalFooter>
                <Button color="link" onClick={toggle} className="text-muted">
                    Cancel
                </Button>
                <Button color="warning" onClick={onSubmit} className="rounded-pill">
                    <Icon icon="reset-index" />
                    Reset
                </Button>
            </ModalFooter>
        </Modal>
    );
}

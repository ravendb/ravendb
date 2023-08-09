import { Icon } from "components/common/Icon";
import React from "react";
import { useState } from "react";
import { Modal, ModalHeader, ModalBody, ModalFooter, Button } from "reactstrap";
import IconName from "typings/server/icons";

interface UseConfirmProps {
    message: string;
    title?: string;
    icon?: IconName;
    confirmText?: string;
}

const useConfirm = (props: UseConfirmProps): [() => React.JSX.Element, () => Promise<unknown>] => {
    const [promise, setPromise] = useState(null);

    const confirm = () =>
        new Promise((resolve) => {
            setPromise({ resolve });
        });

    const onClose = () => {
        setPromise(null);
    };

    const onConfirm = () => {
        promise?.resolve(true);
        onClose();
    };

    const onCancel = () => {
        promise?.resolve(false);
        onClose();
    };

    return [
        () => <ConfirmationModal {...props} onCancel={onCancel} isOpen={promise !== null} onConfirm={onConfirm} />,
        confirm,
    ];
};

interface ConfirmationModalProps extends UseConfirmProps {
    isOpen: boolean;
    onCancel: () => void;
    onConfirm: () => void;
}

function ConfirmationModal({
    isOpen,
    onCancel,
    onConfirm,
    title,
    message,
    icon,
    confirmText,
}: ConfirmationModalProps): React.JSX.Element {
    return (
        <Modal
            isOpen={isOpen}
            toggle={onCancel}
            wrapClassName="bs5"
            centered
            contentClassName="modal-border bulge-warning"
        >
            <ModalHeader>{title ?? "Are you sure?"}</ModalHeader>
            <ModalBody>{message}</ModalBody>
            <ModalFooter>
                <Button color="link" onClick={onCancel} className="text-muted">
                    Cancel
                </Button>
                <Button color="warning" onClick={onConfirm} className="rounded-pill">
                    {icon && <Icon icon={icon} />}
                    {confirmText ?? "Yes"}
                </Button>
            </ModalFooter>
        </Modal>
    );
}

export default useConfirm;

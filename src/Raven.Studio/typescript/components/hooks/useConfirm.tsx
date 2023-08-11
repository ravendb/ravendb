import { Icon } from "components/common/Icon";
import React, { ReactNode } from "react";
import { useState } from "react";
import { Modal, ModalHeader, ModalBody, ModalFooter, Button } from "reactstrap";
import IconName from "typings/server/icons";

interface UseConfirmProps {
    message?: ReactNode;
    title: string;
    icon?: IconName;
    confirmText?: string;
    actionColor?: "primary" | "secondary" | "success" | "info" | "warning" | "danger" | "node" | "shard";
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
    actionColor,
}: ConfirmationModalProps): React.JSX.Element {
    return (
        <Modal
            isOpen={isOpen}
            toggle={onCancel}
            wrapClassName="bs5"
            centered
            contentClassName={`modal-border bulge-${actionColor ?? "secondary"}`}
        >
            <ModalBody className="vstack gap-4 position-relative">
                <div className="text-center">
                    <Icon icon={icon} color={actionColor} className="fs-1" margin="m-0" />
                </div>
                <div className="position-absolute m-2 end-0 top-0">
                    <Button close onClick={onCancel} />
                </div>
                <div className="text-center lead">{title}</div>
                {message ? message : null}
            </ModalBody>
            <ModalFooter>
                <Button color="link" onClick={onCancel} className="text-muted">
                    Cancel
                </Button>
                <Button color={actionColor} onClick={onConfirm} className="rounded-pill">
                    {icon && <Icon icon={icon} />}
                    {confirmText ?? "Yes"}
                </Button>
            </ModalFooter>
        </Modal>
    );
}

export default useConfirm;

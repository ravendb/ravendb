import { ThemeColor } from "components/models/common";
import React, { createContext, PropsWithChildren, ReactNode, useContext, useRef, useState } from "react";
import IconName from "typings/server/icons";
import { Icon } from "./Icon";
import Button from "react-bootstrap/Button";
import Modal from "./Modal";
import { ModalProps } from "react-bootstrap/Modal";

export interface ConfirmOptions {
    title: ReactNode;
    icon?: IconName;
    actionColor?: ThemeColor;
    message?: ReactNode;
    confirmText?: string;
    confirmIcon?: IconName;
    size?: ModalProps["size"];
}

type InnerOptions = Partial<ConfirmOptions> & { isOpen: boolean };

const ConfirmDialog = createContext<(options: ConfirmOptions) => Promise<boolean>>(null);

export function ConfirmDialogProvider({ children }: PropsWithChildren) {
    const [options, setOptions] = useState<InnerOptions>({ isOpen: false });
    const promise = useRef<(choice: boolean) => void>(null);

    const { isOpen, title, icon, confirmIcon, message, size } = options;

    const confirmText = options.confirmText ?? "Yes";
    const actionColor = options.actionColor ?? "warning";

    const exposedConfirm = (incomingOptions: ConfirmOptions) => {
        return new Promise<boolean>((resolve) => {
            setOptions({ ...incomingOptions, isOpen: true });

            promise.current = (choice: boolean) => {
                resolve(choice);
                setOptions({ isOpen: false });
            };
        });
    };

    const onCancel = () => promise.current(false);
    const onConfirm = () => promise.current(true);

    return (
        <ConfirmDialog.Provider value={exposedConfirm}>
            {children}
            {isOpen && (
                <Modal show onHide={onCancel} contentClassName={`modal-border bulge-${actionColor}`} size={size}>
                    <Modal.Header closeButton className="vstack gap-4" onCloseClick={onCancel}>
                        {icon && (
                            <div className="text-center">
                                <Icon icon={icon} color={actionColor} className="fs-1" margin="m-0" />
                            </div>
                        )}
                        <div className="text-center lead">{title}</div>
                    </Modal.Header>
                    <Modal.Body className="vstack gap-3">{message}</Modal.Body>
                    <Modal.Footer>
                        <Button variant="link" onClick={onCancel} className="link-muted">
                            Cancel
                        </Button>
                        <Button variant={actionColor} onClick={onConfirm} className="rounded-pill">
                            {confirmIcon && <Icon icon={confirmIcon} />}
                            {confirmText}
                        </Button>
                    </Modal.Footer>
                </Modal>
            )}
        </ConfirmDialog.Provider>
    );
}

export default function useConfirm() {
    return useContext(ConfirmDialog);
}

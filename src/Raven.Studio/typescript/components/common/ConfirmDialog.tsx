import { TextColor } from "components/models/common";
import React, { createContext, PropsWithChildren, ReactNode, useContext, useRef, useState } from "react";
import { CloseButton, Modal, ModalBody, ModalFooter } from "reactstrap";
import IconName from "typings/server/icons";
import { Icon } from "./Icon";
import Button from "react-bootstrap/Button";

interface ConfirmOptions {
    title: ReactNode;
    icon?: IconName;
    actionColor?: TextColor;
    message?: ReactNode;
    confirmText?: string;
    confirmIcon?: IconName;
}

type InnerOptions = Partial<ConfirmOptions> & { isOpen: boolean };

const ConfirmDialog = createContext<(options: ConfirmOptions) => Promise<boolean>>(null);

export function ConfirmDialogProvider({ children }: PropsWithChildren) {
    const [options, setOptions] = useState<InnerOptions>({ isOpen: false });
    const promise = useRef<(choice: boolean) => void>();

    const { isOpen, title, icon, confirmIcon, message } = options;

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
                <Modal
                    isOpen
                    toggle={onCancel}
                    wrapClassName="bs5"
                    centered
                    contentClassName={`modal-border bulge-${actionColor}`}
                >
                    <ModalBody className="vstack gap-4 position-relative">
                        {icon && (
                            <div className="text-center">
                                <Icon icon={icon} color={actionColor} className="fs-1" margin="m-0" />
                            </div>
                        )}
                        <div className="position-absolute m-2 end-0 top-0">
                            <CloseButton onClick={onCancel} />
                        </div>
                        <div className="text-center lead">{title}</div>
                        {message}
                    </ModalBody>
                    <ModalFooter>
                        <Button variant="link" onClick={onCancel} className="link-muted">
                            Cancel
                        </Button>
                        <Button variant={actionColor} onClick={onConfirm} className="rounded-pill">
                            {confirmIcon && <Icon icon={confirmIcon} />}
                            {confirmText}
                        </Button>
                    </ModalFooter>
                </Modal>
            )}
        </ConfirmDialog.Provider>
    );
}

export default function useConfirm() {
    return useContext(ConfirmDialog);
}

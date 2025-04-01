import { TextColor } from "components/models/common";
import React, { ReactNode, createContext, useContext, useState, PropsWithChildren, useRef } from "react";
import { Modal, ModalBody, Button, ModalFooter } from "reactstrap";
import IconName from "typings/server/icons";
import { Icon } from "./Icon";

interface DialogOptions {
    title: ReactNode;
    icon?: IconName;
    actionColor?: TextColor;
    message?: ReactNode;
    closeText?: string;
    closeIcon?: IconName;
    modalSize?: "sm" | "lg" | "xl" | undefined;
}

type InnerOptions = Partial<DialogOptions> & { isOpen: boolean };

const Dialog = createContext<(options: DialogOptions) => Promise<boolean>>(null);

export function DialogProvider({ children }: PropsWithChildren) {
    const [options, setOptions] = useState<InnerOptions>({ isOpen: false });
    const promise = useRef<() => void>();

    const { isOpen, title, icon, closeIcon, message, modalSize } = options;

    const closeText = options.closeText ?? "Close";
    const actionColor = options.actionColor ?? "primary";

    const exposedPromise = (incomingOptions: DialogOptions) => {
        return new Promise<boolean>((resolve) => {
            setOptions({ ...incomingOptions, isOpen: true });

            promise.current = () => {
                resolve(undefined);
                setOptions({ isOpen: false });
            };
        });
    };

    const onClose = () => promise.current();

    return (
        <Dialog.Provider value={exposedPromise}>
            {children}
            {isOpen && (
                <Modal
                    isOpen
                    toggle={onClose}
                    wrapClassName="bs5"
                    size={modalSize}
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
                            <Button close onClick={onClose} />
                        </div>
                        <div className="text-center lead">{title}</div>
                        {message}
                    </ModalBody>
                    <ModalFooter>
                        <Button color={actionColor} onClick={onClose} className="rounded-pill">
                            {closeIcon && <Icon icon={closeIcon} />}
                            {closeText}
                        </Button>
                    </ModalFooter>
                </Modal>
            )}
        </Dialog.Provider>
    );
}

export default function useDialog() {
    return useContext(Dialog);
}

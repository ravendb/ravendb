import { ThemeColor } from "components/models/common";
import { ReactNode, createContext, useContext, useState, PropsWithChildren, useRef } from "react";
import Modal from "./Modal";
import IconName from "typings/server/icons";
import { Icon } from "./Icon";
import Button from "react-bootstrap/Button";

interface DialogOptions {
    title: ReactNode;
    icon?: IconName;
    actionColor?: ThemeColor;
    message?: ReactNode;
    closeText?: string;
    closeIcon?: IconName;
    modalSize?: "sm" | "lg" | "xl" | undefined;
}

type InnerOptions = Partial<DialogOptions> & { isOpen: boolean };

const Dialog = createContext<(options: DialogOptions) => Promise<boolean>>(null);

export function DialogProvider({ children }: PropsWithChildren) {
    const [options, setOptions] = useState<InnerOptions>({ isOpen: false });
    const promise = useRef<() => void>(null);

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
                <Modal size={modalSize} show onHide={onClose} contentClassName={`modal-border bulge-${actionColor}`}>
                    <Modal.Header closeButton className="vstack gap-4" onCloseClick={onClose}>
                        {icon && (
                            <div className="text-center">
                                <Icon icon={icon} color={actionColor} className="fs-1" margin="m-0" />
                            </div>
                        )}
                        <div className="text-center lead">{title}</div>
                    </Modal.Header>
                    <Modal.Body className="vstack gap-4 position-relative">{message}</Modal.Body>
                    <Modal.Footer>
                        <Button variant={actionColor} onClick={onClose} className="rounded-pill">
                            {closeIcon && <Icon icon={closeIcon} />}
                            {closeText}
                        </Button>
                    </Modal.Footer>
                </Modal>
            )}
        </Dialog.Provider>
    );
}

export default function useDialog() {
    return useContext(Dialog);
}

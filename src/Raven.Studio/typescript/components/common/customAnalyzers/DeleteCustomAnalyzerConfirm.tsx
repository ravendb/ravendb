import React from "react";
import { Modal, ModalBody, Button, ModalFooter } from "reactstrap";
import { Icon } from "../Icon";
import IconName from "typings/server/icons";

interface DeleteCustomAnalyzerConfirmProps {
    name: string;
    toggle: () => void;
    onConfirm: (name: string) => void;
    isServerWide?: boolean;
}

export default function DeleteCustomAnalyzerConfirm(props: DeleteCustomAnalyzerConfirmProps) {
    const { name, onConfirm, toggle, isServerWide } = props;

    const onSubmit = () => {
        onConfirm(name);
        toggle();
    };

    const iconName: IconName = isServerWide ? "server-wide-custom-analyzers" : "custom-analyzers";

    return (
        <Modal isOpen toggle={toggle} wrapClassName="bs5" centered contentClassName={"modal-border bulge-danger"}>
            <ModalBody className="vstack gap-4 position-relative">
                <div className="text-center">
                    <Icon icon={iconName} color="danger" className="fs-1" margin="m-0" />
                </div>
                <div className="position-absolute m-2 end-0 top-0">
                    <Button close onClick={toggle} />
                </div>
                <div className="text-center lead">
                    You&apos;re about to <span className="text-danger">delete</span> following{" "}
                    {isServerWide ? "server-wide" : ""} custom analyzer:
                </div>
                <span className="d-flex align-items-center word-break bg-faded-primary py-1 px-3 w-fit-content rounded-pill mx-auto">
                    <Icon icon={iconName} />
                    {name}
                </span>
            </ModalBody>
            <ModalFooter>
                <Button color="link" onClick={toggle} className="text-muted">
                    Cancel
                </Button>
                <Button color="danger" onClick={onSubmit} className="rounded-pill">
                    <Icon icon="trash" />
                    {"Delete"}
                </Button>
            </ModalFooter>
        </Modal>
    );
}

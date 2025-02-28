import { PopoverWithHover } from "components/common/PopoverWithHover";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import React from "react";

interface NodeInfoFailureProps {
    target: HTMLElement;
    openErrorModal: () => void;
}

export function NodeInfoFailure(props: NodeInfoFailureProps) {
    const { target, openErrorModal } = props;

    return (
        <PopoverWithHover target={target} placement="top">
            <div className="vstack gap-2 p-3">
                <div className="text-danger">
                    <Icon icon="warning" color="danger" /> Unable to load task status
                </div>
                <Button variant="danger" onClick={openErrorModal} className="rounded-pill">
                    Open error in modal <Icon icon="newtab" margin="ms-1" />
                </Button>
            </div>
        </PopoverWithHover>
    );
}

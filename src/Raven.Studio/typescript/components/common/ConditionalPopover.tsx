import { ReactNode, PropsWithChildren, useState } from "react";
import { Placement } from "react-bootstrap/types";
import { PopoverWithHover } from "./PopoverWithHover";
import Popover from "react-bootstrap/Popover";

interface Condition {
    isActive: boolean;
    message?: ReactNode | ReactNode[];
}

interface ConditionalPopoverProps extends Required<PropsWithChildren> {
    conditions: Condition | Condition[];
    popoverPlacement?: Placement;
}

export function ConditionalPopover(props: ConditionalPopoverProps) {
    const { children, popoverPlacement } = props;

    const [target, setTarget] = useState<HTMLElement>();

    const conditions = Array.isArray(props.conditions) ? props.conditions : [props.conditions];
    const message = conditions.find((x) => x.isActive)?.message;

    return (
        <>
            <div ref={setTarget} className="d-flex w-fit-content">
                {children}
            </div>
            {message != null && (
                <PopoverWithHover target={target} placement={popoverPlacement}>
                    <Popover.Body>{message}</Popover.Body>
                </PopoverWithHover>
            )}
        </>
    );
}

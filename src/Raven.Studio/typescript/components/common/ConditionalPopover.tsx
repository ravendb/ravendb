import React, { ReactNode, PropsWithChildren } from "react";
import { PopoverBody, UncontrolledPopover } from "reactstrap";
import useId from "hooks/useId";
import { Placement } from "@popperjs/core";

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

    const containerId = useId("conditional-popover-");

    const conditions = Array.isArray(props.conditions) ? props.conditions : [props.conditions];
    const message = conditions.find((x) => x.isActive)?.message;

    return (
        <>
            <div id={containerId} className="d-flex w-fit-content">
                {children}
            </div>

            {message && (
                <UncontrolledPopover target={containerId} trigger="hover" placement={popoverPlacement} className="bs5">
                    <PopoverBody>{message}</PopoverBody>
                </UncontrolledPopover>
            )}
        </>
    );
}

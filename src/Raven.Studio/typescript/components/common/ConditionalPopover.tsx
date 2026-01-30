import React, { ReactNode, PropsWithChildren, useState } from "react";
import { Placement } from "@popperjs/core";
import { PopoverWithHover } from "./PopoverWithHover";
import Popover from "react-bootstrap/Popover";
import classNames from "classnames";
import { ClassNameProps } from "components/models/common";

interface Condition {
    isActive: boolean;
    message?: ReactNode | ReactNode[];
}

interface ConditionalPopoverProps extends Required<PropsWithChildren>, ClassNameProps {
    conditions: Condition | Condition[];
    popoverPlacement?: Placement;
    className?: string;
    style?: React.CSSProperties;
}

export function ConditionalPopover(props: ConditionalPopoverProps) {
    const { children, popoverPlacement, className, style } = props;

    const [target, setTarget] = useState<HTMLElement>();

    const conditions = Array.isArray(props.conditions) ? props.conditions : [props.conditions];
    const message = conditions.find((x) => x.isActive)?.message;

    return (
        <>
            <div ref={setTarget} className={classNames("d-flex w-fit-content", className)} style={style}>
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

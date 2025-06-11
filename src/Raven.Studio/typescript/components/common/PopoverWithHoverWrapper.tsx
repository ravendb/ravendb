import { ReactNode, useState } from "react";
import { PopoverWithHover, PopoverWithHoverProps } from "./PopoverWithHover";
import Popover from "react-bootstrap/Popover";
import classNames from "classnames";

interface PopoverWithHoverWrapperProps extends Omit<PopoverWithHoverProps, "target"> {
    message: ReactNode | ReactNode[];
    isInPopoverBody?: boolean;
    inline?: boolean;
    wrapperClassName?: string;
}

export default function PopoverWithHoverWrapper({
    children,
    message,
    isInPopoverBody = true,
    inline = true,
    wrapperClassName,
    ...rest
}: PopoverWithHoverWrapperProps) {
    const [target, setTarget] = useState<HTMLElement>();
    return (
        <>
            <div ref={setTarget} className={classNames({ "d-inline-block": inline }, wrapperClassName)}>
                {children}
            </div>
            {message && (
                <PopoverWithHover target={target} {...rest}>
                    {isInPopoverBody ? <Popover.Body>{message}</Popover.Body> : message}
                </PopoverWithHover>
            )}
        </>
    );
}

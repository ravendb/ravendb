import { ReactNode, useState } from "react";
import { PopoverWithHover, PopoverWithHoverProps } from "./PopoverWithHover";
import Popover from "react-bootstrap/Popover";

interface PopoverWithHoverWrapperProps extends Omit<PopoverWithHoverProps, "target"> {
    message: ReactNode | ReactNode[];
    isInPopoverBody?: boolean;
}

export default function PopoverWithHoverWrapper({
    children,
    message,
    isInPopoverBody = true,
    ...rest
}: PopoverWithHoverWrapperProps) {
    const [target, setTarget] = useState<HTMLElement>();

    return (
        <>
            <div ref={setTarget} className="d-inline-block">
                {children}
            </div>
            <PopoverWithHover target={target} {...rest}>
                {isInPopoverBody ? <Popover.Body>{message}</Popover.Body> : message}
            </PopoverWithHover>
        </>
    );
}

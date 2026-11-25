import React, { ReactElement, ReactNode } from "react";
import OverlayTrigger from "components/common/OverlayTriggerWithMotion";
import Popover from "react-bootstrap/Popover";
import useUniqueId from "components/hooks/useUniqueId";

interface ClickPopoverProps {
    content: ReactNode;
    children: ReactElement;
}

export function ClickPopover({ content, children }: ClickPopoverProps) {
    const popoverId = useUniqueId("click-popover-");

    return (
        <OverlayTrigger
            trigger="click"
            rootClose
            placement="bottom"
            overlay={
                <Popover id={popoverId} className="bs5">
                    <Popover.Body>{content}</Popover.Body>
                </Popover>
            }
        >
            {children}
        </OverlayTrigger>
    );
}

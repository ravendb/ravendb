import React, { useRef } from "react";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import { Icon } from "components/common/Icon";

export function DatabaseLoadError(props: { error: string }) {
    const tooltipRef = useRef<HTMLElement>();

    return (
        <strong className="text-danger" ref={tooltipRef}>
            <Icon icon="exclamation" className="me-1"></Icon> Load error
            <PopoverWithHover target={tooltipRef.current} placement="top">
                <div className="p-2">Unable to load database: {props.error}</div>
            </PopoverWithHover>
        </strong>
    );
}

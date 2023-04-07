import React, { useRef } from "react";
import { PopoverWithHover } from "components/common/PopoverWithHover";

export function DatabaseLoadError(props: { error: string }) {
    const tooltipRef = useRef<HTMLElement>();

    return (
        <strong className="text-danger" ref={tooltipRef}>
            <i className="icon-exclamation"/> Load error
            <PopoverWithHover target={tooltipRef.current} placement="top">
                <div className="p-2">Unable to load database: {props.error}</div>
            </PopoverWithHover>
        </strong>
    );
}

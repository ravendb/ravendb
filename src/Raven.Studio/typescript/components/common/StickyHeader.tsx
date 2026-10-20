import React, { HTMLAttributes, ReactNode, useEffect, useRef } from "react";
import classNames from "classnames";
import { useResizeObserver } from "hooks/useResizeObserver";
import "./StickyHeader.scss";

interface StickyHeaderProps extends HTMLAttributes<HTMLDivElement> {
    children: ReactNode;
    flush?: boolean;
}

const stickyHeaderHeightVariable = "--sticky-header-height";

export function StickyHeader({ flush, children, className, ...rest }: StickyHeaderProps) {
    const ref = useRef<HTMLDivElement>(null);
    const { height } = useResizeObserver({ ref, box: "border-box" });

    useEffect(() => {
        const parent = ref.current?.parentElement;
        if (!parent || height == null) {
            return;
        }

        parent.style.setProperty(stickyHeaderHeightVariable, `${height}px`);
        return () => {
            parent.style.removeProperty(stickyHeaderHeightVariable);
        };
    }, [height]);

    return (
        <div {...rest} ref={ref} className={classNames("sticky-header", className, flush && "m-0 p-0")}>
            {children}
        </div>
    );
}

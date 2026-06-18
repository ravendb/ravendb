import React, { HTMLAttributes, ReactNode } from "react";
import classNames from "classnames";
import "./StickyHeader.scss";

interface StickyHeaderProps extends HTMLAttributes<HTMLDivElement> {
    children: ReactNode;
    flush?: boolean;
}

export function StickyHeader({ flush, children, className, ...rest }: StickyHeaderProps) {
    return (
        <div {...rest} className={classNames("sticky-header", className, flush && "sticky-header--flush")}>
            {children}
        </div>
    );
}

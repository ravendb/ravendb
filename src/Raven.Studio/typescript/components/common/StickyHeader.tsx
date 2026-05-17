import React, { HTMLAttributes, ReactNode } from "react";
import classNames from "classnames";
import "./StickyHeader.scss";

interface StickyHeaderProps extends HTMLAttributes<HTMLDivElement> {
    children: ReactNode;
}

export function StickyHeader({ children, className, ...rest }: StickyHeaderProps) {
    return (
        <div {...rest} className={classNames("sticky-header", className)}>
            {children}
        </div>
    );
}

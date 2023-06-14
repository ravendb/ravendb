import React, { ReactNode } from "react";
import classNames from "classnames";

import "./StickyHeader.scss";

interface StickyHeaderProps {
    children: ReactNode | ReactNode[];
    className?: string;
}

export function StickyHeader(props: StickyHeaderProps) {
    const { children, className } = props;

    return <div className={classNames("sticky-header", className)}>{children}</div>;
}

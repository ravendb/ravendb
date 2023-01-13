import React, { ReactNode } from "react";

interface StickyHeaderProps {
    children: ReactNode | ReactNode[];
}

export function StickyHeader(props: StickyHeaderProps) {
    const { children } = props;

    return <div className="sticky-header">{children}</div>;
}

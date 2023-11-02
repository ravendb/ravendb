import React, { ReactNode } from "react";
import classNames from "classnames";

interface HstackProps {
    className?: string;
    children: ReactNode;
}
export function HStack(props: HstackProps) {
    const { className, children } = props;
    return <div className={classNames("hstack", className)}>{children}</div>;
}

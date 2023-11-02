import React, { ReactNode } from "react";
import classNames from "classnames";

interface VstackProps {
    className?: string;
    children: ReactNode;
}
export function VStack(props: VstackProps) {
    const { className, children } = props;
    return <div className={classNames("vstack", className)}>{children}</div>;
}

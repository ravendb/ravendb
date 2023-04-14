import React, { ReactNode } from "react";
import { Icon } from "./Icon";

interface EmptySetProps {
    children?: ReactNode | ReactNode[];
}

export function EmptySet(props: EmptySetProps) {
    const { children } = props;

    return (
        <div className="text-center mb-2">
            <Icon icon="empty-set" color="muted" className="icon-xl"></Icon>
            <div className="lead">{children}</div>
        </div>
    );
}

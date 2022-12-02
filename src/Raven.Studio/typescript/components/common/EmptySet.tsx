import React, { ReactNode } from "react";

interface EmptySetProps {
    children?: ReactNode | ReactNode[];
}

export function EmptySet(props: EmptySetProps) {
    const { children } = props;

    return (
        <div className="text-center mb-2">
            <i className="icon-xl icon-empty-set text-muted"></i>
            <div className="lead">{children}</div>
        </div>
    );
}

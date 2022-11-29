import React, { ReactNode } from "react";

interface HrHeaderProps {
    children?: ReactNode | ReactNode[];
    right?: ReactNode | ReactNode[];
}

export function HrHeader(props: HrHeaderProps) {
    const { right, children } = props;

    return (
        <div className="flex-horizontal align-items-center my-4">
            {children && <h5 className="m-0 me-3 text-uppercase">{children}</h5>}
            <hr className="flex-grow-1 m-0" />
            {right && <div className="ms-3">{right}</div>}
        </div>
    );
}

import React, { ReactNode } from "react";
import classNames from "classnames";
import { Badge } from "reactstrap";

import "./HrHeader.scss";

interface HrHeaderProps {
    children?: ReactNode | ReactNode[];
    right?: ReactNode | ReactNode[];
    className?: string;
    count?: number;
    margin?: string;
}

export function HrHeader(props: HrHeaderProps) {
    const { right, children, className, count, margin, ...rest } = props;

    return (
        <div className={classNames("hstack align-items-center", margin ? margin : "my-3")}>
            {children && (
                <h5 className={classNames("m-0 me-3 fw-semibold", className)} {...rest}>
                    {children}
                </h5>
            )}
            {count > 0 && (
                <Badge pill className="me-3" color="secondary">
                    {count}
                </Badge>
            )}
            <hr className="flex-grow-1 m-0" />
            {right && <div className="ms-3">{right}</div>}
        </div>
    );
}

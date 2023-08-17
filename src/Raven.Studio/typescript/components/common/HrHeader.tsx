import React, { ReactNode } from "react";
import classNames from "classnames";
import { Badge, UncontrolledTooltip } from "reactstrap";

import "./HrHeader.scss";

interface HrHeaderProps {
    children?: ReactNode | ReactNode[];
    right?: ReactNode | ReactNode[];
    className?: string;
    count?: ReactNode | ReactNode[];
    limit?: number;
}

export function HrHeader(props: HrHeaderProps) {
    const { right, children, className, count, limit, ...rest } = props;
    const maxThreshold = 8;
    const limitThreshold = Math.min(Math.ceil(Number(count) * 0.2), maxThreshold);
    const countLimit = Math.abs(Number(count) - limit) <= limitThreshold;

    return (
        <div className="flex-horizontal align-items-center my-3">
            {children && (
                <h5 className={classNames("m-0 me-3 text-uppercase", { className })} {...rest}>
                    {children}
                </h5>
            )}
            {count !== 0 && (
                <Badge
                    pill
                    className="me-3"
                    id={limit ? "limitLabel" : null}
                    color={count === limit ? "warning" : "secondary"}
                >
                    {count}
                    {limit && countLimit && <span> / {limit}</span>}
                </Badge>
            )}
            <hr className="flex-grow-1 m-0" />
            {right && <div className="ms-3">{right}</div>}
            {limit && countLimit && (
                <UncontrolledTooltip target="limitLabel" placement="top">
                    The limit for your current license is <strong>{limit}</strong>
                </UncontrolledTooltip>
            )}
        </div>
    );
}

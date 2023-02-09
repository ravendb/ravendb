import React, { ReactNode } from "react";

import "./PropSummary.scss";
import classNames from "classnames";

interface PropSummaryProps {
    children: ReactNode | ReactNode[];
    className?: string;
}

export function PropSummary(props: PropSummaryProps) {
    const { children, className } = props;
    return <div className={classNames("prop-summary", className)}>{children}</div>;
}

export function PropSummaryItem(props: PropSummaryProps) {
    const { children, className } = props;
    return <div className={classNames("prop-summary-item", className)}>{children}</div>;
}

export function PropSummaryName(props: PropSummaryProps) {
    const { children, className } = props;
    return <div className={classNames("prop-summary-name", className)}>{children}</div>;
}

interface PropSummaryValueProps {
    children: ReactNode | ReactNode[];
    color?: string;
    className?: string;
}

export function PropSummaryValue(props: PropSummaryValueProps) {
    const { children, color, className } = props;
    const colorClass = color ? "text-" + color : null;
    return (
        <div className={classNames("prop-summary-value", colorClass, className)}>
            <strong>{children}</strong>
        </div>
    );
}

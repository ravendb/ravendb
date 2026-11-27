import React from "react";
import classNames from "classnames";
import "./LoadingDots.scss";

interface LoadingDotsProps {
    label: string;
    className?: string;
}

export function LoadingDots({ label, className }: LoadingDotsProps) {
    return (
        <span role="status" aria-label={label} className={classNames("loading-dots", className)}>
            <span />
            <span />
            <span />
        </span>
    );
}

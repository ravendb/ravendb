import React, { HTMLAttributes, ReactNode } from "react";

import classNames from "classnames";
import { LazyLoad } from "./LazyLoad";
import { Icon } from "./Icon";

import "./LocationDistribution.scss";

interface DistributionItemProps extends HTMLAttributes<HTMLDivElement> {
    loading?: boolean;
}

export function DistributionItem(props: DistributionItemProps) {
    const { loading, children, className, ...rest } = props;
    return (
        <LazyLoad active={loading ?? false}>
            <div className={classNames("distribution-item", className)} {...rest}>
                {children}
            </div>
        </LazyLoad>
    );
}

export function DistributionSummary(props: { children: ReactNode }) {
    const { children } = props;
    return <div className="distribution-summary">{children}</div>;
}

export function DistributionLegend(props: { children: ReactNode }) {
    const { children } = props;
    return <div className="distribution-legend">{children}</div>;
}

export function LocationDistribution(props: { children: ReactNode }) {
    return <div className="location-distribution">{props.children}</div>;
}

interface ClickableProgressProps {
    onClick?: () => void;
    children: ReactNode;
}

export function ClickableProgress({ onClick, children }: ClickableProgressProps) {
    return (
        <div className="clickable-progress">
            {children}
            {/* decorative only - the progress indicator above carries the same onClick and is keyboard/AT accessible */}
            <div
                className={classNames("clickable-progress-hint", onClick ? "cursor-pointer" : "invisible")}
                onClick={onClick}
                aria-hidden
            >
                <Icon icon="preview" margin="me-1" />
                See details
            </div>
        </div>
    );
}

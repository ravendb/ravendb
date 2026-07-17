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
    if (!onClick) {
        return children;
    }
    return (
        <div className="clickable-progress">
            {children}
            <div className="clickable-progress-hint cursor-pointer" onClick={onClick}>
                <Icon icon="preview" margin="me-1" />
                See details
            </div>
        </div>
    );
}

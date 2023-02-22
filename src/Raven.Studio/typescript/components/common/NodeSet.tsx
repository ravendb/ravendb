import React, { ReactNode } from "react";
import classNames from "classnames";
import { Card } from "reactstrap";

import "./NodeSet.scss";

interface NodeSetProps {
    children?: ReactNode | ReactNode[];
    className?: string;
    color?: string;
}

export function NodeSet(props: NodeSetProps) {
    const { children, className, color } = props;

    const colorClass = color ? "bg-faded-" + color : "bg-faded-secondary";

    return <div className={classNames("node-set", colorClass, className)}>{children}</div>;
}

export function NodeSetList(props: { children?: ReactNode | ReactNode[] }) {
    const { children } = props;

    return <div className="node-set-list">{children}</div>;
}

export function NodeSetListCard(props: { children?: ReactNode | ReactNode[] }) {
    const { children } = props;

    return <Card className="node-set-list">{children}</Card>;
}

interface NodeSetItemProps {
    children?: ReactNode | ReactNode[];
    icon?: string;
    color?: string;
    title?: string;
    extraIconClassName?: string;
}

export function NodeSetLabel(props: NodeSetItemProps) {
    const { children, icon, color, ...rest } = props;

    const colorClass = color ? "text-" + color : undefined;

    return (
        <div className="node-set-label align-self-center" {...rest}>
            {icon && <i className={classNames("icon-" + icon, colorClass)} />}
            <strong className="node-set-label-name">{children}</strong>
        </div>
    );
}

export function NodeSetItem(props: NodeSetItemProps) {
    const { children, icon, color, extraIconClassName, ...rest } = props;
    const colorClass = color ? "text-" + color : undefined;

    return (
        <div className="node-set-item" {...rest}>
            {icon && <i className={classNames("icon-" + icon, colorClass, extraIconClassName)} />}
            <strong className="node-set-item-name">{children}</strong>
        </div>
    );
}

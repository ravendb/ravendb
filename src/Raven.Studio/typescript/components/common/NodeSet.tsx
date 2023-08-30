import React, { ReactNode } from "react";
import classNames from "classnames";
import { Card } from "reactstrap";

import "./NodeSet.scss";
import { Icon } from "./Icon";
import IconName from "typings/server/icons";
import { TextColor } from "components/models/common";

interface NodeSetProps {
    children?: ReactNode | ReactNode[];
    className?: string;
    color?: string;
    onClick?: () => void;
    title?: string;
}

export function NodeSet(props: NodeSetProps) {
    const { children, className, color, onClick, title } = props;

    const colorClass = color ? "bg-faded-" + color : null;

    return (
        <div className={classNames("node-set", colorClass, className)} onClick={onClick} title={title}>
            {children}
        </div>
    );
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
    icon?: IconName;
    color?: TextColor;
    title?: string;
    extraIconClassName?: string;
}

export function NodeSetLabel(props: NodeSetItemProps) {
    const { children, icon, color, ...rest } = props;

    const colorClass = color ?? undefined;

    return (
        <div className="node-set-label align-self-center" {...rest}>
            {icon && <Icon icon={icon} color={colorClass} />}
            <strong className="node-set-label-name">{children}</strong>
        </div>
    );
}

export function NodeSetItem(props: NodeSetItemProps) {
    const { children, icon, color, extraIconClassName, ...rest } = props;
    const colorClass = color ?? undefined;

    return (
        <div className="node-set-item" {...rest}>
            {icon && <Icon icon={icon} color={colorClass} className={extraIconClassName} />}
            <strong className="node-set-item-name">{children}</strong>
        </div>
    );
}

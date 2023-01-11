import React, { ReactNode } from "react";
import classNames from "classnames";

interface NodeSetProps {
    children?: ReactNode | ReactNode[];
    className?: string;
    color?: string;
}

export function NodeSet(props: NodeSetProps) {
    const { children, className, color, ...rest } = props;

    return <div className={classNames("node-set rounded-pill", "outline-" + color, className)}>{children}</div>;
}

interface NodeSetItemProps {
    children?: ReactNode | ReactNode[];
    icon?: string;
    color?: string;
}

export function NodeSetLabel(props: NodeSetItemProps) {
    const { children, icon, color } = props;

    return (
        <div className="node-set-label">
            {icon && <i className={classNames("icon-" + icon, "text-" + color)} />}
            <strong className="node-set-label-name">{children}</strong>
        </div>
    );
}

export function NodeSetItem(props: NodeSetItemProps) {
    const { children, icon, color } = props;

    return (
        <div className="node-set-item">
            {icon && <i className={classNames("icon-" + icon, "text-" + color)} />}
            <strong className="node-set-item-name">{children}</strong>
        </div>
    );
}

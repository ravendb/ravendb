import React, { ReactNode } from "react";
import classNames from "classnames";
import "./SelectionActions.scss";

interface SelectionActionsProps {
    active?: boolean;
    children: ReactNode;
}

export function SelectionActions(props: SelectionActionsProps) {
    const { active, children } = props;

    return (
        <div className={classNames("selection-actions", "rounded-pill", "bg-faded-primary", { active: active })}>
            {children}
        </div>
    );
}

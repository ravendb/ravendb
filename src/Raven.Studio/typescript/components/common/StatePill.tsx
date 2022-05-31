import React, { ReactNode } from "react";
import classNames from "classnames";
import assertUnreachable from "../utils/assertUnreachable";

export type StatePillColor = "success" | "danger" | "warning";

interface StatePillProps {
    color: StatePillColor;
    children: ReactNode;
}

import "./StatePill.scss";

function colorClass(color: StatePillColor) {
    switch (color) {
        case "success":
            return "state-pill-success";
        case "danger":
            return "state-pill-danger";
        case "warning":
            return "state-pill-warning";
        default:
            assertUnreachable(color);
    }
}

export function StatePill(props: StatePillProps) {
    const { color, children } = props;

    return <div className={classNames("state-pill", colorClass(color))}>{children}</div>;
}

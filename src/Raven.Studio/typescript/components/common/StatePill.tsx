import React, { ReactNode } from "react";
import classNames from "classnames";

interface StatePillProps {
    color?: string;
    children: ReactNode;
}

import { Badge } from "reactstrap";

export function StatePill(props: StatePillProps) {
    const { color, children } = props;

    return (
        <Badge color={color} className={classNames("rounded-pill", "text-uppercase", "fs-5")}>
            {children}
        </Badge>
    );
}

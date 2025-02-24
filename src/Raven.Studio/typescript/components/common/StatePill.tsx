import React, { ReactNode } from "react";
import classNames from "classnames";

interface StatePillProps {
    bg?: Badge.RavenBadgeBgVariants;
    children: ReactNode;
}

import Badge from "react-bootstrap/Badge";

export function StatePill(props: StatePillProps) {
    const { bg = "secondary", children } = props;

    return (
        <Badge bg={bg} className={classNames("rounded-pill", "text-uppercase", "fs-5")}>
            {children}
        </Badge>
    );
}

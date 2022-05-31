import classNames from "classnames";

export interface ProgressCircleProps {
    state: "success" | "failed" | "running";
    children: ReactNode;
    icon?: string;
    progress?: number | null;
    inline?: boolean;
}

import React, { ReactNode } from "react";
import "./ProgressCircle.scss";

const stateIndicatorProgressRadius = 13;
const circumference = 2 * Math.PI * stateIndicatorProgressRadius;

export function ProgressCircle(props: ProgressCircleProps) {
    const { state, children, inline, icon, progress } = props;

    return (
        <div className={classNames("progress-circle", state, { inline })}>
            <div className="state-desc">
                {progress != null && <strong>{(100 * progress).toFixed(0)}%</strong>}
                {children}
            </div>
            <div className="state-indicator">
                {icon && <i className={icon} />}
                {progress != null && (
                    <svg className="progress-ring">
                        <circle strokeDashoffset={circumference * (1.0 - progress)} />
                    </svg>
                )}
            </div>
        </div>
    );
}

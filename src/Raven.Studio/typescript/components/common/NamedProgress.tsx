import React from "react";
import { ReactNode } from "react";
import classNames from "classnames";

import "./NamedProgress.scss";

export function NamedProgress(props: {
    name: string | ReactNode;
    children: ReactNode | ReactNode[];
    vertical?: boolean;
}) {
    const { name, children, vertical } = props;
    return (
        <div className="named-progress-container">
            <small className="m-auto w-100 text-center small-label">{name}</small>
            <div className={classNames("named-progress", { "named-progress--vertical": vertical })}>{children}</div>
        </div>
    );
}

export function NamedProgressItem(props: {
    children: ReactNode | ReactNode[];
    progress: Progress;
    incomplete?: boolean;
}) {
    const { children, progress, incomplete } = props;
    const progressFormatted = formatPercentage(progress, incomplete);
    const completed = !incomplete && progress.total === progress.processed;
    const remaining = progress.total - progress.processed;
    const title = completed
        ? "Processed all items (" + progress.processed.toLocaleString() + ")"
        : "Processed " +
          progress.processed.toLocaleString() +
          " out of " +
          progress.total.toLocaleString() +
          " (" +
          remaining.toLocaleString() +
          " left)";
    return (
        <div className="progress-item" title={title}>
            <strong className="progress-percentage">{progressFormatted}%</strong>{" "}
            <div className="progress-label">{children}</div>
            <div className="progress">
                <div className={classNames("progress-bar", { completed })} style={{ width: progressFormatted + "%" }} />
            </div>
        </div>
    );
}

function formatPercentage(progress: Progress, incomplete: boolean) {
    const processed = progress.processed;
    const total = progress.total;
    if (total === 0) {
        return incomplete ? 99.9 : 100;
    }

    const result = Math.floor((processed * 100.0) / total);

    return result === 100 && incomplete ? 99.9 : result;
}

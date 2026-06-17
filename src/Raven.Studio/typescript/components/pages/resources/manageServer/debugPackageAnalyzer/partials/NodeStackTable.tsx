import React from "react";
import classNames from "classnames";
import { Row } from "@tanstack/react-table";
import { Icon } from "components/common/Icon";
import NodeTagPill from "./NodeTagPill";
import { MetricRange, formatRange } from "./analyzerUtils";
import "./NodeStackTable.scss";

// Shared building blocks for the node-grouped analyzer tables (Databases Overview, Ongoing Tasks,
// Storage per Database). Each has a parent row per database/task whose per-node rows live in subRows
// and stay collapsed by default, showing an avatar stack until the row is expanded.

// Small, muted chevron shown on expandable parent rows; rotates to indicate the expanded state.
export function ExpandIndicator({ expanded }: { expanded: boolean }) {
    return (
        <Icon
            icon="chevron-right"
            size="sm"
            margin="m-0"
            title={expanded ? "Collapse" : "Expand"}
            className={classNames("node-table-chevron", { expanded })}
        />
    );
}

// Overlapping node avatars (shadcn-style); the first node is rendered on top.
export function NodeTagPillStack({ tags }: { tags: string[] }) {
    return (
        <span className="node-tag-stack">
            {tags.map((tag, index) => (
                <span key={tag} className="node-tag-stack-item" style={{ zIndex: tags.length - index }}>
                    <NodeTagPill tag={tag} />
                </span>
            ))}
        </span>
    );
}

// A parent row is only worth expanding when it has more than one node - with a single node the
// expanded sub-row would just repeat the data already summarized on the parent row.
export function canExpandNodeRow<T extends { subRows?: unknown[] }>(row: Row<T>): boolean {
    return (row.original.subRows?.length ?? 0) > 1;
}

// Renders a replica rollup on a parent row: a single value when the per-node values agree (after
// formatting), or a "min-max" range when they diverge. Diverged ranges get a dotted underline and a
// tooltip hinting that the per-node values are one expand away. Node sub-rows pass an equal min/max
// and so render a plain single value through the same component.
export function RangeCell({ range, format }: { range: MetricRange; format: (value: number) => string }) {
    const diverged = format(range.min) !== format(range.max);
    const text = formatRange(range, format);

    if (!diverged) {
        return <>{text}</>;
    }

    return (
        <span className="node-range-diverged" title="Data differs across nodes - expand for details">
            {text}
        </span>
    );
}

// VirtualTable props that toggle a parent (expandable) row when it is clicked.
export function expandableRowProps<T>() {
    return {
        onRowClick: (row: Row<T>) => {
            if (row.getCanExpand()) {
                row.toggleExpanded();
            }
        },
        getRowClassName: (row: Row<T>) => (row.getCanExpand() ? "clickable-row" : ""),
    };
}

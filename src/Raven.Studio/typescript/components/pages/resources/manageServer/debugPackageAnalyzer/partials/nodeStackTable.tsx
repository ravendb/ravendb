import React from "react";
import classNames from "classnames";
import { Row } from "@tanstack/react-table";
import { Icon } from "components/common/Icon";
import NodeTagPill from "./NodeTagPill";

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

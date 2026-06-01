import React, { useMemo, useState } from "react";
import classNames from "classnames";
import { Icon } from "components/common/Icon";

export type SortDirection = "asc" | "desc";

// Reusable client-side sorting for the debug-package tables. The caller passes a stable accessors
// map (column key -> comparable value); clicking a SortableHeader re-sorts by that column.
export function useSortableData<T>(
    items: T[],
    accessors: Record<string, (item: T) => number | string>,
    initialKey: string,
    initialDirection: SortDirection = "desc"
) {
    const [sortKey, setSortKey] = useState<string>(initialKey);
    const [sortDirection, setSortDirection] = useState<SortDirection>(initialDirection);

    const sorted = useMemo(() => {
        const accessor = accessors[sortKey];
        if (!accessor) {
            return items;
        }
        const factor = sortDirection === "asc" ? 1 : -1;
        return [...items].sort((a, b) => {
            const aValue = accessor(a);
            const bValue = accessor(b);
            if (typeof aValue === "number" && typeof bValue === "number") {
                return (aValue - bValue) * factor;
            }
            return String(aValue).localeCompare(String(bValue)) * factor;
        });
    }, [items, accessors, sortKey, sortDirection]);

    const requestSort = (key: string) => {
        if (key === sortKey) {
            setSortDirection((prev) => (prev === "asc" ? "desc" : "asc"));
        } else {
            setSortKey(key);
            setSortDirection("desc");
        }
    };

    return { sorted, sortKey, sortDirection, requestSort };
}

interface SortableHeaderProps {
    label: string;
    columnKey: string;
    sortKey: string;
    sortDirection: SortDirection;
    onSort: (key: string) => void;
    className?: string;
}

export function SortableHeader({ label, columnKey, sortKey, sortDirection, onSort, className }: SortableHeaderProps) {
    const active = sortKey === columnKey;
    return (
        <th role="button" className={classNames("sortable-header", className)} onClick={() => onSort(columnKey)}>
            <span className="hstack gap-1 align-items-center">
                {label}
                <Icon
                    icon={active && sortDirection === "asc" ? "arrow-thin-top" : "arrow-thin-bottom"}
                    margin="m-0"
                    className={classNames("sortable-header-arrow", { "sortable-header-arrow-inactive": !active })}
                />
            </span>
        </th>
    );
}

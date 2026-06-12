import React from "react";
import { Icon } from "components/common/Icon";

interface SummaryBarItem {
    icon: string;
    iconAddon?: string;
    count: number;
    label: string;
    colorClass?: string;
}

interface SummaryBarProps {
    /** First item appears on the left; remaining items appear grouped on the right. */
    items: [SummaryBarItem, ...SummaryBarItem[]];
}

export default function SummaryBar({ items }: SummaryBarProps) {
    const [main, ...secondary] = items;

    return (
        <div className="hstack justify-content-between flex-wrap gap-2 p-3 panel-header-bg rounded">
            <SummaryBarItem item={main} />
            {secondary.length > 0 && (
                <div className="hstack gap-3 flex-wrap">
                    {secondary.map((item) => (
                        <SummaryBarItem key={item.label} item={item} />
                    ))}
                </div>
            )}
        </div>
    );
}

function SummaryBarItem({ item }: { item: SummaryBarItem }) {
    return (
        <span className={`hstack gap-1 small-label${item.colorClass ? ` ${item.colorClass}` : ""}`}>
            <Icon icon={item.icon as any} addon={item.iconAddon as any} margin="m-0" />
            {item.count.toLocaleString()} {item.label}
        </span>
    );
}

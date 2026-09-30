import { Fragment, useState } from "react";

const COLLAPSED_TABLE_NAMES_COUNT = 5;

export function ExpandableTableNames({ labels }: { labels: string[] }) {
    const [isExpanded, setIsExpanded] = useState(false);

    const visibleLabels = isExpanded ? labels : labels.slice(0, COLLAPSED_TABLE_NAMES_COUNT);
    const hiddenCount = labels.length - visibleLabels.length;

    return (
        <>
            {visibleLabels.map((label, index) => (
                <Fragment key={label}>
                    {index > 0 && ", "}
                    <span className="font-mono">{label}</span>
                </Fragment>
            ))}
            {hiddenCount > 0 && (
                <>
                    {" and "}
                    <ToggleNamesButton onClick={() => setIsExpanded(true)}>{hiddenCount} more</ToggleNamesButton>
                </>
            )}
            {isExpanded && labels.length > COLLAPSED_TABLE_NAMES_COUNT && (
                <>
                    {" "}
                    <ToggleNamesButton onClick={() => setIsExpanded(false)}>(show less)</ToggleNamesButton>
                </>
            )}
        </>
    );
}

function ToggleNamesButton({ onClick, children }: { onClick: () => void; children: React.ReactNode }) {
    return (
        <button type="button" className="font-medium underline underline-offset-2" onClick={onClick}>
            {children}
        </button>
    );
}

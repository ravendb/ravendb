import { Icon } from "components/common/Icon";
import { ReactNode } from "react";
import Button from "react-bootstrap/Button";

interface ExpandableListRenderProps {
    isExpanded: boolean;
    visibleCount: number;
    hiddenCount: number;
}

export interface ExpandableListProps {
    itemsCount: number;
    collapsedItemsCount: number;
    isExpanded: boolean;
    setIsExpanded: (isExpanded: boolean) => void;
    children: (props: ExpandableListRenderProps) => ReactNode;
    expandLabel?: (hiddenCount: number) => ReactNode;
    collapseLabel?: ReactNode;
    className?: string;
    contentClassName?: string;
}

export default function ExpandableList({
    itemsCount,
    collapsedItemsCount,
    isExpanded,
    setIsExpanded,
    children,
    expandLabel = (hiddenCount) => `Show ${hiddenCount} more`,
    collapseLabel = "Show less",
    className,
    contentClassName,
}: ExpandableListProps) {
    const toggle = () => {
        setIsExpanded(!isExpanded);
    };

    const visibleCount = isExpanded ? itemsCount : Math.min(collapsedItemsCount, itemsCount);
    const hiddenCount = itemsCount - visibleCount;
    const canToggle = itemsCount > collapsedItemsCount;

    return (
        <div className={className}>
            <div className={contentClassName}>{children({ isExpanded, visibleCount, hiddenCount })}</div>
            {canToggle && (
                <div className="hstack justify-content-center mt-1">
                    <Button variant="link" size="sm" className="p-0" onClick={toggle}>
                        <Icon icon={isExpanded ? "collapse-vertical" : "expand-vertical"} />
                        {isExpanded ? collapseLabel : expandLabel(hiddenCount)}
                    </Button>
                </div>
            )}
        </div>
    );
}

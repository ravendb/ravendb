import type { ReactNode } from "react";
import { ChevronDown, ChevronUp } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";

type ExpandableListRenderProps = {
    isExpanded: boolean;
    visibleCount: number;
    hiddenCount: number;
};

type ExpandableListProps = {
    itemsCount: number;
    collapsedItemsCount: number;
    isExpanded: boolean;
    setIsExpanded: (isExpanded: boolean) => void;
    children: (props: ExpandableListRenderProps) => ReactNode;
    className?: string;
};

/** Shows only the first `collapsedItemsCount` items until expanded, so long lists don't render
 * an item per entry. The children render prop receives how many items to show; expansion is
 * controlled by the parent so it can expand the list when it appends an item. */
export function ExpandableList({
    itemsCount,
    collapsedItemsCount,
    isExpanded,
    setIsExpanded,
    children,
    className,
}: ExpandableListProps) {
    const visibleCount = isExpanded ? itemsCount : Math.min(collapsedItemsCount, itemsCount);
    const hiddenCount = itemsCount - visibleCount;
    const canToggle = itemsCount > collapsedItemsCount;

    return (
        <div className={className}>
            {children({ isExpanded, visibleCount, hiddenCount })}
            {canToggle && (
                <div className="flex justify-center">
                    <Button
                        type="button"
                        variant="link"
                        className="h-auto gap-1 p-0 text-xs font-medium"
                        onClick={() => setIsExpanded(!isExpanded)}
                        aria-expanded={isExpanded}
                    >
                        {isExpanded ? (
                            <>
                                Show less
                                <ChevronUp />
                            </>
                        ) : (
                            <>
                                Show {hiddenCount} more
                                <ChevronDown />
                            </>
                        )}
                    </Button>
                </div>
            )}
        </div>
    );
}

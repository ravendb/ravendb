import classNames from "classnames";
import ExpandableList, { ExpandableListProps } from "components/common/ExpandableList";
import { Gap, getGapClasses } from "components/common/utilities/stackCommon";
import { useState } from "react";

interface ExpandableListContainerProps<T>
    extends Omit<
        ExpandableListProps,
        "isExpanded" | "setIsExpanded" | "itemsCount" | "children" | "collapsedItemsCount"
    > {
    items: T[];
    renderItem: (item: T) => React.ReactNode;
    collapsedItemsCount?: number;
    gap?: Gap;
}

export default function ExpandableListContainer<T>({
    items,
    renderItem,
    collapsedItemsCount = 2,
    gap = 1,
    ...rest
}: ExpandableListContainerProps<T>) {
    const [isExpanded, setIsExpanded] = useState(false);

    const gapClasses = getGapClasses(gap);

    return (
        <ExpandableList
            itemsCount={items.length}
            collapsedItemsCount={collapsedItemsCount}
            isExpanded={isExpanded}
            setIsExpanded={setIsExpanded}
            {...rest}
        >
            {({ visibleCount }) => (
                <div className={classNames("vstack", gapClasses)}>
                    {items.slice(0, visibleCount).map((item, index) => (
                        <div key={index}>{renderItem(item)}</div>
                    ))}
                </div>
            )}
        </ExpandableList>
    );
}

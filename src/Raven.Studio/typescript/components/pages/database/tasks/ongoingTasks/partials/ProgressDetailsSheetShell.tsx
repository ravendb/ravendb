import { ReactNode, useState } from "react";
import { ViewSheet } from "components/common/splitView/ViewSheet";
import { SheetSlideDirection, SheetSlideTransition } from "components/common/splitView/SheetSlideNavigation";
import { Icon } from "components/common/Icon";
import { NodeLocationTabs } from "components/pages/database/tasks/ongoingTasks/partials/NodeLocationSelect";

export function useNodeSlider(initialNodeIndex: number, onNodeChange?: (index: number) => void) {
    const [selectedIndex, setSelectedIndex] = useState(initialNodeIndex);
    const [direction, setDirection] = useState<SheetSlideDirection>(1);
    const [prevInitialNodeIndex, setPrevInitialNodeIndex] = useState(initialNodeIndex);

    if (initialNodeIndex !== prevInitialNodeIndex) {
        setPrevInitialNodeIndex(initialNodeIndex);
        setDirection(initialNodeIndex > selectedIndex ? 1 : -1);
        setSelectedIndex(initialNodeIndex);
    }

    const handleNodeChange = (index: number) => {
        setDirection(index > selectedIndex ? 1 : -1);
        setSelectedIndex(index);
        onNodeChange?.(index);
    };

    return { selectedIndex, direction, handleNodeChange };
}

interface ProgressDetailsSheetShellProps {
    className?: string;
    title: ReactNode;
    locations: { nodeTag: string; shardNumber?: number }[];
    selectedIndex: number;
    direction: SheetSlideDirection;
    onNodeChange: (index: number) => void;
    children: ReactNode;
}

export function ProgressDetailsSheetShell({
    className,
    title,
    locations,
    selectedIndex,
    direction,
    onNodeChange,
    children,
}: ProgressDetailsSheetShellProps) {
    return (
        <ViewSheet className={className}>
            <ViewSheet.Header isPinHidden isCloseHidden className="pb-0">
                <div className="vstack gap-2 w-100">
                    <div className="d-flex justify-content-between align-items-center">
                        <div className="d-flex align-items-center gap-1">
                            <Icon icon="ongoing-tasks" margin="me-0" color="primary" />
                            <h4 className="mb-0">{title} details</h4>
                        </div>
                        <div className="d-flex align-items-center">
                            <ViewSheet.PinButton />
                            <ViewSheet.CloseButton />
                        </div>
                    </div>
                    <div className="d-flex align-items-center">
                        <NodeLocationTabs locations={locations} selectedIndex={selectedIndex} onChange={onNodeChange} />
                    </div>
                </div>
            </ViewSheet.Header>
            <ViewSheet.Body className="p-3">
                <SheetSlideTransition currentIndex={selectedIndex} direction={direction}>
                    {children}
                </SheetSlideTransition>
            </ViewSheet.Body>
        </ViewSheet>
    );
}

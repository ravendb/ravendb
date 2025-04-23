import React, { useCallback, useState } from "react";
import Button from "react-bootstrap/Button";
import {
    NodeInfoReorderComponent,
    NodeInfoReorderPreview,
} from "components/pages/resources/manageDatabaseGroup/partials/NodeInfoComponent";
import { NodeInfo } from "components/models/databases";
import { DatabaseGroup, DatabaseGroupList } from "components/common/DatabaseGroup";
import { Icon } from "components/common/Icon";
import { RadioToggleWithIcon, RadioToggleWithIconInputItem } from "components/common/toggles/RadioToggle";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import {
    closestCenter,
    DndContext,
    DndContextProps,
    DragOverlay,
    KeyboardSensor,
    PointerSensor,
    useSensor,
    useSensors,
} from "@dnd-kit/core";
import {
    horizontalListSortingStrategy,
    SortableContext,
    sortableKeyboardCoordinates,
    arrayMove,
} from "@dnd-kit/sortable";

interface ReorderNodesControlsProps {
    sortableMode: boolean;
    canSort: boolean;
    enableReorder: () => void;
    cancelReorder: () => void;
    onSave: () => Promise<void>;
}

export function ReorderNodesControls(props: ReorderNodesControlsProps) {
    const { canSort, sortableMode, enableReorder, cancelReorder, onSave } = props;
    const [saving, setSaving] = useState(false);

    const onSaveClicked = async () => {
        setSaving(true);
        try {
            await onSave();
        } finally {
            setSaving(false);
        }
    };

    return !sortableMode ? (
        <Button variant="secondary" disabled={canSort} onClick={enableReorder}>
            <Icon icon="reorder" />
            Reorder nodes
        </Button>
    ) : (
        <>
            <ButtonWithSpinner variant="success" onClick={onSaveClicked} isSpinning={saving} icon="save">
                Save reorder
            </ButtonWithSpinner>
            <Button variant="secondary" onClick={cancelReorder}>
                <Icon icon="cancel" />
                <span>Cancel</span>
            </Button>
        </>
    );
}

interface ReorderNodesProps {
    fixOrder: boolean;
    setFixOrder: (fixOrder: React.SetStateAction<boolean>) => void;
    newOrder: NodeInfo[];
    setNewOrder: (newOrder: React.SetStateAction<NodeInfo[]>) => void;
}

export function ReorderNodes(props: ReorderNodesProps) {
    const { fixOrder, setFixOrder, newOrder, setNewOrder } = props;

    const { activeNode, sensors, handleDragStart, handleDragEnd, handleDragCancel } = useReorderNodes({
        newOrder,
        setNewOrder,
    });

    const leftRadioToggleItem: RadioToggleWithIconInputItem = {
        label: (
            <>
                Shuffle nodes order
                <br />
                after failure recovery
            </>
        ),
        value: "shuffle",
        iconName: "shuffle",
    };

    const rightRadioToggleItem: RadioToggleWithIconInputItem = {
        label: "Try to maintain nodes order",
        value: "order",
        iconName: "order",
    };

    const radioToggleSelectedItem = fixOrder ? rightRadioToggleItem.value : leftRadioToggleItem.value;

    const newOrderWithId = newOrder.map((node) => ({
        ...node,
        id: node.tag,
    }));

    return (
        <div>
            <div className="px-3 pt-3">
                <RadioToggleWithIcon
                    name="after-recovery"
                    leftItem={leftRadioToggleItem}
                    rightItem={rightRadioToggleItem}
                    selectedValue={radioToggleSelectedItem}
                    setSelectedValue={(x) => setFixOrder(x !== leftRadioToggleItem.value)}
                />
            </div>
            <DatabaseGroup>
                <DatabaseGroupList>
                    <DndContext
                        sensors={sensors}
                        collisionDetection={closestCenter}
                        onDragStart={handleDragStart}
                        onDragEnd={handleDragEnd}
                        onDragCancel={handleDragCancel}
                    >
                        <SortableContext strategy={horizontalListSortingStrategy} items={newOrderWithId}>
                            {newOrder.map((node) => (
                                <NodeInfoReorderComponent key={node.tag} node={node} />
                            ))}
                        </SortableContext>
                        <DragOverlay adjustScale>
                            {activeNode ? <NodeInfoReorderPreview node={activeNode} /> : null}
                        </DragOverlay>
                    </DndContext>
                </DatabaseGroupList>
            </DatabaseGroup>
        </div>
    );
}

interface UseReorderNodesProps {
    newOrder: NodeInfo[];
    setNewOrder: (newOrder: React.SetStateAction<NodeInfo[]>) => void;
}

function useReorderNodes({ newOrder, setNewOrder }: UseReorderNodesProps) {
    const [activeNode, setActiveNode] = useState<NodeInfo | null>(null);

    const sensors = useSensors(useSensor(PointerSensor));

    const handleDragStart: DndContextProps["onDragStart"] = (event) => {
        const { active } = event;
        const activeNode = newOrder.find((node) => node.tag === active.id);
        setActiveNode(activeNode || null);
    };

    const handleDragEnd: DndContextProps["onDragEnd"] = (event) => {
        const { active, over } = event;

        if (active.id !== over.id) {
            const oldIndex = newOrder.findIndex((node) => node.tag === active.id);
            const newIndex = newOrder.findIndex((node) => node.tag === over.id);

            setNewOrder((items) => arrayMove(items, oldIndex, newIndex));
        }

        setActiveNode(null);
    };

    const handleDragCancel = () => {
        setActiveNode(null);
    };

    return {
        activeNode,
        sensors,
        handleDragStart,
        handleDragEnd,
        handleDragCancel,
    };
}

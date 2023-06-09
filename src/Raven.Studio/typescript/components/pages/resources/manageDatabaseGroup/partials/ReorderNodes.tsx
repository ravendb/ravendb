import React, { useCallback, useState } from "react";
import { Button } from "reactstrap";
import { NodeInfoReorderComponent } from "components/pages/resources/manageDatabaseGroup/partials/NodeInfoComponent";
import { useDrop } from "react-dnd";
import { NodeInfo } from "components/models/databases";
import { DatabaseGroup, DatabaseGroupList } from "components/common/DatabaseGroup";
import { Icon } from "components/common/Icon";
import { RadioToggleWithIcon, RadioToggleWithIconInputItem } from "components/common/RadioToggle";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";

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
        <Button disabled={canSort} onClick={enableReorder}>
            <Icon icon="reorder" />
            Reorder nodes
        </Button>
    ) : (
        <>
            <ButtonWithSpinner color="success" onClick={onSaveClicked} isSpinning={saving} icon="save">
                Save reorder
            </ButtonWithSpinner>
            <Button onClick={cancelReorder}>
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

    const [, drop] = useDrop(() => ({ accept: "node" }));

    const findCardIndex = useCallback((node: NodeInfo) => newOrder.findIndex((x) => x.tag === node.tag), [newOrder]);

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

    return (
        <div ref={drop}>
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
                    {newOrder.map((node) => (
                        <NodeInfoReorderComponent
                            key={node.tag}
                            node={node}
                            setOrder={setNewOrder}
                            findCardIndex={findCardIndex}
                        />
                    ))}
                </DatabaseGroupList>
            </DatabaseGroup>
        </div>
    );
}

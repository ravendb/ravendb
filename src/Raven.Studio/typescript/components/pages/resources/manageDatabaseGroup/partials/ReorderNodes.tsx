import React, { useCallback, useState } from "react";
import { Button, Spinner } from "reactstrap";
import { NodeInfoReorderComponent } from "components/pages/resources/manageDatabaseGroup/partials/NodeInfoComponent";
import { useDrop } from "react-dnd";
import { NodeInfo } from "components/models/databases";
import { DatabaseGroup, DatabaseGroupList } from "components/common/DatabaseGroup";
import { Icon } from "components/common/Icon";
import { RadioToggleWithIcon, RadioToggleWithIconInputItem } from "components/common/RadioToggle";

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
        <Button disabled={canSort} onClick={enableReorder} className="me-2">
            <Icon icon="reorder" /> Reorder nodes
        </Button>
    ) : (
        <>
            <Button color="success" onClick={onSaveClicked} disabled={saving}>
                {saving ? <Spinner size="sm" /> : <Icon icon="save" />}
                <span>Save reorder</span>
            </Button>
            <Button onClick={cancelReorder} className="ms-1">
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

    const radioToggleSelectedItem = fixOrder ? rightRadioToggleItem : leftRadioToggleItem;

    return (
        <div ref={drop}>
            <div className="px-3 pt-3">
                <RadioToggleWithIcon
                    name="after-recovery"
                    leftItem={leftRadioToggleItem}
                    rightItem={rightRadioToggleItem}
                    selectedItem={radioToggleSelectedItem}
                    setSelectedItem={(x) => setFixOrder(x !== leftRadioToggleItem)}
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

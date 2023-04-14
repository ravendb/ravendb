import React, { useCallback, useState } from "react";
import { Button, Spinner } from "reactstrap";
import { NodeInfoReorderComponent } from "components/pages/resources/manageDatabaseGroup/partials/NodeInfoComponent";
import { useDrop } from "react-dnd";
import { NodeInfo } from "components/models/databases";
import { DatabaseGroup, DatabaseGroupList } from "components/common/DatabaseGroup";
import { Radio } from "components/common/Checkbox";
import { Icon } from "components/common/Icon";

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
            <Icon icon="reorder" className="me-1" /> Reorder nodes
        </Button>
    ) : (
        <>
            <Button color="success" onClick={onSaveClicked} disabled={saving}>
                {saving ? <Spinner size="sm" /> : <Icon icon="save" className="me-1" />}
                <span>Save reorder</span>
            </Button>
            <Button onClick={cancelReorder} className="ms-1">
                <Icon icon="cancel" className="me-1" />
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

    return (
        <div ref={drop}>
            <div className="d-flex px-3 pt-3">
                <div className="me-2">After failure recovery:</div>
                <Radio selected={!fixOrder} toggleSelection={() => setFixOrder(false)} className="me-2">
                    Shuffle nodes order
                </Radio>
                <Radio selected={fixOrder} toggleSelection={() => setFixOrder(true)}>
                    Try to maintain nodes order
                </Radio>
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

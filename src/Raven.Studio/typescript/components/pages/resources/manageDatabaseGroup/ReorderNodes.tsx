import React, { useCallback, useState } from "react";
import { Button } from "reactstrap";
import { NodeInfoReorderComponent } from "components/pages/resources/manageDatabaseGroup/NodeInfoComponent";
import { useDrop } from "react-dnd";
import { NodeInfo } from "components/models/databases";
import { DatabaseGroup, DatabaseGroupList } from "components/common/DatabaseGroup";
import { Radio } from "components/common/Checkbox";

interface ReorderNodesControllsProps {
    sortableMode: boolean;
    canSort: boolean;
    enableReorder: () => void;
    cancelReorder: () => void;
    onSave: () => Promise<void>;
}

export function ReorderNodesControlls(props: ReorderNodesControllsProps) {
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
            <i className="icon-reorder me-1" /> Reorder nodes
        </Button>
    ) : (
        <>
            <Button color="success" onClick={onSaveClicked}>
                <i className="icon-save" />
                <span>Save reorder</span>
            </Button>
            <Button onClick={cancelReorder} className="ms-1">
                <i className="icon-cancel" />
                <span>Cancel</span>
            </Button>
        </>
    );
}

interface ReorderNodesProps {
    nodes: NodeInfo[]; // TODO is this necesarry?
    fixOrder: boolean;
    setFixOrder: (fixOrder: React.SetStateAction<boolean>) => void;
    newOrder: NodeInfo[];
    setNewOrder: (newOrder: React.SetStateAction<NodeInfo[]>) => void;
}

export function ReorderNodes(props: ReorderNodesProps) {
    const { nodes, fixOrder, setFixOrder, newOrder, setNewOrder } = props;

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

import React, { useCallback, useEffect, useState } from "react";
import { Button } from "reactstrap";
import classNames from "classnames";
import { NodeInfoReorderComponent } from "components/pages/resources/manageDatabaseGroup/NodeInfoComponent";
import { useDrop } from "react-dnd";
import { NodeInfo } from "components/models/databases";
import {
    DatabaseGroup,
    DatabaseGroupActions,
    DatabaseGroupItem,
    DatabaseGroupList,
    DatabaseGroupNode,
} from "components/common/DatabaseGroup";

interface ReorderNodesControllsProps {
    sortableMode: boolean;
    canSort: boolean;
    enableReorder: () => void;
    cancelReorder: () => void;
    finishReorder: () => void;
}

export function ReorderNodesControlls(props: ReorderNodesControllsProps) {
    const { canSort, sortableMode, enableReorder, cancelReorder, finishReorder } = props;
    return !sortableMode ? (
        <Button disabled={canSort} onClick={enableReorder} className="me-2">
            <i className="icon-reorder me-1" /> Reorder nodes
        </Button>
    ) : (
        <>
            <Button color="success" onClick={finishReorder}>
                <i className="icon-save" />
                <span>Save</span>
            </Button>
            <Button onClick={cancelReorder} className="ms-1">
                <i className="icon-cancel" />
                <span>Cancel</span>
            </Button>
        </>
    );
}

interface ReorderNodesProps {
    cancelReorder: () => void;
    saveReorder: boolean;
    nodes: NodeInfo[];
    saveNewOrder: (order: string[], fixOrder: boolean) => Promise<void>;
}

export function ReorderNodes(props: ReorderNodesProps) {
    const { saveReorder, cancelReorder, nodes, saveNewOrder } = props;

    const [fixOrder, setFixOrder] = useState(false);
    const [newOrder, setNewOrder] = useState<NodeInfo[]>(nodes);

    const [, drop] = useDrop(() => ({ accept: "node" }));

    const findCardIndex = useCallback((node: NodeInfo) => newOrder.findIndex((x) => x.tag === node.tag), [newOrder]);

    useEffect(() => {
        console.log(saveReorder, "- Has changed");
        saveNewOrder(
            newOrder.map((x) => x.tag),
            fixOrder
        );
    }, [saveReorder === true]);

    return (
        <div ref={drop}>
            <div>
                <div>Drag elements to set their order. Click &quot;Save&quot; when finished.</div>
                <Button
                    color="primary"
                    onClick={() =>
                        saveNewOrder(
                            newOrder.map((x) => x.tag),
                            fixOrder
                        )
                    }
                >
                    <i className="icon-save" />
                    <span>Save</span>
                </Button>
                <Button onClick={cancelReorder}>
                    <i className="icon-cancel" />
                    <span>Cancel</span>
                </Button>
            </div>
            <div className="flex-form">
                <div className="form-group">
                    <label className="control-label">After failure recovery</label>
                    <div>
                        <div className="btn-group">
                            <Button className={classNames({ active: !fixOrder })} onClick={() => setFixOrder(false)}>
                                Shuffle nodes order
                            </Button>
                            <Button onClick={() => setFixOrder(true)} className={classNames({ active: fixOrder })}>
                                Try to maintain nodes order
                            </Button>
                        </div>
                    </div>
                </div>
            </div>
            <DatabaseGroup>
                <DatabaseGroupList>
                    <DatabaseGroupItem className="item-new">
                        <DatabaseGroupNode icon="node-add" color="success" />
                        <DatabaseGroupActions>
                            <Button size="xs" color="success" outline className="rounded-pill" disabled={true}>
                                <i className="icon-plus me-1" />
                                Add node
                            </Button>
                        </DatabaseGroupActions>
                    </DatabaseGroupItem>
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

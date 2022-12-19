import React, { useCallback, useState } from "react";
import { Button } from "reactstrap";
import { NodeInfo } from "components/pages/resources/manageDatabaseGroup/types";
import classNames from "classnames";
import { NodeInfoReorderComponent } from "components/pages/resources/manageDatabaseGroup/NodeInfoComponent";
import { useDrop } from "react-dnd";
import { produce } from "immer";

interface ReorderNodesProps {
    cancelReorder: () => void;
    nodes: NodeInfo[];
    saveNewOrder: (order: string[], fixOrder: boolean) => Promise<void>;
}

export function ReorderNodes(props: ReorderNodesProps) {
    const { cancelReorder, nodes, saveNewOrder } = props;

    const [fixOrder, setFixOrder] = useState(false);
    const [newOrder, setNewOrder] = useState<NodeInfo[]>(nodes);

    const [, drop] = useDrop(() => ({ accept: "node" }));

    const findCard = useCallback(
        (tag: string) => {
            const cardIdx = newOrder.findIndex((x) => x.tag === tag);
            if (cardIdx === -1) {
                throw new Error("Unable to find card with tag = " + tag);
            }
            return {
                card: newOrder[cardIdx],
                index: cardIdx,
            };
        },
        [newOrder]
    );

    const moveCard = useCallback(
        (tag: string, atIndex: number) => {
            const { card, index } = findCard(tag);

            setNewOrder(() => {
                return produce(newOrder, (draft) => {
                    draft.splice(index, 1);
                    draft.splice(atIndex, 0, card);
                });
            });
        },
        [findCard, newOrder]
    );

    return (
        <div ref={drop}>
            <div className="sticky-header">
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

            {newOrder.map((node) => (
                <NodeInfoReorderComponent key={node.tag} node={node} moveCard={moveCard} findCard={findCard} />
            ))}
        </div>
    );
}

import React, { useCallback, useEffect } from "react";
import { NodeSet, NodeSetLabel, NodeSetItem, NodeSetList } from "./NodeSet";
import { Checkbox } from "./Checkbox";
import { Label, UncontrolledTooltip } from "reactstrap";
import { Icon } from "./Icon";
import classNames from "classnames";
import { produce } from "immer";
import ActionContextUtils from "components/utils/actionContextUtils";

export type DatabaseActionContexts =
    | {
          nodeTag: string;
          shardNumbers: number[];
          includeOrchestrator?: boolean;
      }
    | {
          nodeTag: string;
          shardNumbers?: never;
          includeOrchestrator?: never;
      };

interface MultipleDatabaseLocationSelectorProps {
    allActionContexts: DatabaseActionContexts[];
    selectedActionContexts: DatabaseActionContexts[];
    setSelectedActionContexts: React.Dispatch<React.SetStateAction<DatabaseActionContexts[]>>;
    className?: string;
}

export function MultipleDatabaseLocationSelector(props: MultipleDatabaseLocationSelectorProps) {
    const { allActionContexts, selectedActionContexts, setSelectedActionContexts, className } = props;

    const isSharded: boolean = ActionContextUtils.isSharded(allActionContexts);

    const getSelectedContextsNode = useCallback(
        (nodeTag: string) => selectedActionContexts?.find((x) => x.nodeTag === nodeTag),
        [selectedActionContexts]
    );

    const isNodeSelected = useCallback(
        (nodeTag: string): boolean => !!getSelectedContextsNode(nodeTag),
        [getSelectedContextsNode]
    );

    const isOrchestratorSelected = (nodeTag: string): boolean => {
        return !!getSelectedContextsNode(nodeTag)?.includeOrchestrator;
    };

    const isShardSelected = useCallback(
        (nodeTag: string, shardNumber: number): boolean => {
            return isNodeSelected(nodeTag) && !!getSelectedContextsNode(nodeTag).shardNumbers?.includes(shardNumber);
        },
        [getSelectedContextsNode, isNodeSelected]
    );

    const isAllNodeItemsSelected = (nodeTag: string): boolean => {
        const contextNode = getSelectedContextsNode(nodeTag);

        if (!contextNode) {
            return false;
        }

        if (!isSharded) {
            return true;
        }

        const initialContextNode = allActionContexts.find((x) => x.nodeTag === nodeTag);

        return (
            contextNode.includeOrchestrator === initialContextNode.includeOrchestrator &&
            _.isEqual([...contextNode.shardNumbers].sort(), [...initialContextNode.shardNumbers].sort())
        );
    };

    const isAllSelected: boolean = allActionContexts.every((x) => isAllNodeItemsSelected(x.nodeTag));
    const isSomeSelected: boolean = !isAllSelected && selectedActionContexts.length > 0;

    const isNodeIndeterminate = (nodeTag: string): boolean => {
        return isNodeSelected(nodeTag) && !isAllNodeItemsSelected(nodeTag);
    };

    const toggleAll = () => {
        if (selectedActionContexts.length === 0) {
            setSelectedActionContexts(allActionContexts);
        } else {
            setSelectedActionContexts([]);
        }
    };

    const toggleNode = (nodeTag: string) => {
        if (isNodeSelected(nodeTag)) {
            setSelectedActionContexts((contexts) => contexts.filter((x) => x.nodeTag !== nodeTag));
        } else {
            setSelectedActionContexts((contexts) => [
                ...contexts,
                allActionContexts.find((x) => x.nodeTag === nodeTag),
            ]);
        }
    };

    const toggleShard = (nodeTag: string, shardNumber: number) => {
        if (isShardSelected(nodeTag, shardNumber)) {
            setSelectedActionContexts((prevState) =>
                produce(prevState, (draft) => {
                    const contextNode = draft.find((x) => x.nodeTag === nodeTag);
                    contextNode.shardNumbers = contextNode.shardNumbers.filter((x) => x !== shardNumber);
                })
            );
        } else {
            setSelectedActionContexts((prevState) =>
                produce(prevState, (draft) => {
                    const contextNode = draft.find((x) => x.nodeTag === nodeTag);

                    if (contextNode) {
                        contextNode.shardNumbers = contextNode.shardNumbers.filter((x) => x !== shardNumber);
                        draft.find((x) => x.nodeTag === nodeTag).shardNumbers.push(shardNumber);
                    } else {
                        draft.push({
                            nodeTag,
                            shardNumbers: [shardNumber],
                        });
                    }
                })
            );
        }
    };

    const toggleOrchestrator = (nodeTag: string) => {
        if (isOrchestratorSelected(nodeTag)) {
            setSelectedActionContexts((prevState) =>
                produce(prevState, (draft) => {
                    draft.find((x) => x.nodeTag === nodeTag).includeOrchestrator = false;
                })
            );
        } else {
            setSelectedActionContexts((prevState) =>
                produce(prevState, (draft) => {
                    const contextNode = draft.find((x) => x.nodeTag === nodeTag);

                    if (contextNode) {
                        draft.find((x) => x.nodeTag === nodeTag).includeOrchestrator = true;
                    } else {
                        draft.push({
                            nodeTag,
                            includeOrchestrator: true,
                            shardNumbers: [],
                        });
                    }
                })
            );
        }
    };

    useEffect(() => {
        if (!isSharded) {
            return;
        }

        selectedActionContexts.forEach((selectedContext) => {
            if (selectedContext.shardNumbers.length === 0 && !selectedContext.includeOrchestrator) {
                setSelectedActionContexts((contexts) => contexts.filter((x) => x.nodeTag !== selectedContext.nodeTag));
            }
        });
    }, [isSharded, selectedActionContexts, setSelectedActionContexts]);

    return (
        <>
            {!isSharded ? (
                <NodeSet className={classNames(className)}>
                    <NodeSetLabel>
                        <Checkbox
                            size="lg"
                            toggleSelection={toggleAll}
                            indeterminate={isSomeSelected}
                            selected={isAllSelected}
                            title="Select all or none"
                        />
                    </NodeSetLabel>
                    <NodeSetList>
                        {allActionContexts.map(({ nodeTag }) => {
                            const uniqueKey = getUniqueKey(nodeTag);
                            return (
                                <NodeSetItem key={nodeTag}>
                                    <Label htmlFor={uniqueKey} title={"Node " + nodeTag}>
                                        <Icon icon="node" color="node" />
                                        {nodeTag}
                                        <div className="d-flex justify-content-center">
                                            <Checkbox
                                                id={uniqueKey}
                                                toggleSelection={() => toggleNode(nodeTag)}
                                                selected={isNodeSelected(nodeTag)}
                                            />
                                        </div>
                                    </Label>
                                </NodeSetItem>
                            );
                        })}
                    </NodeSetList>
                </NodeSet>
            ) : (
                <>
                    <Checkbox
                        size="lg"
                        toggleSelection={toggleAll}
                        indeterminate={isSomeSelected}
                        selected={isAllSelected}
                        title="Select all or none"
                    >
                        <span className="small-label">
                            {isAllSelected ? "Reset selection" : isSomeSelected ? "Deselect all" : "Select all"}
                        </span>
                    </Checkbox>

                    {allActionContexts.map(({ nodeTag, shardNumbers, includeOrchestrator: includeOrchestrator }) => {
                        const uniqueKey = getUniqueKey(nodeTag);
                        return (
                            <div key={uniqueKey}>
                                <NodeSet className={classNames(className, "mt-1")}>
                                    <NodeSetLabel>
                                        <Label htmlFor={uniqueKey} className="text-node" title={"Node " + nodeTag}>
                                            <Icon icon="node" />
                                            {nodeTag}
                                            <div className="d-flex justify-content-center">
                                                <Checkbox
                                                    id={uniqueKey}
                                                    indeterminate={isNodeIndeterminate(nodeTag)}
                                                    toggleSelection={() => toggleNode(nodeTag)}
                                                    selected={isNodeSelected(nodeTag)}
                                                />
                                            </div>
                                        </Label>
                                    </NodeSetLabel>
                                    <div className="node-set-separator" />
                                    <NodeSetList>
                                        {includeOrchestrator && (
                                            <NodeSetItem>
                                                <Label
                                                    htmlFor={getUniqueKey(nodeTag, true)}
                                                    id={getUniqueKey(nodeTag, true) + "Tooltip"}
                                                    title="Orchestrator"
                                                >
                                                    <Icon icon="orchestrator" color="orchestrator" className="ms-1" />
                                                    <div className="d-flex justify-content-center">
                                                        <Checkbox
                                                            id={getUniqueKey(nodeTag, true)}
                                                            selected={isOrchestratorSelected(nodeTag)}
                                                            toggleSelection={() => toggleOrchestrator(nodeTag)}
                                                            color="orchestrator"
                                                        />
                                                    </div>
                                                </Label>
                                                <UncontrolledTooltip
                                                    placement="top"
                                                    target={getUniqueKey(nodeTag, true) + "Tooltip"}
                                                >
                                                    Orchestrator
                                                </UncontrolledTooltip>
                                            </NodeSetItem>
                                        )}

                                        {shardNumbers.length > 0 &&
                                            shardNumbers.map((shardNumber) => {
                                                if (shardNumber == null) {
                                                    return null;
                                                }

                                                const uniqueKey = getUniqueKey(nodeTag, false, shardNumber);

                                                return (
                                                    <NodeSetItem key={uniqueKey}>
                                                        <Label htmlFor={uniqueKey} title={"Shard " + shardNumber}>
                                                            <Icon icon="shard" color="shard" />
                                                            {shardNumber}
                                                            <div className="d-flex justify-content-center">
                                                                <Checkbox
                                                                    selected={isShardSelected(nodeTag, shardNumber)}
                                                                    toggleSelection={() =>
                                                                        toggleShard(nodeTag, shardNumber)
                                                                    }
                                                                    color="shard"
                                                                    id={uniqueKey}
                                                                />
                                                            </div>
                                                        </Label>
                                                    </NodeSetItem>
                                                );
                                            })}
                                    </NodeSetList>
                                </NodeSet>
                            </div>
                        );
                    })}
                </>
            )}
        </>
    );
}

function getUniqueKey(nodeTag: string, isOrchestrator?: boolean, shardNumber?: number) {
    return (
        "location-selector-" +
        nodeTag +
        (isOrchestrator ? "-orchestrator" : "") +
        (shardNumber > -1 ? `-${shardNumber}` : "")
    );
}

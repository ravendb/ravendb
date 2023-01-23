import React, { useCallback } from "react";
import {
    Badge,
    Button,
    DropdownItem,
    DropdownMenu,
    DropdownToggle,
    UncontrolledDropdown,
    UncontrolledTooltip,
} from "reactstrap";
import genUtils from "common/generalUtils";
import assertUnreachable from "components/utils/assertUnreachable";
import app from "durandal/app";
import showDataDialog from "viewmodels/common/showDataDialog";
import useId from "hooks/useId";
import classNames from "classnames";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import { useDraggableItem } from "hooks/useDraggableItem";
import { NodeInfo } from "components/models/databases";
import appUrl from "common/appUrl";
import {
    DatabaseGroup,
    DatabaseGroupActions,
    DatabaseGroupError,
    DatabaseGroupItem,
    DatabaseGroupList,
    DatabaseGroupNode,
    DatabaseGroupType,
} from "components/common/DatabaseGroup";

interface OrchestratorInfoComponentProps {
    node: NodeInfo;
    deleteFromGroup: (nodeTag: string) => void;
}

export function OrchestratorInfoComponent(props: OrchestratorInfoComponentProps) {
    const { node, deleteFromGroup } = props;

    return (
        <DatabaseGroupItem>
            <DatabaseGroupNode>{node.tag}</DatabaseGroupNode>
            <DatabaseGroupType node={node} />
            <DatabaseGroupActions>
                <Button
                    size="xs"
                    color="danger"
                    outline
                    className="rounded-pill"
                    onClick={() => deleteFromGroup(node.tag)}
                >
                    <i className="icon-cancel" /> Remove
                </Button>
            </DatabaseGroupActions>
            <DatabaseGroupError node={node} />
        </DatabaseGroupItem>
    );
}

interface NodeInfoComponentProps {
    node: NodeInfo;
    databaseLockMode: DatabaseLockMode;
    deleteFromGroup: (nodeTag: string, hardDelete: boolean) => void;
}

export function NodeInfoComponent(props: NodeInfoComponentProps) {
    const { node, databaseLockMode, deleteFromGroup } = props;

    const deleteLockId = useId("delete-lock");

    const canDelete = databaseLockMode === "Unlock";

    return (
        <DatabaseGroupItem>
            <DatabaseGroupNode>{node.tag}</DatabaseGroupNode>

            <DatabaseGroupType node={node} />
            <DatabaseGroupActions>
                {node.responsibleNode && (
                    <div
                        className="text-center"
                        title="Database group node that is responsible for caught up of this node"
                    >
                        <i className="icon-cluster-node"></i>
                        <span>{node.responsibleNode}</span>
                    </div>
                )}
                {canDelete ? (
                    <UncontrolledDropdown key="can-delete">
                        <DropdownToggle color="danger" caret outline size="xs" className="rounded-pill">
                            <i className="icon-disconnected" />
                            Delete from group
                        </DropdownToggle>
                        <DropdownMenu>
                            <DropdownItem onClick={() => deleteFromGroup(node.tag, false)}>
                                <i className="icon-stop" />
                                <span>Soft Delete</span>&nbsp;
                                <br />
                                <small>stop replication and keep database files on the node</small>
                            </DropdownItem>
                            <DropdownItem onClick={() => deleteFromGroup(node.tag, true)}>
                                <i className="icon-trash text-danger"></i>{" "}
                                <span className="text-danger">Hard Delete</span>
                                <br />
                                &nbsp;<small>stop replication and remove database files on the node</small>
                            </DropdownItem>
                        </DropdownMenu>
                    </UncontrolledDropdown>
                ) : (
                    <React.Fragment key="cannot-delete">
                        <UncontrolledDropdown id={deleteLockId}>
                            <DropdownToggle color="danger" caret disabled size="xs" className="rounded-pill">
                                <i
                                    className={classNames("icon-trash", {
                                        "icon-addon-exclamation": databaseLockMode === "PreventDeletesError",
                                        "icon-addon-cancel": databaseLockMode === "PreventDeletesIgnore",
                                    })}
                                />
                                Delete from group
                            </DropdownToggle>
                        </UncontrolledDropdown>
                        <UncontrolledTooltip target={deleteLockId} placeholder="top" color="danger">
                            Database cannot be deleted from node because of the set lock mode
                        </UncontrolledTooltip>
                    </React.Fragment>
                )}
            </DatabaseGroupActions>
            <DatabaseGroupError node={node} />
        </DatabaseGroupItem>
    );
}

interface ShardInfoComponentProps {
    node: NodeInfo;
    shardName: string;
    databaseLockMode: DatabaseLockMode;
    deleteFromGroup: (nodeTag: string, hardDelete: boolean) => void;
}

export function ShardInfoComponent(props: ShardInfoComponentProps) {
    const { node, databaseLockMode, deleteFromGroup, shardName } = props;

    const deleteLockId = useId("delete-lock");

    const canDelete = databaseLockMode === "Unlock";

    const documentsUrl = appUrl.forDocuments(null, shardName);
    const debugUrl = appUrl.toExternalUrl(node.nodeUrl, documentsUrl);

    return (
        <DatabaseGroupItem>
            <DatabaseGroupNode>{node.tag}</DatabaseGroupNode>
            <DatabaseGroupType node={node} />
            <DatabaseGroupActions>
                <UncontrolledDropdown key="advanced">
                    <DropdownToggle caret outline size="xs" color="secondary" className="rounded-pill">
                        <i className="icon-debug-advanced" />
                        Advanced
                    </DropdownToggle>
                    <DropdownMenu>
                        <DropdownItem href={debugUrl} target="_blank">
                            Debug this shard
                        </DropdownItem>
                    </DropdownMenu>
                </UncontrolledDropdown>
                {node.responsibleNode && (
                    <div
                        className="text-center"
                        title="Database group node that is responsible for caught up of this node"
                    >
                        <i className="icon-cluster-node"></i>
                        <span>{node.responsibleNode}</span>
                    </div>
                )}
                {canDelete ? (
                    <UncontrolledDropdown key="can-delete" className="mt-1">
                        <DropdownToggle color="danger" caret outline size="xs" className="rounded-pill">
                            <i className="icon-disconnected" />
                            Delete from group
                        </DropdownToggle>
                        <DropdownMenu>
                            <DropdownItem onClick={() => deleteFromGroup(node.tag, false)}>
                                <i className="icon-trash" />
                                <span>Soft Delete</span>&nbsp;
                                <br />
                                <small>stop replication and keep database files on the node</small>
                            </DropdownItem>
                            <DropdownItem onClick={() => deleteFromGroup(node.tag, true)}>
                                <i className="icon-alerts text-danger"></i>{" "}
                                <span className="text-danger">Hard Delete</span>
                                <br />
                                &nbsp;<small>stop replication and remove database files on the node</small>
                            </DropdownItem>
                        </DropdownMenu>
                    </UncontrolledDropdown>
                ) : (
                    <React.Fragment key="cannot-delete">
                        <UncontrolledDropdown id={deleteLockId} className="mt-1">
                            <DropdownToggle color="danger" caret disabled outline size="xs" className="rounded-pill">
                                <i
                                    className={classNames("icon-trash-cutout", {
                                        "icon-addon-exclamation": databaseLockMode === "PreventDeletesError",
                                        "icon-addon-cancel": databaseLockMode === "PreventDeletesIgnore",
                                    })}
                                />
                                Delete from group
                            </DropdownToggle>
                        </UncontrolledDropdown>
                        <UncontrolledTooltip target={deleteLockId} placeholder="top" color="danger">
                            Database cannot be deleted from node because of the set lock mode
                        </UncontrolledTooltip>
                    </React.Fragment>
                )}
            </DatabaseGroupActions>

            <DatabaseGroupError node={node} />
        </DatabaseGroupItem>
    );
}

interface NodeInfoReorderComponentProps {
    node: NodeInfo;
    findCardIndex: (node: NodeInfo) => number;
    setOrder: (action: (state: NodeInfo[]) => NodeInfo[]) => void;
}

const tagExtractor = (node: NodeInfo) => node.tag;

export function NodeInfoReorderComponent(props: NodeInfoReorderComponentProps) {
    const { node, setOrder, findCardIndex } = props;

    const { drag, drop, isDragging } = useDraggableItem("node", node, tagExtractor, findCardIndex, setOrder);

    const opacity = isDragging ? 0.5 : 1;

    return (
        <div ref={(node) => drag(drop(node))} style={{ opacity }}>
            <DatabaseGroupItem className="item-reorder">
                <DatabaseGroupNode>{node.tag}</DatabaseGroupNode>

                <DatabaseGroupType node={node} />
                <DatabaseGroupActions>
                    {node.responsibleNode && (
                        <div
                            className="text-center"
                            title="Database group node that is responsible for caught up of this node"
                        >
                            <i className="icon-cluster-node"></i>
                            <span>{node.responsibleNode}</span>
                        </div>
                    )}
                </DatabaseGroupActions>
            </DatabaseGroupItem>
        </div>
    );
}

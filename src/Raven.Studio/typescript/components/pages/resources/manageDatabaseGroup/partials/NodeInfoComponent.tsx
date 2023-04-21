import React from "react";
import {
    Button,
    DropdownItem,
    DropdownMenu,
    DropdownToggle,
    UncontrolledDropdown,
    UncontrolledTooltip,
} from "reactstrap";
import useId from "hooks/useId";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import { useDraggableItem } from "hooks/useDraggableItem";
import { NodeInfo } from "components/models/databases";
import appUrl from "common/appUrl";
import {
    DatabaseGroupActions,
    DatabaseGroupError,
    DatabaseGroupItem,
    DatabaseGroupNode,
    DatabaseGroupType,
} from "components/common/DatabaseGroup";
import { Icon } from "components/common/Icon";

interface OrchestratorInfoComponentProps {
    node: NodeInfo;
    canDelete: boolean;
    deleteFromGroup: (nodeTag: string) => void;
}

export function OrchestratorInfoComponent(props: OrchestratorInfoComponentProps) {
    const { node, deleteFromGroup, canDelete } = props;

    return (
        <DatabaseGroupItem>
            <DatabaseGroupNode>{node.tag}</DatabaseGroupNode>
            <DatabaseGroupType node={node} />
            <DatabaseGroupActions>
                <Button
                    size="xs"
                    color="danger"
                    outline
                    disabled={!canDelete}
                    className="rounded-pill"
                    onClick={() => deleteFromGroup(node.tag)}
                >
                    <Icon icon="cancel" /> Remove
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
                {canDelete ? (
                    <UncontrolledDropdown key="can-delete">
                        <DropdownToggle color="danger" caret outline size="xs" className="rounded-pill">
                            <Icon icon="disconnected" />
                            Delete from group
                        </DropdownToggle>
                        <DropdownMenu>
                            <DropdownItem onClick={() => deleteFromGroup(node.tag, false)}>
                                <Icon icon="disconnected" />
                                <span>Soft Delete</span>&nbsp;
                                <br />
                                <small>stop replication and keep database files on the node</small>
                            </DropdownItem>
                            <DropdownItem onClick={() => deleteFromGroup(node.tag, true)}>
                                <Icon icon="trash" color="danger" /> <span className="text-danger">Hard Delete</span>
                                <br />
                                &nbsp;<small>stop replication and remove database files on the node</small>
                            </DropdownItem>
                        </DropdownMenu>
                    </UncontrolledDropdown>
                ) : (
                    <React.Fragment key="cannot-delete">
                        <UncontrolledDropdown id={deleteLockId}>
                            <DropdownToggle color="danger" caret disabled size="xs" className="rounded-pill">
                                {databaseLockMode === "PreventDeletesError" && (
                                    <Icon icon="trash" addon="exclamation" />
                                )}
                                {databaseLockMode === "PreventDeletesIgnore" && <Icon icon="trash" addon="cancel" />}
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
    databaseName: string;
    databaseLockMode: DatabaseLockMode;
    deleteFromGroup: (nodeTag: string, hardDelete: boolean) => void;
}

export function ShardInfoComponent(props: ShardInfoComponentProps) {
    const { node, databaseLockMode, deleteFromGroup, databaseName } = props;

    const deleteLockId = useId("delete-lock");

    const canDelete = databaseLockMode === "Unlock";

    const documentsUrl = appUrl.forDocuments(null, databaseName);
    const debugUrl = appUrl.toExternalUrl(node.nodeUrl, documentsUrl);

    return (
        <DatabaseGroupItem>
            <DatabaseGroupNode>{node.tag}</DatabaseGroupNode>
            <DatabaseGroupType node={node} />
            <DatabaseGroupActions>
                <UncontrolledDropdown key="advanced">
                    <DropdownToggle caret outline size="xs" color="secondary" className="rounded-pill">
                        <Icon icon="debug-advanced" />
                        Advanced
                    </DropdownToggle>
                    <DropdownMenu>
                        <DropdownItem href={debugUrl} target="_blank">
                            Debug this shard
                        </DropdownItem>
                    </DropdownMenu>
                </UncontrolledDropdown>
                {canDelete ? (
                    <UncontrolledDropdown key="can-delete" className="mt-1">
                        <DropdownToggle color="danger" caret outline size="xs" className="rounded-pill">
                            <Icon icon="disconnected" />
                            Delete from group
                        </DropdownToggle>
                        <DropdownMenu>
                            <DropdownItem onClick={() => deleteFromGroup(node.tag, false)}>
                                <Icon icon="trash" />
                                <span>Soft Delete</span>&nbsp;
                                <br />
                                <small>stop replication and keep database files on the node</small>
                            </DropdownItem>
                            <DropdownItem onClick={() => deleteFromGroup(node.tag, true)}>
                                <Icon icon="alerts" color="danger" /> <span className="text-danger">Hard Delete</span>
                                <br />
                                &nbsp;<small>stop replication and remove database files on the node</small>
                            </DropdownItem>
                        </DropdownMenu>
                    </UncontrolledDropdown>
                ) : (
                    <React.Fragment key="cannot-delete">
                        <UncontrolledDropdown id={deleteLockId} className="mt-1">
                            <DropdownToggle color="danger" caret disabled outline size="xs" className="rounded-pill">
                                {databaseLockMode === "PreventDeletesError" && (
                                    <Icon icon="trash" addon="exclamation" />
                                )}
                                {databaseLockMode === "PreventDeletesIgnore" && <Icon icon="trash" addon="cancel" />}
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
    setOrder: (order: React.SetStateAction<NodeInfo[]>) => void;
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
            </DatabaseGroupItem>
        </div>
    );
}

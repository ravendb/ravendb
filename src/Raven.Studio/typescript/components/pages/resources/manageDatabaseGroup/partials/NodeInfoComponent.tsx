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
import { useDraggableItem } from "hooks/useDraggableItem";
import { DatabaseSharedInfo, NodeInfo } from "components/models/databases";
import appUrl from "common/appUrl";
import {
    DatabaseGroupActions,
    DatabaseGroupError,
    DatabaseGroupItem,
    DatabaseGroupNode,
    DatabaseGroupType,
} from "components/common/DatabaseGroup";
import { Icon } from "components/common/Icon";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import useConfirm from "components/common/ConfirmDialog";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAppSelector } from "components/store";

interface OrchestratorInfoComponentProps {
    node: NodeInfo;
    deleteFromGroup: (nodeTag: string) => void;
    canDelete: boolean;
}

interface PromoteButtonProps {
    databaseName: string;
    nodeTag: string;
}

function PromoteButton({ databaseName, nodeTag }: PromoteButtonProps) {
    const { databasesService } = useServices();
    const asyncPromoteImmediately = useAsyncCallback(() => databasesService.promoteDatabaseNode(databaseName, nodeTag));

    const confirm = useConfirm();

    const promote = async () => {
        const isConfirmed = await confirm({
            title: `Do you want to promote node ${nodeTag} to become a member?`,
            icon: "promote",
            actionColor: "primary",
            confirmText: "Promote",
        });

        if (isConfirmed) {
            await asyncPromoteImmediately.execute();
        }
    };

    return (
        <ButtonWithSpinner
            className="rounded-pill justify-content-center"
            title="Promote to become a member"
            icon="promote"
            size="sm"
            onClick={promote}
            isSpinning={asyncPromoteImmediately.status === "loading"}
        >
            Promote
        </ButtonWithSpinner>
    );
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
    deleteFromGroup: (nodeTag: string, hardDelete: boolean) => Promise<void>;
    db: DatabaseSharedInfo;
}

export function NodeInfoComponent(props: NodeInfoComponentProps) {
    const { node, db, deleteFromGroup } = props;

    const deleteLockId = useId("delete-lock");
    const isOperatorOrAbove = useAppSelector(accessManagerSelectors.isOperatorOrAbove);

    const canPromote = isOperatorOrAbove && node.type === "Promotable";
    const canDelete = db.lockMode === "Unlock";

    return (
        <DatabaseGroupItem>
            <DatabaseGroupNode>{node.tag}</DatabaseGroupNode>

            <DatabaseGroupType node={node} />
            <DatabaseGroupActions>
                {canPromote && <PromoteButton databaseName={db.name} nodeTag={node.tag} />}
                {canDelete ? (
                    <UncontrolledDropdown key="can-delete">
                        <DropdownToggle color="danger" caret outline size="xs" className="rounded-pill">
                            <Icon icon="disconnected" />
                            Delete from group
                        </DropdownToggle>
                        <DropdownMenu>
                            <DropdownItem
                                onClick={() => deleteFromGroup(node.tag, false)}
                                className="d-flex flex-row align-items-center gap-1"
                            >
                                <Icon icon="disconnected" />
                                <div className="d-flex flex-column">
                                    <span className="lh-1">Soft Delete</span>
                                    <small className="text-muted">
                                        Stop replication and keep database files on the node
                                    </small>
                                </div>
                            </DropdownItem>
                            <DropdownItem
                                onClick={() => deleteFromGroup(node.tag, true)}
                                className="d-flex flex-row align-items-center gap-1"
                            >
                                <Icon icon="trash" color="danger" />
                                <div className="d-flex flex-column">
                                    <span className="text-danger lh-1">Hard Delete</span>
                                    <small className="text-muted">
                                        Stop replication and remove database files on the node
                                    </small>
                                </div>
                            </DropdownItem>
                        </DropdownMenu>
                    </UncontrolledDropdown>
                ) : (
                    <React.Fragment key="cannot-delete">
                        <UncontrolledDropdown id={deleteLockId}>
                            <DropdownToggle color="danger" caret disabled size="xs" className="rounded-pill">
                                {db.lockMode === "PreventDeletesError" && <Icon icon="trash" addon="exclamation" />}
                                {db.lockMode === "PreventDeletesIgnore" && <Icon icon="trash" addon="cancel" />}
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
    deleteFromGroup: (nodeTag: string, hardDelete: boolean) => void;
    db: DatabaseSharedInfo;
}

export function ShardInfoComponent(props: ShardInfoComponentProps) {
    const { node, deleteFromGroup, db } = props;

    const deleteLockId = useId("delete-lock");
    const isOperatorOrAbove = useAppSelector(accessManagerSelectors.isOperatorOrAbove);

    const canDelete = db.lockMode === "Unlock";

    const documentsUrl = appUrl.forDocuments(null, db.name);
    const debugUrl = appUrl.toExternalUrl(node.nodeUrl, documentsUrl);

    const canPromote = isOperatorOrAbove && node.type === "Promotable";

    return (
        <DatabaseGroupItem>
            <DatabaseGroupNode>{node.tag}</DatabaseGroupNode>
            <DatabaseGroupType node={node} />
            <DatabaseGroupActions>
                {canPromote && <PromoteButton databaseName={db.name} nodeTag={node.tag} />}
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
                    <UncontrolledDropdown key="can-delete">
                        <DropdownToggle color="danger" caret outline size="xs" className="rounded-pill">
                            <Icon icon="disconnected" />
                            Delete from group
                        </DropdownToggle>
                        <DropdownMenu>
                            <DropdownItem
                                onClick={() => deleteFromGroup(node.tag, false)}
                                className="d-flex flex-row align-items-center gap-1"
                            >
                                <Icon icon="trash" />
                                <div className="d-flex flex-column">
                                    <span className="lh-1">Soft Delete</span>
                                    <small className="text-muted">
                                        Stop replication and keep database files on the node
                                    </small>
                                </div>
                            </DropdownItem>
                            <DropdownItem
                                onClick={() => deleteFromGroup(node.tag, true)}
                                className="d-flex flex-row align-items-center gap-1"
                            >
                                <Icon icon="alerts" color="danger" />
                                <div className="d-flex flex-column">
                                    <span className="text-danger lh-1">Hard Delete</span>
                                    <small className="text-muted">
                                        Stop replication and remove database files on the node
                                    </small>
                                </div>
                            </DropdownItem>
                        </DropdownMenu>
                    </UncontrolledDropdown>
                ) : (
                    <React.Fragment key="cannot-delete">
                        <UncontrolledDropdown id={deleteLockId}>
                            <DropdownToggle color="danger" caret disabled outline size="xs" className="rounded-pill">
                                {db.lockMode === "PreventDeletesError" && <Icon icon="trash" addon="exclamation" />}
                                {db.lockMode === "PreventDeletesIgnore" && <Icon icon="trash" addon="cancel" />}
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

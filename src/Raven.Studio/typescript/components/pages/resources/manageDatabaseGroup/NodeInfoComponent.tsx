import React, { useCallback } from "react";
import { DropdownItem, DropdownMenu, DropdownToggle, UncontrolledDropdown, UncontrolledTooltip } from "reactstrap";
import { NodeInfo } from "components/pages/resources/manageDatabaseGroup/types";
import viewHelpers from "common/helpers/view/viewHelpers";
import genUtils from "common/generalUtils";
import database from "models/resources/database";
import { useServices } from "hooks/useServices";
import assertUnreachable from "components/utils/assertUnreachable";
import app from "durandal/app";
import showDataDialog from "viewmodels/common/showDataDialog";
import useId from "hooks/useId";
import classNames from "classnames";
import { FlexGrow } from "components/common/FlexGrow";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import {
    RichPanel,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelName,
    RichPanelStatus,
} from "components/common/RichPanel";

interface NodeInfoComponentProps {
    node: NodeInfo;
    db: database;
    databaseLockMode: DatabaseLockMode;
}

export function NodeInfoComponent(props: NodeInfoComponentProps) {
    const { node, db, databaseLockMode } = props;

    const { databasesService } = useServices();

    const deleteLockId = useId("delete-lock");

    const canDelete = databaseLockMode === "Unlock";
    const lastErrorShort = node.lastError ? genUtils.trimMessage(node.lastError) : null;

    const deleteNodeFromGroup = useCallback(
        (hardDelete: boolean) => {
            const nodeTag = node.tag;

            viewHelpers
                .confirmationMessage(
                    "Are you sure",
                    "Do you want to delete database '" + genUtils.escapeHtml(db.name) + "' from node: " + nodeTag + "?",
                    {
                        buttons: ["Cancel", "Yes, delete"],
                        html: true,
                    }
                )
                .done((result) => {
                    if (result.can) {
                        // noinspection JSIgnoredPromiseFromCall
                        databasesService.deleteDatabaseFromNode(db, [nodeTag], hardDelete);
                    }
                });
        },
        [node, db, databasesService]
    );

    const showErrorsDetails = useCallback(() => {
        app.showBootstrapDialog(new showDataDialog("Error details. Node: " + node.tag, node.lastError, "plain"));
    }, [node]);

    return (
        <RichPanel className="flex-row">
            <RichPanelStatus color={nodeBadgeColor(node)}>{nodeBadgeText(node)}</RichPanelStatus>

            <div className="flex-grow-1">
                <RichPanelHeader>
                    <RichPanelName title={node.type}>
                        <i className={classNames(cssIcon(node), "me-1")} />
                        Node: {node.tag}
                    </RichPanelName>

                    <FlexGrow />

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
                            <DropdownToggle color="danger" caret>
                                <i className="icon-disconnected" />
                                Delete from group
                            </DropdownToggle>
                            <DropdownMenu>
                                <DropdownItem onClick={() => deleteNodeFromGroup(false)}>
                                    <i className="icon-trash" />
                                    <span>Soft Delete</span>&nbsp;
                                    <br />
                                    <small>stop replication and keep database files on the node</small>
                                </DropdownItem>
                                <DropdownItem onClick={() => deleteNodeFromGroup(true)}>
                                    <i className="icon-alerts text-danger"></i>{" "}
                                    <span className="text-danger">Hard Delete</span>
                                    <br />
                                    &nbsp;<small>stop replication and remove database files on the node</small>
                                </DropdownItem>
                            </DropdownMenu>
                        </UncontrolledDropdown>
                    ) : (
                        <React.Fragment key="cannot-delete">
                            <UncontrolledDropdown id={deleteLockId}>
                                <DropdownToggle color="danger" caret disabled>
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
                </RichPanelHeader>
                {lastErrorShort && (
                    <RichPanelDetails>
                        <RichPanelDetailItem
                            label={
                                <>
                                    <i className="icon-warning me-1" />
                                    Error
                                </>
                            }
                        >
                            <a className="link" onClick={showErrorsDetails}>
                                {lastErrorShort}
                            </a>
                        </RichPanelDetailItem>
                    </RichPanelDetails>
                )}
            </div>
        </RichPanel>
    );
}

function nodeBadgeColor(node: NodeInfo) {
    switch (node.lastStatus) {
        case "Ok":
            return "success";
        case "NotResponding":
            return "danger";
        default:
            return "warning";
    }
}

function cssIcon(node: NodeInfo) {
    const type = node.type;

    switch (type) {
        case "Member":
            return "icon-dbgroup-member";
        case "Promotable":
            return "icon-dbgroup-promotable";
        case "Rehab":
            return "icon-dbgroup-rehab";
        default:
            assertUnreachable(type);
    }
}

function nodeBadgeText(node: NodeInfo) {
    switch (node.lastStatus) {
        case "Ok":
            return "Active";
        case "NotResponding":
            return "Error";
        default:
            return "Catching up";
    }
}

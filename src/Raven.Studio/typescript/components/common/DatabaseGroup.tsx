import React, { HTMLAttributes, ReactNode, useCallback } from "react";

import classNames from "classnames";
import { NodeInfo } from "components/models/databases";
import app from "durandal/app";
import showDataDialog from "viewmodels/common/showDataDialog";
import { Badge } from "reactstrap";
import genUtils from "common/generalUtils";
import assertUnreachable from "components/utils/assertUnreachable";

import "./DatabaseGroup.scss";
import { Icon } from "./Icon";
import IconName from "typings/server/icons";
import { TextColor } from "components/models/common";

interface DatabaseGroupProps extends HTMLAttributes<HTMLDivElement> {
    children?: ReactNode | ReactNode[];
    className?: string;
}

export function DatabaseGroup(props: DatabaseGroupProps) {
    const { children, className } = props;
    return <div className={classNames("dbgroup", className)}>{children}</div>;
}

export function DatabaseGroupList(props: { children?: ReactNode | ReactNode[] }) {
    const { children } = props;
    return <div className="dbgroup-list">{children}</div>;
}

interface DatabaseGroupItemProps extends HTMLAttributes<HTMLDivElement> {
    children?: ReactNode | ReactNode[];
    className?: string;
}

export function DatabaseGroupItem(props: DatabaseGroupItemProps) {
    const { children, className } = props;
    return <div className={classNames("dbgroup-item", className)}>{children}</div>;
}

interface DatabaseGroupNodeProps extends HTMLAttributes<HTMLDivElement> {
    children?: ReactNode | ReactNode[];
    icon?: IconName;
    color?: TextColor;
}

export function DatabaseGroupNode(props: DatabaseGroupNodeProps) {
    const { children, icon, color } = props;
    const nodeIcon = icon ?? "node";
    const nodeColor = color ?? "node";
    return (
        <div className="dbgroup-node">
            <Icon icon={nodeIcon} color={nodeColor} /> {children ? <strong>{children}</strong> : null}
        </div>
    );
}

interface DatabaseGroupTypeProps extends HTMLAttributes<HTMLDivElement> {
    node?: NodeInfo;
    children?: ReactNode | ReactNode[];
}

export function DatabaseGroupType(props: DatabaseGroupTypeProps) {
    const { node, children } = props;

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

    function cssIcon(node: NodeInfo): IconName {
        const type = node.type;

        switch (type) {
            case "Member":
                return "dbgroup-member";
            case "Promotable":
                return "dbgroup-promotable";
            case "Rehab":
                return "dbgroup-rehab";
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

    return (
        <div className="dbgroup-type">
            <div title={node.type} className="mb-1">
                <Icon icon={cssIcon(node)} /> {node.type}
            </div>
            <div>
                <Badge color={nodeBadgeColor(node)}>
                    {nodeBadgeText(node)}
                    {node.responsibleNode && (
                        <span
                            className="ms-1"
                            title="Database group node that is responsible for caught up of this node"
                        >
                            <Icon icon="node" margin="m-0" />
                            <strong className="text-reset"> {node.responsibleNode}</strong>
                        </span>
                    )}
                </Badge>
            </div>
            {children}
        </div>
    );
}

export function DatabaseGroupActions(props: { children?: ReactNode | ReactNode[] }) {
    const { children } = props;
    return <div className="dbgroup-actions">{children}</div>;
}

interface DatabaseGroupErrorProps extends HTMLAttributes<HTMLDivElement> {
    node: NodeInfo;
}

export function DatabaseGroupError(props: DatabaseGroupErrorProps) {
    const { node } = props;
    const lastErrorShort = node.lastError ? genUtils.trimMessage(node.lastError) : null;

    const showErrorsDetails = useCallback(() => {
        app.showBootstrapDialog(new showDataDialog("Error details for Node " + node.tag, node.lastError, "plain"));
    }, [node]);

    return lastErrorShort ? (
        <div className="dbgroup-error position-relative">
            <div className="text-danger">
                <Icon icon="warning" /> Error
            </div>

            <small className="d-flex flex-column">
                {lastErrorShort}
                <a className="link stretched-link ms-2" title="Click to see error details" onClick={showErrorsDetails}>
                    Error Details <Icon icon="info" margin="m-0" />
                </a>
            </small>
            <div></div>
        </div>
    ) : null;
}

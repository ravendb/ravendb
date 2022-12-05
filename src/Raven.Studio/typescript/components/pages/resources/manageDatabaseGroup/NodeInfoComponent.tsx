import React from "react";
import {
    Alert,
    Badge,
    Card,
    CardBody,
    CardHeader,
    DropdownItem,
    DropdownMenu,
    DropdownToggle,
    UncontrolledDropdown,
} from "reactstrap";
import { NodeInfo } from "components/pages/resources/manageDatabaseGroup/types";

interface NodeInfoComponentProps {
    node: NodeInfo;
}

export function NodeInfoComponent(props: NodeInfoComponentProps) {
    const { node } = props;
    return (
        <Card>
            <CardHeader>
                <Badge color={nodeBadgeColor(node)}>status: {nodeBadgeText(node)}</Badge>
                Node: {node.tag}
                <UncontrolledDropdown>
                    <DropdownToggle color="danger" caret>
                        <i className="icon-disconnected" />
                        Delete from group
                    </DropdownToggle>
                    <DropdownMenu>
                        <DropdownItem data-bind="click: _.partial($parent.deleteNodeFromGroup, $data, false)">
                            <i className="icon-trash" />
                            <span>Soft Delete</span>&nbsp;
                            <small>(stop replication and keep database files on the node)</small>
                        </DropdownItem>
                        <DropdownItem data-bind="click: _.partial($parent.deleteNodeFromGroup, $data, true)">
                            <i className="icon-alerts text-danger"></i> <span className="text-danger">Hard Delete</span>
                            &nbsp;<small>(stop replication and remove database files on the node)</small>
                        </DropdownItem>
                    </DropdownMenu>
                </UncontrolledDropdown>
            </CardHeader>
            <CardBody></CardBody>
        </Card>
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

/* TODO
 cssIcon = ko.pureComputed(() => {
        const type = this.type();
        switch (type) {
            case "Member":
                return "icon-dbgroup-member";
            case "Promotable":
                return "icon-dbgroup-promotable";
            case "Rehab":
                return "icon-dbgroup-rehab";
        }
        return "";
    });
 */

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

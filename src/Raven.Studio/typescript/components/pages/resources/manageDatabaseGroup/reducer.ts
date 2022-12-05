import { DatabaseSharedInfo } from "components/models/databases";
import { Reducer } from "react";
import DatabasePromotionStatus = Raven.Client.ServerWide.DatabasePromotionStatus;
import NodeId = Raven.Client.ServerWide.Operations.NodeId;
import DatabaseGroupNodeStatus = Raven.Client.ServerWide.Operations.DatabaseGroupNodeStatus;
import {
    DatabaseInfoLoaded,
    ManageDatabaseGroupState,
    NodeInfo,
} from "components/pages/resources/manageDatabaseGroup/types";

/* TODO node tag
    lastErrorShort = ko.pureComputed(() => {
        const longError = this.lastError();
        return generalUtils.trimMessage(longError);
    });

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

    static for(tag: string, serverUrl: string, responsibleNode: string, type: databaseGroupNodeType) {
        const node = new databaseGroupNode();
        node.tag(tag);
        node.serverUrl(serverUrl);
        node.responsibleNode(responsibleNode);
        node.type(type);
        return node;
    }
    
    update(incoming: databaseGroupNode) {
        this.tag(incoming.tag());
        this.serverUrl(incoming.serverUrl());
        this.type(incoming.type());
        this.responsibleNode(incoming.responsibleNode());
        this.lastStatus(incoming.lastStatus());
        this.lastError(incoming.lastError());
    }
 */

type ManageDatabaseGroupReducerAction = DatabaseInfoLoaded;

function mapNode(
    node: NodeId,
    type: databaseGroupNodeType,
    statusSource: Record<string, DatabaseGroupNodeStatus>
): NodeInfo {
    const matchingStatus = statusSource?.[node.NodeTag];

    return {
        tag: node.NodeTag,
        serverUrl: node.NodeUrl,
        responsibleNode: node.ResponsibleNode,
        type,
        lastError: matchingStatus?.LastError,
        lastStatus: matchingStatus?.LastStatus,
    };
}

export const manageDatabaseGroupReducer: Reducer<ManageDatabaseGroupState, ManageDatabaseGroupReducerAction> = (
    state: ManageDatabaseGroupState,
    action: ManageDatabaseGroupReducerAction
): ManageDatabaseGroupState => {
    switch (action.type) {
        case "DatabaseInfoLoaded": {
            const topology = action.info.NodesTopology;
            const mappedMembers = topology.Members.map((x) => mapNode(x, "Member", topology.Status));
            const mappedPromotables = topology.Promotables.map((x) => mapNode(x, "Promotable", topology.Status));
            const mappedRehabs = topology.Rehabs.map((x) => mapNode(x, "Rehab", topology.Status));
            const allNodes = [...mappedMembers, ...mappedPromotables, ...mappedRehabs];

            return {
                nodes: allNodes,
            };
        }
        default:
            console.warn("Unhandled action: ", action);
            return state;
    }
};

import { Reducer } from "react";
import NodeId = Raven.Client.ServerWide.Operations.NodeId;
import DatabaseGroupNodeStatus = Raven.Client.ServerWide.Operations.DatabaseGroupNodeStatus;
import {
    DatabaseInfoLoaded,
    ManageDatabaseGroupState,
    NodeInfo,
} from "components/pages/resources/manageDatabaseGroup/types";

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
    //TODO: check if we simply use react async here?
    switch (action.type) {
        case "DatabaseInfoLoaded": {
            const topology = action.info.NodesTopology;
            const mappedMembers = topology.Members.map((x) => mapNode(x, "Member", topology.Status));
            const mappedPromotables = topology.Promotables.map((x) => mapNode(x, "Promotable", topology.Status));
            const mappedRehabs = topology.Rehabs.map((x) => mapNode(x, "Rehab", topology.Status));
            const allNodes = [...mappedMembers, ...mappedPromotables, ...mappedRehabs];

            const priorityOrder = action.info.NodesTopology.PriorityOrder;
            return {
                nodes: allNodes,
                deletionInProgress: action.info.DeletionInProgress ? Object.keys(action.info.DeletionInProgress) : [],
                encrypted: action.info.IsEncrypted,
                lockMode: action.info.LockMode,
                dynamicDatabaseDistribution: action.info.DynamicNodesDistribution,
                priorityOrder,
                fixOrder: priorityOrder?.length > 0,
            };
        }
        default:
            console.warn("Unhandled action: ", action);
            return state;
    }
};

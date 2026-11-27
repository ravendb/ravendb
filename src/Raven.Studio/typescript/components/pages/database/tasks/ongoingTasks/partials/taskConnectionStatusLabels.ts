import OngoingTaskConnectionStatus = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskConnectionStatus;

export const taskConnectionStatusLabels: Record<OngoingTaskConnectionStatus, string> = {
    None: "None",
    Active: "Active",
    NotActive: "Not active",
    Reconnect: "Reconnect",
    NotOnThisNode: "Not on this node",
};

export const connectionStatusRanks: Record<OngoingTaskConnectionStatus, number> = {
    Reconnect: 1,
    NotActive: 2,
    None: 3,
    NotOnThisNode: 4,
    Active: 5,
};

type ManagerState = {
    localTag: string;
    nodeTags: string[];
};

//TODO: use mock store?
const mockClusterTopologyManagerState = ko.observable<ManagerState>({
    databasesLocal: [],
});

export class MockClusterTopologyManager {
    get state() {
        return mockClusterTopologyManagerState;
    }

    with_Cluster() {
        mockClusterTopologyManagerState({
            nodeTags: ["A", "B", "C"],
            localTag: "A",
        });
    }

    with_Single() {
        mockClusterTopologyManagerState({
            nodeTags: ["A"],
            localTag: "A",
        });
    }
}

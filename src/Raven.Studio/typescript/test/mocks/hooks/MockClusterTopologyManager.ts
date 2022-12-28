import { DatabaseSharedInfo } from "components/models/databases";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { MockedValue } from "test/mocks/services/AutoMockService";
import { createValue } from "../utils";

type ManagerState = {
    localTag: string;
    nodeTags: string[];
};

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
            localTag: "A"
        });
    }

    with_Single() {
        mockClusterTopologyManagerState({
            nodeTags: ["A"],
            localTag: "A"
        });
    }
    
}

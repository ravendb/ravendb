import { globalDispatch } from "components/storeCompat";
import { clusterActions } from "components/common/shell/clusterSlice";

export class MockClusterManager {
    with_Cluster() {
        globalDispatch(
            clusterActions.nodesLoaded([
                {
                    nodeTag: "A",
                    serverUrl: "https://a.server-url.com",
                },
                {
                    nodeTag: "B",
                    serverUrl: "https://b.server-url.com",
                },
                {
                    nodeTag: "C",
                    serverUrl: "https://c.server-url.com",
                },
            ])
        );
        globalDispatch(clusterActions.localNodeTagLoaded("A"));
    }

    with_Single() {
        globalDispatch(
            clusterActions.nodesLoaded([
                {
                    nodeTag: "A",
                    serverUrl: "https://a.server-url.com",
                },
            ])
        );
        globalDispatch(clusterActions.localNodeTagLoaded("A"));
    }
}

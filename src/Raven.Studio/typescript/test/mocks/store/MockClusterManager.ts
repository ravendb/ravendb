import { globalDispatch } from "components/storeCompat";
import { clusterActions } from "components/common/shell/clusterSlice";

export class MockClusterManager {
    with_Cluster() {
        globalDispatch(clusterActions.nodeTagsLoaded(["A", "B", "C"]));
        globalDispatch(clusterActions.localNodeTagLoaded("A"));
    }

    with_Single() {
        globalDispatch(clusterActions.nodeTagsLoaded(["A"]));
        globalDispatch(clusterActions.localNodeTagLoaded("A"));
    }
}

import { globalDispatch } from "components/storeCompat";
import { localNodeTagLoaded, nodeTagsLoaded } from "components/common/shell/clusterSlice";

export class MockClusterManager {
    with_Cluster() {
        globalDispatch(nodeTagsLoaded(["A", "B", "C"]));
        globalDispatch(localNodeTagLoaded("A"));
    }

    with_Single() {
        globalDispatch(nodeTagsLoaded(["A"]));
        globalDispatch(localNodeTagLoaded("A"));
    }
}

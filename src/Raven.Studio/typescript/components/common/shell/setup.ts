import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import { globalDispatch } from "components/storeCompat";
import { activeDatabaseChanged, databasesLoaded, localNodeTagChanged } from "components/common/shell/databasesSlice";
import databasesManager from "common/shell/databasesManager";
import clusterTopologyManager from "common/shell/clusterTopologyManager";

let initialized = false;

function updateReduxStore() {
    const dtos = databasesManager.default.databases().map((x) => x.toDto());
    globalDispatch(databasesLoaded(dtos));
}

const throttledUpdateReduxStore = _.throttle(() => updateReduxStore(), 200);

export function initRedux() {
    if (initialized) {
        return;
    }

    initialized = true;

    databasesManager.default.onUpdateCallback = throttledUpdateReduxStore;
    activeDatabaseTracker.default.database.subscribe((db) => globalDispatch(activeDatabaseChanged(db?.name ?? null)));
    clusterTopologyManager.default.localNodeTag.subscribe((nodeTag) => globalDispatch(localNodeTagChanged(nodeTag)));
}

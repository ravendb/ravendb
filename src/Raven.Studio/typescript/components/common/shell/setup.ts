import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import { globalDispatch } from "components/storeCompat";
import databasesManager from "common/shell/databasesManager";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { databaseActions } from "components/common/shell/databaseSliceActions";
import { setLocale } from "yup";
import { ClusterNode, clusterActions } from "components/common/shell/clusterSlice";

let initialized = false;

function updateReduxStore() {
    const dtos = databasesManager.default.databases().map((x) => x.toDto());
    globalDispatch(databaseActions.databasesLoaded(dtos));
}

const throttledUpdateReduxStore = _.throttle(() => updateReduxStore(), 200);

function initRedux() {
    if (initialized) {
        return;
    }

    initialized = true;

    databasesManager.default.onUpdateCallback = throttledUpdateReduxStore;
    activeDatabaseTracker.default.database.subscribe((db) =>
        globalDispatch(databaseActions.activeDatabaseChanged(db?.name ?? null))
    );

    clusterTopologyManager.default.localNodeTag.subscribe((tag) => {
        globalDispatch(clusterActions.localNodeTagLoaded(tag));
    });

    const onClusterTopologyChanged = () => {
        const clusterNodes: ClusterNode[] =
            clusterTopologyManager.default
                .topology()
                ?.nodes()
                .map((x) => ({
                    nodeTag: x.tag(),
                    serverUrl: x.serverUrl(),
                })) ?? [];

        globalDispatch(clusterActions.nodesLoaded(clusterNodes));
    };

    clusterTopologyManager.default.topology.subscribe((topology) => {
        onClusterTopologyChanged();
        topology.nodes.subscribe(onClusterTopologyChanged);
    });
}

function initYup() {
    setLocale({
        mixed: {
            required: "Required",
            notType(params) {
                switch (params.type) {
                    case "number":
                        return "Please enter valid number";
                    case "string":
                        return "Please enter valid text";
                    default:
                        return "Please enter valid value";
                }
            },
        },
        string: {
            email: "Please enter valid e-mail",
            length: ({ length }) => `Please enter exactly ${length} character${length > 1 ? "s" : ""}`,
            min: ({ min }) => `The provided text should not exceed ${min} characters`,
            max: ({ max }) => `Please provide at least ${max} characters`,
        },
        number: {
            integer: "Please enter integer number",
            positive: "Please enter positive number",
            min: ({ min }) => `Value must be greater than or equal ${min}`,
            max: ({ max }) => `Value must be less than or equal ${max}`,
        },
    });
}

export function commonInit() {
    initRedux();
    initYup();
}

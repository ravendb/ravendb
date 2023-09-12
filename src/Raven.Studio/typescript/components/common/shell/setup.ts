import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import { globalDispatch } from "components/storeCompat";
import databasesManager from "common/shell/databasesManager";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { databaseActions } from "components/common/shell/databaseSliceActions";
import { setLocale } from "yup";
import { ClusterNode, clusterActions } from "components/common/shell/clusterSlice";
import licenseModel from "models/auth/licenseModel";
import { licenseActions } from "./licenseSlice";
import collectionsTracker from "common/helpers/database/collectionsTracker";
import { collectionsTrackerActions } from "./collectionsTrackerSlice";
import changesContext from "common/changesContext";
import { todo } from "common/developmentHelper";
import { services } from "hooks/useServices";
import viewModelBase from "viewmodels/viewModelBase";

let initialized = false;

function updateDatabases() {
    const dtos = databasesManager.default.databases().map((x) => x.toDto());
    globalDispatch(databaseActions.databasesLoaded(dtos));
}

const throttledUpdateDatabases = _.throttle(updateDatabases, 200);

function updateReduxCollectionsTracker() {
    globalDispatch(
        collectionsTrackerActions.collectionsLoaded(
            collectionsTracker.default.collections().map((x) => ({
                name: x.name,
                countPrefix: x.countPrefix(),
                documentCount: x.documentCount(),
                hasBounceClass: x.hasBounceClass(),
                lastDocumentChangeVector: x.lastDocumentChangeVector(),
                sizeClass: x.sizeClass(),
            }))
        )
    );
}

function updateLicenseLimitsUsage() {
    services.licenseService.getLimitsUsage().then((dto) => globalDispatch(licenseActions.limitsUsageLoaded(dto)));
}

function initRedux() {
    if (initialized) {
        return;
    }

    initialized = true;

    databasesManager.default.onUpdateCallback = throttledUpdateDatabases;

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

    viewModelBase.clientVersion.subscribe((version) => globalDispatch(clusterActions.clientVersionLoaded(version)));

    licenseModel.licenseStatus.subscribe((licenseStatus) => {
        globalDispatch(licenseActions.statusLoaded(licenseStatus));

        todo("Other", "Damian", "Call it only when the usage have changed");
        updateLicenseLimitsUsage();
    });

    collectionsTracker.default.registerOnGlobalChangeVectorUpdatedHandler(updateReduxCollectionsTracker);
    collectionsTracker.default.onUpdateCallback = updateReduxCollectionsTracker;

    changesContext.default.connectServerWideNotificationCenter();
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
            min: ({ min }) => `Please provide at least ${min} characters`,
            max: ({ max }) => `The provided text should not exceed ${max} characters`,
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

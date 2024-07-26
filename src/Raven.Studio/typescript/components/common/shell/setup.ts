import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import { globalDispatch } from "components/storeCompat";
import databasesManager from "common/shell/databasesManager";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { databaseActions } from "components/common/shell/databaseSliceActions";
import * as yup from "yup";
import { ClusterNode, clusterActions } from "components/common/shell/clusterSlice";
import licenseModel from "models/auth/licenseModel";
import { licenseActions } from "./licenseSlice";
import collectionsTracker from "common/helpers/database/collectionsTracker";
import { collectionsTrackerActions } from "./collectionsTrackerSlice";
import changesContext from "common/changesContext";
import { services } from "hooks/useServices";
import viewModelBase from "viewmodels/viewModelBase";
import buildInfo = require("models/resources/buildInfo");
import genUtils = require("common/generalUtils");
import accessManager = require("common/shell/accessManager");
import { accessManagerActions } from "components/common/shell/accessManagerSlice";

let initialized = false;

function updateDatabases() {
    const dtos = databasesManager.default.databases().map((x) => x.toDto());
    globalDispatch(databaseActions.databasesLoaded(dtos));
}

const throttledUpdateDatabases = _.throttle(updateDatabases, 200);

export const throttledUpdateLicenseLimitsUsage = _.throttle(() => {
    services.licenseService
        .getClusterLimitsUsage()
        .then((dto) => globalDispatch(licenseActions.limitsUsageLoaded(dto)));
}, 200);

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

        globalDispatch(clusterActions.serverStateLoaded({ passive: topology.isPassive() }));
        topology.isPassive.subscribe((isPassive) => {
            globalDispatch(clusterActions.serverStateLoaded({ passive: isPassive }));
        });
    });

    viewModelBase.clientVersion.subscribe((version) => globalDispatch(clusterActions.clientVersionLoaded(version)));
    buildInfo.serverBuildVersion.subscribe((version) => globalDispatch(clusterActions.serverVersionLoaded(version)));

    licenseModel.licenseStatus.subscribe((licenseStatus) => {
        globalDispatch(licenseActions.statusLoaded(licenseStatus));
        throttledUpdateLicenseLimitsUsage();
    });
    licenseModel.supportCoverage.subscribe((supportCoverage) => {
        globalDispatch(licenseActions.supportLoaded(supportCoverage));
    });

    collectionsTracker.default.collections.subscribe((collections) =>
        globalDispatch(collectionsTrackerActions.collectionsLoaded(collections.map((x) => x.toCollectionState())))
    );

    changesContext.default.connectServerWideNotificationCenter();

    accessManager.default.securityClearance.subscribe((securityClearance) =>
        globalDispatch(accessManagerActions.onSecurityClearanceSet(securityClearance))
    );
    accessManager.default.secureServer.subscribe((isSecureServer) =>
        globalDispatch(accessManagerActions.onIsSecureServerSet(isSecureServer))
    );
}

declare module "yup" {
    interface StringSchema {
        basicUrl(msg?: string): this;
        base64(msg?: string): this;
    }
}

function initYup() {
    yup.setLocale({
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
            url: "Please enter valid URL",
            email: "Please enter valid e-mail",
            length: ({ length }) => `Please enter exactly ${length} character${length > 1 ? "s" : ""}`,
            min: ({ min }) => `Please provide at least ${min} characters`,
            max: ({ max }) => `The provided text should not exceed ${max} characters`,
            trim: "Please remove whitespace",
        },
        number: {
            integer: "Please enter integer number",
            positive: "Please enter positive number",
            min: ({ min }) => `Value must be greater than or equal ${min}`,
            max: ({ max }) => `Value must be less than or equal ${max}`,
        },
        date: {
            min: ({ min }) => `Value must be ${min} or later`,
            max: ({ max }) => `Value must be ${max} or earlier`,
        },
        array: {
            min: ({ min }) => `The list should contain at least ${min} item${min > 1 ? "s" : ""}`,
            max: ({ max }) => `The list should contain a maximum of ${max} item${max > 1 ? "s" : ""}`,
        },
    });

    yup.addMethod<yup.StringSchema>(yup.string, "basicUrl", function (msg = genUtils.invalidUrlMessage) {
        return this.matches(genUtils.urlRegex, msg);
    });

    yup.addMethod<yup.StringSchema>(yup.string, "base64", function (msg = "Please enter valid base64 string") {
        return this.matches(/^([0-9a-zA-Z+/]{4})*(([0-9a-zA-Z+/]{2}==)|([0-9a-zA-Z+/]{3}=))?$/, msg);
    });
}

export function commonInit() {
    initRedux();
    initYup();
}

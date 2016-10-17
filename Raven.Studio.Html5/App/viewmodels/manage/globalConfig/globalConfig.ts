import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import adminSettings = require("viewmodels/manage/adminSettings");
import license = require("models/auth/license");

class globalConfig extends viewModelBase {

    router: DurandalRouter;
    currentRouteTitle: KnockoutComputed<string>;

    static developerLicense = ko.computed(() => !license.licenseStatus().IsCommercial);
    static canUseGlobalConfigurations = ko.computed(() => !license.licenseStatus().IsCommercial || license.licenseStatus().Attributes.globalConfigurations === "true");

    constructor() {
        super();

        this.router = adminSettings.adminSettingsRouter.createChildRouter()
            .map([ 
                { route: "globalConfig", moduleId: "viewmodels/manage/globalConfig/globalConfigPeriodicExport", title: "Periodic export", tooltip: "", nav: true, hash: appUrl.forGlobalConfigPeriodicExport() },
                { route: "globalConfigDatabaseSettings", moduleId: "viewmodels/manage/globalConfig/globalConfigDatabaseSettings", title: "Cluster-wide database settings", tooltip: "Global cluster-wide database settings", nav: true, hash: appUrl.forGlobalConfigDatabaseSettings() },
                { route: "globalConfigReplication", moduleId: "viewmodels/manage/globalConfig/globalConfigReplications", title: "Replication", tooltip: "Global replication settings", nav: true, hash: appUrl.forGlobalConfigReplication() },
                { route: "globalConfigSqlReplication", moduleId: "viewmodels/manage/globalConfig/globalConfigSqlReplication", title: "SQL Replication", tooltip: "Global SQL replication settings", nav: true, hash: appUrl.forGlobalConfigSqlReplication()},
                { route: "globalConfigQuotas", moduleId: "viewmodels/manage/globalConfig/globalConfigQuotas", title: "Quotas", tooltip: "Global quotas settings", nav: true, hash: appUrl.forGlobalConfigQuotas() },
                { route: "globalConfigCustomFunctions", moduleId: "viewmodels/manage/globalConfig/globalConfigCustomFunctions", title: "Custom functions", tooltip: "Global custom functions settings", nav: true, hash: appUrl.forGlobalConfigCustomFunctions() },
                { route: "globalConfigVersioning", moduleId: "viewmodels/manage/globalConfig/globalConfigVersioning", title: "Versioning", tooltip: "Global versioning settings", nav: true, hash: appUrl.forGlobalConfigVersioning() }
            ])
            .buildNavigationModel();

        appUrl.mapUnknownRoutes(this.router);

        this.currentRouteTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }

    protected shouldReportUsage(): boolean {
        return false;
    }
}

export = globalConfig;    

import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import adminSettings = require("viewmodels/adminSettings");

class globalConfig extends viewModelBase {

    router: DurandalRouter;
    currentRouteTitle: KnockoutComputed<string>;

	constructor() {
        super();

        this.router = adminSettings.adminSettingsRouter.createChildRouter()
            .map([
                { route: "admin/settings/globalConfig", moduleId: "viewmodels/globalConfigPeriodicExport", title: "Periodic export", tooltip: "", nav: true, hash: appUrl.forGlobalConfigPeriodicExport() },
                { route: "admin/settings/globalConfigReplication", moduleId: "viewmodels/globalConfigReplications", title: "Replication", tooltip: "Global replication settings", nav: true, hash: appUrl.forGlobalConfigReplication() },
                { route: "admin/settings/globalConfigSqlReplication", moduleId: "viewmodels/globalConfigSqlReplication", title: "SQL Replication", tooltip: "Global SQL replication settings", nav: true, hash: appUrl.forGlobalConfigSqlReplication()},
                { route: "admin/settings/globalConfigQuotas", moduleId: "viewmodels/globalConfigQuotas", title: "Quotas", tooltip: "Global quotas settings", nav: true, hash: appUrl.forGlobalConfigQuotas() },
                { route: "admin/settings/globalConfigCustomFunctions", moduleId: "viewmodels/globalConfigCustomFunctions", title: "Custom functions", tooltip: "Global custom functions settings", nav: true, hash: appUrl.forGlobalConfigCustomFunctions() },
                { route: "admin/settings/globalConfigVersioning", moduleId: "viewmodels/globalConfigVersioning", title: "Versioning", tooltip: "Global versioning settings", nav: true, hash: appUrl.forGlobalConfigVersioning() }
            ])
            .buildNavigationModel();

        appUrl.mapUnknownRoutes(this.router);

        this.currentRouteTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }
}

export = globalConfig;    
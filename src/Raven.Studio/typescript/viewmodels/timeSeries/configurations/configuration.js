var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "plugins/router", "common/appUrl", "viewmodels/viewModelBase"], function (require, exports, durandalRouter, appUrl, viewModelBase) {
    var configuration = (function (_super) {
        __extends(configuration, _super);
        function configuration() {
            var _this = this;
            _super.call(this);
            this.router = null;
            this.bundleMap = { types: "Types" };
            this.appUrls = appUrl.forCurrentDatabase();
            var typesRoute = { route: 'timeSeries/settings/types', moduleId: 'viewmodels/timeSeries/configuration/types', title: 'Types', nav: true, hash: appUrl.forCurrentTimeSeries().timeSeriesConfigurationTypes };
            this.router = durandalRouter.createChildRouter()
                .map([
                typesRoute
            ])
                .buildNavigationModel();
            appUrl.mapUnknownRoutes(this.router);
            this.activeSubViewTitle = ko.computed(function () {
                // Is there a better way to get the active route?
                var activeRoute = _this.router.navigationModel().first(function (r) { return r.isActive(); });
                return activeRoute != null ? activeRoute.title : "";
            });
        }
        return configuration;
    })(viewModelBase);
    return configuration;
});

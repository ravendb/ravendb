
import MENU_BASED_ROUTER_CONFIGURATION = require("common/shell/routerConfiguration");

class Routes {

    static get(appUrls: computedAppUrls): Array<DurandalRouteConfiguration> {
        return MENU_BASED_ROUTER_CONFIGURATION;
    }
}

export = Routes;

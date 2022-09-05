
import MENU_BASED_ROUTER_CONFIGURATION = require("common/shell/routerConfiguration");

class Routes {

    static get(): Array<DurandalRouteConfiguration> {
        return MENU_BASED_ROUTER_CONFIGURATION;
    }
}

export = Routes;

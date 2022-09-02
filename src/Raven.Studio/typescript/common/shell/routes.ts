
import MENU_BASED_ROUTER_CONFIGURATION = require("common/shell/routerConfiguration");

class Routes {

    static get(): Array<DurandalRouteConfiguration> {
        const routes: DurandalRouteConfiguration[] = [
            {
                route: "databases/edit",
                title: "Edit Document",
                moduleId: "viewmodels/database/documents/editDocument",
                nav: false
            }
        ];

        return routes.concat(MENU_BASED_ROUTER_CONFIGURATION);
    }
}

export = Routes;

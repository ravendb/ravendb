import leafMenuItem = require("common/shell/menu/leafMenuItem");
import appUrl from "common/appUrl";
import { bridgeToReact } from "common/reactUtils";
import { DatabasesPage } from "components/pages/resources/databases/DatabasesPage";

export = getDatabasesMenuItem;

function getDatabasesMenuItem(appUrls: computedAppUrls) {
    const databasesView = bridgeToReact(DatabasesPage, "nonShardedView");
    
    appUrl.defaultModule = databasesView;
    
    return new leafMenuItem({
        route: "databases",
        title: "Databases",
        moduleId: databasesView,
        nav: true,
        css: 'icon-resources',
        dynamicHash: appUrls.databasesManagement
    });
}

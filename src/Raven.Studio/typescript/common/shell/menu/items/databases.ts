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
        search: {
            alternativeTitles: ["or1", "or2", "or3"],
            innerActions: [
                {
                    name: "New Database",
                    alternativeNames: ["or", "second", "third"] //TODO:
                },
                {
                    name: "fruits",
                    alternativeNames: ["or", "or2"] //TODO:
                }
            ],
        },
        moduleId: databasesView,
        nav: true,
        css: 'icon-resources',
        dynamicHash: appUrls.databasesManagement
    });
}

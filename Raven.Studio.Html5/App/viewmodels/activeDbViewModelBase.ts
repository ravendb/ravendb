import appUrl = require("common/appUrl");
import database = require("models/database");

/*
 * Base view model class that keeps track of the currently selected database.
*/
class activeDbViewModelBase {
    activeDatabase = ko.observable<database>().subscribeTo("ActivateDatabase", true);

    activate(args) {
        this.activeDatabase(appUrl.getDatabase());
    }

    deactivate() {
        this.activeDatabase.unsubscribeFrom("ActivateDatabase");
    }
}

export = activeDbViewModelBase;
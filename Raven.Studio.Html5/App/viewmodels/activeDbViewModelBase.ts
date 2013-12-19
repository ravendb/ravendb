import appUrl = require("common/appUrl");
import database = require("models/database");

/*
 * Base view model class that keeps track of the currently selected database.
*/
class activeDbViewModelBase {
    activeDatabase = ko.observable<database>().subscribeTo("ActivateDatabase", true);

    activate(args) {
        var db = appUrl.getDatabase();
        var currentDb = this.activeDatabase();
        if (!currentDb || currentDb.name !== db.name) {
            ko.postbox.publish("ActivateDatabaseWithName", db.name);
        }
    }

    deactivate() {
        this.activeDatabase.unsubscribeFrom("ActivateDatabase");
    }
}

export = activeDbViewModelBase;
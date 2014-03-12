import appUrl = require("common/appUrl");
import database = require("models/database");

/*
 * Base view model class that provides basic view model services, such as tracking the active database and providing a means to add keyboard shortcuts.
*/
class viewModelBase {
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

    createKeyboardShortcut(keys: string, handler: () => void, elementSelector: string) {
        jwerty.key(keys, e => {
            e.preventDefault();
            handler();
        }, this, elementSelector);
    }

    removeKeyboardShortcuts(elementSelector: string) {
        $(elementSelector).unbind('keydown.jwerty');
    }
}

export = viewModelBase;
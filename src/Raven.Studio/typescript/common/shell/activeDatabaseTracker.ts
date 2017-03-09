import EVENTS = require("common/constants/events");
import database = require("models/resources/database");
import databaseActivatedEventArgs = require("viewmodels/resources/databaseActivatedEventArgs");
import databaseDisconnectedEventArgs = require("viewmodels/resources/databaseDisconnectedEventArgs");
import router = require("plugins/router");
import messagePublisher = require("common/messagePublisher");
import appUrl = require("common/appUrl");

export = activeDatabaseTracker;

class activeDatabaseTracker {

    static default: activeDatabaseTracker = new activeDatabaseTracker();

    database: KnockoutObservable<database> = ko.observable<database>();

    constructor() {
        ko.postbox.subscribe(EVENTS.Database.Activate, (e: databaseActivatedEventArgs) => {

            // If the 'same' database was selected from the top resources selector dropdown, 
            // then we want the knockout observable to be aware of it so that scrollling on page will occur
            if (e.database === this.database()) {
                this.database(null);
            }
            // Set the active database
            this.database(e.database);
          });

        ko.postbox.subscribe(EVENTS.Database.Disconnect, (e: databaseDisconnectedEventArgs) => {
            if (e.database === this.database()) {
                this.database(null);
            }

            // display warning to user if another user deleted or disabled active database
            // but don't do this on databases page
            if (!this.onDatabasesPage()) {
                if (e.cause === "DatabaseDeleted") {
                    messagePublisher.reportWarning(e.database.fullTypeName + " " + e.database.name + " was deleted");
                    router.navigate(appUrl.forDatabases());
                } else if (e.cause === "DatabaseDisabled") {
                    messagePublisher.reportWarning(e.database.fullTypeName + " " + e.database.name + " was disabled");
                    router.navigate(appUrl.forDatabases());
                }
            }
        });
    }

    private onDatabasesPage() {
        const instruction = router.activeInstruction();
        if (!instruction) {
            return false;
        }
        return instruction.fragment === "databases";
    }

}

import EVENTS = require("common/constants/events");
import database = require("models/resources/database");
import databaseDisconnectedEventArgs = require("viewmodels/resources/databaseDisconnectedEventArgs");
import router = require("plugins/router");
import messagePublisher = require("common/messagePublisher");
import databaseSettings = require("common/settings/databaseSettings");
import studioSettings = require("common/settings/studioSettings");

class activeDatabaseTracker {

    static default: activeDatabaseTracker = new activeDatabaseTracker();

    database: KnockoutObservable<database> = ko.observable<database>();

    settings: KnockoutObservable<databaseSettings> = ko.observable<databaseSettings>();

    constructor() {
        ko.postbox.subscribe(EVENTS.Database.Disconnect, (e: databaseDisconnectedEventArgs) => {
            if (e.database === this.database()) {
                this.database(null);
            }

            // display warning to user if another user deleted or disabled active database
            // but don't do this on databases page
            if (!this.onDatabasesPage()) {
                if (e.cause === "DatabaseDeleted") {
                    messagePublisher.reportWarning(e.database.fullTypeName + " " + e.database.name + " was deleted");
                    router.navigate("#databases"); // don't use appUrl since it will create dependecy cycle
                } else if (e.cause === "DatabaseDisabled") {
                    messagePublisher.reportWarning(e.database.fullTypeName + " " + e.database.name + " was disabled");
                    router.navigate("#databases"); // don't use appUrl since it will create dependecy cycle
                } else if (e.cause === "DatabaseIsNotRelevant") {
                    messagePublisher.reportWarning(e.database.fullTypeName + " " + e.database.name + " is not longer relevant on this node");
                    router.navigate("#databases"); // don't use appUrl since it will create dependecy cycle
                }
            }
        });

        studioSettings.default.init(this.settings);
    }

    onActivation(db: database): JQueryPromise<void> {
        const task = $.Deferred<void>();

        // If the 'same' database was selected from the top databases selector dropdown, 
        // then we want the knockout observable to be aware of it so that scrolling on page will occur
        if (db === this.database()) {
            this.database(null);
        }

        studioSettings.default.forDatabase(db)
            .done((settings) => {

                this.settings(settings);
                // Set the active database
                this.database(db);
                task.resolve();
            })
            .fail(() => task.reject());

        return task;
    }

    private onDatabasesPage() {
        const instruction = router.activeInstruction();
        if (!instruction) {
            return false;
        }
        return instruction.fragment === "databases";
    }

}

export = activeDatabaseTracker;

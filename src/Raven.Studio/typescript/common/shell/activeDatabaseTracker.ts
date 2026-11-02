import EVENTS = require("common/constants/events");
import database = require("models/resources/database");
import router = require("plugins/router");
import messagePublisher = require("common/messagePublisher");
import databaseSettings = require("common/settings/databaseSettings");
import studioSettings = require("common/settings/studioSettings");

interface databaseActivation {
    db: database;
    task: JQueryDeferred<void>;
}

class activeDatabaseTracker {

    static default: activeDatabaseTracker = new activeDatabaseTracker();

    database: KnockoutObservable<database> = ko.observable<database>();
    
    settings: KnockoutObservable<databaseSettings> = ko.observable<databaseSettings>();

    private pendingActivation: databaseActivation = null;

    constructor() {
        ko.postbox.subscribe(EVENTS.Database.Disconnect, (e: databaseDisconnectedEventArgs) => {
            if (e.cause !== "ChangingDatabase" && e.databaseName === this.database().name) {
                this.database(null);
            }

            // display warning to user if another user deleted or disabled active database
            // but don't do this on databases page
            if (!this.onDatabasesPage()) {
                if (e.cause === "DatabaseDeleted") {
                    /* TODO
                    messagePublisher.reportWarning("Database " + e.databaseName + " was deleted");
                    router.navigate("#databases"); // don't use appUrl since it will create dependency cycle*/
                } else if (e.cause === "DatabaseDisabled") {
                    messagePublisher.reportWarning("Database " + e.databaseName + " was disabled");
                    router.navigate("#databases"); // don't use appUrl since it will create dependency cycle
                } else if (e.cause === "DatabaseIsNotRelevant") {
                    messagePublisher.reportWarning("Database " + e.databaseName + " is not longer relevant on this node");
                    router.navigate("#databases"); // don't use appUrl since it will create dependency cycle
                }
            }
        });

        studioSettings.default.init(this.settings);
    }

    getActivationTask(db: database): JQueryPromise<void> | null {
        if (this.pendingActivation) {
            return this.pendingActivation.db.name === db.name ? this.pendingActivation.task : null;
        }

        return this.database()?.name === db.name ? $.Deferred<void>().resolve() : null;
    }

    onActivation(db: database): JQueryPromise<void> {
        this.pendingActivation?.task.reject();

        const activation: databaseActivation = { db, task: $.Deferred<void>() };
        this.pendingActivation = activation;

        studioSettings.default.forDatabase(db)
            .done((settings) => {
                if (this.pendingActivation !== activation) {
                    return;
                }

                this.pendingActivation = null;
                this.settings(settings);
                // Set the active database
                this.database(db);
                activation.task.resolve();
            })
            .fail(() => {
                if (this.pendingActivation !== activation) {
                    return;
                }

                this.pendingActivation = null;
                activation.task.reject();
            });

        return activation.task;
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

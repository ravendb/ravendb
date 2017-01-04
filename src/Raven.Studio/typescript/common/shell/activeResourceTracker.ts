import EVENTS = require("common/constants/events");
import database = require("models/resources/database");
import resource = require("models/resources/resource");
import filesystem = require("models/filesystem/filesystem");
import counterStorage = require("models/counter/counterStorage");
import timeSeries = require("models/timeSeries/timeSeries");
import resourceActivatedEventArgs = require("viewmodels/resources/resourceActivatedEventArgs");
import resourceDisconnectedEventArgs = require("viewmodels/resources/resourceDisconnectedEventArgs");
import router = require("plugins/router");
import messagePublisher = require("common/messagePublisher");

export = activeResourceTracker;

class activeResourceTracker {

    static default: activeResourceTracker = new activeResourceTracker();

    resource: KnockoutObservable<resource> = ko.observable<resource>();

    database: KnockoutComputed<database> = ko.computed<database>(() =>
        this.tryCastActiveResource<database>(database.qualifier));

    fileSystem: KnockoutComputed<filesystem> = ko.computed<filesystem>(() =>
        this.tryCastActiveResource<filesystem>(filesystem.qualifier));

    counterStorage: KnockoutComputed<counterStorage> = ko.computed<counterStorage>(() =>
        this.tryCastActiveResource<counterStorage>(counterStorage.qualifier));

    timeSeries: KnockoutComputed<timeSeries> = ko.computed<timeSeries>(() =>
        this.tryCastActiveResource<timeSeries>(timeSeries.qualifier));

    constructor() {
        ko.postbox.subscribe(EVENTS.Resource.Activate, (e: resourceActivatedEventArgs) => {
            this.resource(e.resource);
        });

        ko.postbox.subscribe(EVENTS.Resource.Disconnect, (e: resourceDisconnectedEventArgs) => {
            if (e.resource === this.resource()) {
                this.resource(null);
            }

            // display warning to user if another user deleted or disabled active resource
            // but don't do this on resources page
            if (!this.onResourcesPage()) {
                if (e.cause === "ResourceDeleted") {
                    messagePublisher.reportWarning(e.resource.fullTypeName + " " + e.resource.name + " was deleted");
                    router.navigate("#resources");
                } else if (e.cause === "ResourceDisabled") {
                    messagePublisher.reportWarning(e.resource.fullTypeName + " " + e.resource.name + " was disabled");
                    router.navigate("#resources");
                }
            }
        });
    }

    private onResourcesPage() {
        const instruction = router.activeInstruction();
        if (!instruction) {
            return false;
        }
        return instruction.fragment === "resources";
    }

    private tryCastActiveResource<T extends resource>(expectedQualified: string): T {
        const resource = this.resource();
        if (resource && resource.qualifier === expectedQualified) {
            return resource as T;
        }

        return null;
    }
}

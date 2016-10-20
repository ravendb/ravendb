import EVENTS = require("common/constants/events");
import database = require("models/resources/database");
import resource = require("models/resources/resource");
import filesystem = require("models/filesystem/filesystem");
import counterStorage = require("models/counter/counterstorage");
import timeSeries = require("models/timeseries/timeseries");
import resourceActivatedEventArgs = require("viewmodels/resources/resourceActivatedEventArgs");

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

        ko.postbox.subscribe(EVENTS.Resource.Disconnect, (e: any) => {
            if (e.resource === this.resource()) {
                this.resource(null);
            }
        });
    }

    private tryCastActiveResource<T extends resource>(expectedQualified: string): T {
        const resource = this.resource();
        if (resource && resource.qualifier === expectedQualified) {
            return resource as T;
        }

        return null;
    }
}
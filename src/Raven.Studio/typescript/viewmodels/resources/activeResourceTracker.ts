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
        this.tryCastActiveResource<database>(TenantType.Database));

    fileSystem: KnockoutComputed<filesystem> = ko.computed<filesystem>(() =>
        this.tryCastActiveResource<filesystem>(TenantType.FileSystem));

    counterStorage: KnockoutComputed<counterStorage> = ko.computed<counterStorage>(() =>
        this.tryCastActiveResource<counterStorage>(TenantType.CounterStorage));

    timeSeries: KnockoutComputed<timeSeries> = ko.computed<timeSeries>(() =>
        this.tryCastActiveResource<timeSeries>(TenantType.TimeSeries));

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

    private tryCastActiveResource<T extends resource>(checkType: TenantType): T {
        let resource = this.resource();
        if (resource && resource.type === checkType) {
            return resource as T;
        }

        return null;
    }
}
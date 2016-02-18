/// <reference path="../../../typings/tsd.d.ts"/>

import database = require("models/resources/database");

class serverSmugglingItem {

    resource: database;
    incremental = ko.observable<boolean>(true);
    stripReplicationInformation = ko.observable<boolean>(false);
    shouldDisableVersioningBundle = ko.observable<boolean>(false);
    hasReplicationBundle: KnockoutComputed<boolean>;
    hasVersioningBundle: KnockoutComputed<boolean>;

    constructor(database: database) {
        this.resource = database;
        this.hasReplicationBundle = ko.computed(() => this.resource.isBundleActive("replication"));
        this.hasVersioningBundle = ko.computed(() => this.resource.isBundleActive("versioning"));
    }

    toDto(): serverSmugglingItemDto {
        return {
            Name: this.resource.name,
            Incremental: this.incremental(),
            StripReplicationInformation: this.stripReplicationInformation(),
            ShouldDisableVersioningBundle: this.shouldDisableVersioningBundle()
        }
    }
    
}

export = serverSmugglingItem;

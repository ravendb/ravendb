/// <reference path="../../../typings/tsd.d.ts"/>

import database = require("models/resources/database");

class serverSmugglingItem {

    database: database;
    incremental = ko.observable<boolean>(true);
    stripReplicationInformation = ko.observable<boolean>(false);
    shouldDisableVersioningBundle = ko.observable<boolean>(false);
    hasReplicationBundle: KnockoutComputed<boolean>;
    hasVersioningBundle: KnockoutComputed<boolean>;

    constructor(database: database) {
        this.database = database;
        this.hasReplicationBundle = ko.computed(() => this.database.isBundleActive("replication"));
        this.hasVersioningBundle = ko.computed(() => this.database.isBundleActive("versioning"));
    }

    toDto(): serverSmugglingItemDto {
        return {
            Name: this.database.name,
            Incremental: this.incremental(),
            StripReplicationInformation: this.stripReplicationInformation(),
            ShouldDisableVersioningBundle: this.shouldDisableVersioningBundle()
        }
    }
    
}

export = serverSmugglingItem;

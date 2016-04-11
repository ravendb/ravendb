/// <reference path="../../../typings/tsd.d.ts"/>

import resource = require("models/resources/resource");
import license = require("models/auth/license");
import databaseStatistics = require("models/resources/databaseStatistics");

class database extends resource {
    statistics = ko.observable<databaseStatistics>();
    indexingDisabled = ko.observable<boolean>(false);
    rejectClientsMode = ko.observable<boolean>(false);
    clusterWide = ko.observable<boolean>(false);
    recentQueriesLocalStorageName: string;
    mergedIndexLocalStoragePrefix: string;
    static type = "database";
    iconName: KnockoutComputed<string>;

    constructor(name: string, isAdminCurrentTenant: boolean = true, isDisabled: boolean = false, isIndexingDisabled: boolean = false, isRejectClientsMode = false, isLoaded = false, clusterWide = false) {
        super(name, TenantType.Database, isAdminCurrentTenant);
        this.fullTypeName = "Database";
        this.disabled(isDisabled);
        this.activeBundles(['sql-replication', 'replication']);
        this.indexingDisabled(isIndexingDisabled);
        this.rejectClientsMode(isRejectClientsMode);
        this.isLoaded(isLoaded);
        this.clusterWide(clusterWide);
        this.iconName = ko.computed(() => !this.clusterWide() ? "fa fa-database" : "fa-cubes");
        this.itemCountText = ko.computed(() => !!this.statistics() ? this.statistics().countOfDocumentsText() : "");
        this.isLicensed = ko.computed(() => {
            // TODO: Implement
            return true;
        });
        this.recentQueriesLocalStorageName = "ravenDB-recentQueries." + name;
        this.mergedIndexLocalStoragePrefix = "ravenDB-mergedIndex." + name;
    }

    activate() {
        this.isLoaded(true);
        ko.postbox.publish("ActivateDatabase", this);
    }

    saveStatistics(dto: databaseStatisticsDto) {
        if (!this.statistics()) {
            this.statistics(new databaseStatistics());
        }

        this.statistics().fromDto(dto);
    }

    static getNameFromUrl(url: string) {
        var index = url.indexOf("databases/");
        return (index > 0) ? url.substring(index + 10) : "";
    }
}

export = database;

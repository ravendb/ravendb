/// <reference path="../../../../typings/tsd.d.ts"/>

import databaseInfo = require("models/resources/info/databaseInfo");

class databasesInfo {

    sortedResources = ko.observableArray<databaseInfo>();

    databasesCount: KnockoutComputed<number>;

    constructor(dto: Raven.Client.Server.Operations.DatabasesInfo) {

        const databases = dto.Databases.map(db => new databaseInfo(db));

        const resources = [...databases];
        resources.sort((a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()));

        this.sortedResources(resources);

        this.initObservables();
    }

    getByQualifiedName(qualifiedName: string) {
        return this.sortedResources().find(x => x.qualifiedName.toLowerCase() === qualifiedName.toLowerCase());
    }

    updateDatabase(newDatabaseInfo: Raven.Client.Server.Operations.DatabaseInfo, resourceType: string) {
        let resourceToUpdate = this.getByQualifiedName(resourceType + "/" + newDatabaseInfo.Name);

        if (resourceToUpdate) {
            resourceToUpdate.update(newDatabaseInfo);
        } else { // new resource - create instance of it
            let dto = newDatabaseInfo as Raven.Client.Server.Operations.DatabaseInfo;
            let resourceToAdd = new databaseInfo(dto);

            let locationToInsert = _.sortedIndexBy(this.sortedResources(), resourceToAdd, function (item) { return item.name.toLowerCase() });
            this.sortedResources.splice(locationToInsert, 0, resourceToAdd);
        }
    }

    private initObservables() {
        this.databasesCount = ko.pureComputed(() => this
            .sortedResources()
            .filter(r => r instanceof databaseInfo)
            .length);
    }
}

export = databasesInfo;

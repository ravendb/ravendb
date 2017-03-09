/// <reference path="../../../../typings/tsd.d.ts"/>

import databaseInfo = require("models/resources/info/databaseInfo");

class databasesInfo {

    sortedDatabases = ko.observableArray<databaseInfo>();

    databasesCount: KnockoutComputed<number>;

    constructor(dto: Raven.Client.Server.Operations.DatabasesInfo) {

        const databases = dto.Databases.map(db => new databaseInfo(db));

        const dbs = [...databases];
        dbs.sort((a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()));

        this.sortedDatabases(dbs);

        this.initObservables();
    }

    getByQualifiedName(qualifiedName: string) {
        return this.sortedDatabases().find(x => x.qualifiedName.toLowerCase() === qualifiedName.toLowerCase());
    }

    updateDatabase(newDatabaseInfo: Raven.Client.Server.Operations.DatabaseInfo, resourceType: string) {
        let resourceToUpdate = this.getByQualifiedName(resourceType + "/" + newDatabaseInfo.Name);

        if (resourceToUpdate) {
            resourceToUpdate.update(newDatabaseInfo);
        } else { // new resource - create instance of it
            let dto = newDatabaseInfo as Raven.Client.Server.Operations.DatabaseInfo;
            let resourceToAdd = new databaseInfo(dto);

            let locationToInsert = _.sortedIndexBy(this.sortedDatabases(), resourceToAdd, function (item) { return item.name.toLowerCase() });
            this.sortedDatabases.splice(locationToInsert, 0, resourceToAdd);
        }
    }

    private initObservables() {
        this.databasesCount = ko.pureComputed(() => this
            .sortedDatabases()
            .filter(r => r instanceof databaseInfo)
            .length);
    }
}

export = databasesInfo;

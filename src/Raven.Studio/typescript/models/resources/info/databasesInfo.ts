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

    getByName(name: string) {
        return this.sortedDatabases().find(x => x.name.toLowerCase() === name.toLowerCase());
    }

    updateDatabase(newDatabaseInfo: Raven.Client.Server.Operations.DatabaseInfo) {
        let databaseToUpdate = this.getByName(newDatabaseInfo.Name);

        if (databaseToUpdate) {
            databaseToUpdate.update(newDatabaseInfo);
        } else { // new database - create instance of it
            let dto = newDatabaseInfo as Raven.Client.Server.Operations.DatabaseInfo;
            let databaseToAdd = new databaseInfo(dto);

            let locationToInsert = _.sortedIndexBy(this.sortedDatabases(), databaseToAdd, function (item) { return item.name.toLowerCase() });
            this.sortedDatabases.splice(locationToInsert, 0, databaseToAdd);
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

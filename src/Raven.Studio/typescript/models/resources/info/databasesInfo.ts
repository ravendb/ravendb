/// <reference path="../../../../typings/tsd.d.ts"/>

import databaseInfo = require("models/resources/info/databaseInfo");
import generalUtils = require("common/generalUtils");

class databasesInfo {

    sortedDatabases = ko.observableArray<databaseInfo>();

    databasesCount: KnockoutComputed<number>;

    constructor(dto: Raven.Client.ServerWide.Operations.DatabasesInfo) {

        const databases = dto.Databases.map(db => new databaseInfo(db));

        const dbs = [...databases];
        dbs.sort((a, b) => generalUtils.sortAlphaNumeric(a.name, b.name));

        this.sortedDatabases(dbs);

        this.initObservables();
    }

    getByName(name: string) {
        return this.sortedDatabases().find(x => x.name.toLowerCase() === name.toLowerCase());
    }

    updateDatabase(newDatabaseInfo: Raven.Client.ServerWide.Operations.DatabaseInfo) {
        const databaseToUpdate = this.getByName(newDatabaseInfo.Name);

        if (databaseToUpdate) {
            databaseToUpdate.update(newDatabaseInfo);
        } else { // new database - create instance of it
            const dto = newDatabaseInfo as Raven.Client.ServerWide.Operations.DatabaseInfo;
            const databaseToAdd = new databaseInfo(dto);
            this.sortedDatabases.push(databaseToAdd);
            this.sortedDatabases.sort((a, b) => generalUtils.sortAlphaNumeric(a.name, b.name));
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

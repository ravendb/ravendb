import getStatusDebugSqlReplicationCommand = require("commands/getStatusDebugSqlReplicationCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import sqlReplicationStats = require("common/sqlReplicationStats");


class statusDebugSqlReplication extends viewModelBase {
    data = ko.observable<sqlReplicationStats[]>();
    columnWidths: Array<KnockoutObservable<number>>;
    constructor() {
        super();
    }
    
    activate(args) {
        var widthUnit = 8;
        this.columnWidths = [
            ko.observable<number>(3*widthUnit),
            ko.observable<number>(widthUnit),
            ko.observable<number>(widthUnit),
            ko.observable<number>(widthUnit),
            ko.observable<number>(widthUnit),
            ko.observable<number>(widthUnit),
            ko.observable<number>(3* widthUnit),
            ko.observable<number>(widthUnit),
            ko.observable<number>(widthUnit),
            ko.observable<number>(widthUnit),
            ko.observable<number>(widthUnit),
            ko.observable<number>(widthUnit),
            ko.observable<number>(4*widthUnit)
        ];
        super.activate(args);

        this.activeDatabase.subscribe(() => this.fetchSqlReplicationStats());
        return this.fetchSqlReplicationStats();
    }

    fetchSqlReplicationStats(): JQueryPromise<sqlReplicationStatsDto[]> {
        var db = this.activeDatabase();
        if (db) {
            return new getStatusDebugSqlReplicationCommand(db)
                .execute()
                .done((results: sqlReplicationStatsDto[]) => {
                    this.data(results.map((x: sqlReplicationStatsDto)=> new sqlReplicationStats(x)));
            });
        }

        return null;
    }
}

export = statusDebugSqlReplication;
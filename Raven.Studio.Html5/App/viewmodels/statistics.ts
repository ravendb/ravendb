import activeDbViewModelBase = require("viewmodels/activeDbViewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import database = require("models/database");

class statistics extends activeDbViewModelBase {

    stats = ko.observable<databaseStatisticsDto>();

    constructor() {
        super();
    }

    activate(args) {
        super.activate(args);

        this.activeDatabase.subscribe(() => this.fetchStats());
        this.fetchStats();
    }

    fetchStats(): JQueryPromise<databaseStatisticsDto> {
        var db = this.activeDatabase();
        if (db) {
            return new getDatabaseStatsCommand(db)
                .execute()
                .done((result: databaseStatisticsDto) => this.stats(result));
        }

        return null;
    }
}

export = statistics;    
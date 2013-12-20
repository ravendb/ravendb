import getLogsCommand = require("commands/getLogsCommand");
import activeDbViewModelBase = require("viewmodels/activeDbViewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import database = require("models/database");
import moment = require("moment");

class logs extends activeDbViewModelBase {

    fetchedLogs = ko.observableArray<logDto>();

    activate(args) {
        super.activate(args);

        new getLogsCommand(this.activeDatabase())
            .execute()
            .done((results: logDto[]) => this.fetchedLogs(results));
    }
}

export = logs;
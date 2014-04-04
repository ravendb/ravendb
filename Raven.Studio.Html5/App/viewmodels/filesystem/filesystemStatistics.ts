import viewModelBase = require("viewmodels/viewModelBase");
import getFilesystemStatsCommand = require("commands/filesystem/getFilesystemStatsCommand");
import filesystem = require("models/filesystem/filesystem");
import moment = require("moment");

class filesystemStatistics extends viewModelBase {

    stats = ko.observable<filesystemStatisticsDto>();  

    fetchStats(): JQueryPromise<filesystemStatisticsDto> {
        var db = this.activeFilesystem();
        if (db) {
            return new getFilesystemStatsCommand(db)
                .execute()
                .done((result: filesystemStatisticsDto) => this.processStatsResults(result));
        }

        return null;
    }

    modelPolling() {
        this.fetchStats();
    }

    processStatsResults(results: filesystemStatisticsDto) {
        if (filesystemStatistics != null)
        {
            this.stats(results);
        }
            
    }
}

export = filesystemStatistics;
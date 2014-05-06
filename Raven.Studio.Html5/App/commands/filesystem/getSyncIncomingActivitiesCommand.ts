import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import synchronizationDetails = require("models/filesystem/synchronizationDetails");
import synchronizationReport = require("models/filesystem/synchronizationReport");
import pagedResultSet = require("common/pagedResultSet");

class getSyncIncomingActivitiesCommand extends commandBase {

    constructor(private fs: filesystem, private skip : number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {

        var args = {
            start: this.skip,
            pageSize: this.take
        };

        var task = $.Deferred();
        // Incoming: All the finished activities. 
        var url = "/synchronization/finished";

        this.query<filesystemListPageDto<synchronizationReport>>(url, args, this.fs)
            .done(x => {
                task.resolve(new pagedResultSet(x.Items.map(x => new synchronizationReport(x)), x.TotalCount));
            })
            .fail((xhr) => {
                this.reportError("Failed to get synchronization incoming activities.")
                task.reject(xhr);
            });

        return task;
    }
}

export = getSyncIncomingActivitiesCommand;
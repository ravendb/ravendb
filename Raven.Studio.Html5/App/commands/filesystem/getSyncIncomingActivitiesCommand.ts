import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import synchronizationDetails = require("models/filesystem/synchronizationDetails");
import synchronizationReport = require("models/filesystem/synchronizationReport");
import pagedResultSet = require("common/pagedResultSet");

class getSyncIncomingActivitiesCommand extends commandBase {

    //maxItems = 50;

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
        var pageSize = this.take;

        //if (this.skip < this.maxItems) {
            this.query<filesystemListPageDto<filesystemSynchronizationReportDto>>(url, args, this.fs)
                .done((x: filesystemListPageDto<filesystemSynchronizationReportDto>) => {
                    //var totalCount = x.TotalCount > 50 ? 50 : x.TotalCount;
                    //if (pageSize + x.Items.length <= this.maxItems) {
                    task.resolve(new pagedResultSet(x.Items.map(item => new synchronizationReport(item)), x.TotalCount));
                    //}
                    //else {
                    //    var itemsToTake = this.maxItems - this.skip;
                    //    task.resolve(new pagedResultSet(x.Items.slice(0, itemsToTake), this.maxItems));
                    //}
                })
                .fail((xhr) => {
                    this.reportError("Failed to get synchronization incoming activities.")
                        task.reject(xhr);
                });
        //}
        //else {
        //    task.resolve(new pagedResultSet([], this.maxItems));
        //}

        return task;
    }
}

export = getSyncIncomingActivitiesCommand;
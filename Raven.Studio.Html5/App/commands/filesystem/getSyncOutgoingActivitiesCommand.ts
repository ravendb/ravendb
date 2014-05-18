import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import synchronizationDetails = require("models/filesystem/synchronizationDetails");
import synchronizationDetail = require("models/filesystem/synchronizationDetail");
import pagedResultSet = require("common/pagedResultSet");

class getSyncOutgoingActivitiesCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<synchronizationDetail[]> {

        // Outgoing: All the pending and active activities. 
        var pendingTask = this.getPendingActivity(0, 1);
        var activeTask = this.getActiveActivity(0, 1);

        var doneTask = $.Deferred();
        var start = 0;
        var pageSize = 50;

        var combinedTask = $.when(pendingTask, activeTask);
        combinedTask.done((
            pendingList: filesystemListPageDto<filesystemSynchronizationDetailsDto>,
            activeList: filesystemListPageDto<filesystemSynchronizationDetailsDto>) => {

            if (start < pendingList.TotalCount - pageSize) {
                this.getPendingActivity(start, pageSize).done(x => doneTask.resolve(x.Items.map(x => new synchronizationDetail(x, "Pending"))));
            }
            else if (start > pendingList.TotalCount) {
                var activeListStart = start - pendingList.TotalCount;
                this.getActiveActivity(activeListStart, pageSize).done(x => doneTask.resolve(x.Items.map(x => new synchronizationDetail(x, "Active")), x.TotalCount));
            }
            else {
                var pendingPageSize = pendingList.TotalCount - start;
                var activePageSize = pageSize - pendingPageSize;

                var pendingTask = this.getPendingActivity(start, pendingPageSize);
                var activeTask = this.getActiveActivity(0, activePageSize);

                var combinedTask = $.when(pendingTask, activeTask);

                combinedTask.done( (x, y) => {
                        var page = [];
                        page.pushAll(x.Items.map(item => new synchronizationDetail(item, "Pending")));
                        page.pushAll(y.Items.map(item => new synchronizationDetail(item, "Active")));
                        doneTask.resolve(page);
                    });
            }
        });
        combinedTask.fail(xhr => {
            this.reportError("Failed to get synchronization outgoing activities.")
            doneTask.reject(xhr)
        });

        return doneTask;        
    }

    getPendingActivity(skip: number, take: number): JQueryPromise<filesystemListPageDto<filesystemSynchronizationDetailsDto>> {
        var pendingUrl = "/synchronization/pending";
        return this.query<filesystemListPageDto<filesystemSynchronizationDetailsDto>>(pendingUrl, { start: skip, pageSize: take }, this.fs, x => <filesystemListPageDto<filesystemSynchronizationDetailsDto>>x);
    }

    getActiveActivity(skip: number, take: number): JQueryPromise<filesystemListPageDto<filesystemSynchronizationDetailsDto>> {
        var activeUrl = "/synchronization/active";
        return this.query<filesystemListPageDto<filesystemSynchronizationDetailsDto>>(activeUrl, { start: skip, pageSize: take }, this.fs, x => <filesystemListPageDto<filesystemSynchronizationDetailsDto>>x);
    }
}

export = getSyncOutgoingActivitiesCommand; 
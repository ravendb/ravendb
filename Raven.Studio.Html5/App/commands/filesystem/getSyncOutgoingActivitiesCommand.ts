import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import synchronizationDetail = require("models/filesystem/synchronizationDetail");

class getSyncOutgoingActivitiesCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<synchronizationDetail[]> {

        // Outgoing: All the pending and active activities. 
        var pendingTask = this.getPendingActivity(0, 50);
        var activeTask = this.getActiveActivity(0, 50);

        var doneTask = $.Deferred();
        var start = 0;
        var pageSize = 50;

        var combinedTask = $.when(pendingTask, activeTask);
        combinedTask.done((
            pendingList: filesystemListPageDto<filesystemSynchronizationDetailsDto>,
            activeList: filesystemListPageDto<filesystemSynchronizationDetailsDto>) => {

            var activePageSize = pageSize - pendingList.TotalCount;

            var page = [];
            page.pushAll(pendingList.Items.map(item =>
                new synchronizationDetail({
                    FileSystemName: this.fs.name,
                    FileName: item.FileName,
                    DestinationFileSystemUrl: item.DestinationUrl,
                    SourceServerId: "",
                    SourceFileSystemUrl: "",
                    Type: filesystemSynchronizationType.Unknown,
                    Direction: item.Direction,
                    Action: synchronizationAction.Enqueue
                }, "Pending", item.Type)));

            if (activePageSize > 0) {
                page.pushAll(activeList.Items.slice(0, activePageSize - 1).map(item => 
                    new synchronizationDetail({
                        FileSystemName: this.fs.name,
                        FileName: item.FileName,
                        DestinationFileSystemUrl: item.DestinationUrl,
                        SourceServerId: "",
                        SourceFileSystemUrl: "",
                        Type: filesystemSynchronizationType.Unknown,
                        Direction: item.Direction,
                        Action: synchronizationAction.Start
                    }, "Active", item.Type)));
            }

            doneTask.resolve(page);
        });
        combinedTask.fail(xhr => {
            this.reportError("Failed to get synchronization outgoing activities.");
            doneTask.reject(xhr);
        });

        return doneTask;        
    }

    getPendingActivity(skip: number, take: number): JQueryPromise<filesystemListPageDto<filesystemSynchronizationDetailsDto>> {
        var pendingUrl = "/synchronization/pending";
        var resultsSelector = (x: filesystemListPageDto<filesystemSynchronizationDetailsDto>) => { x.Items.map((item: filesystemSynchronizationDetailsDto) => item.Direction = synchronizationDirection.Outgoing); return x; };
        return this.query<filesystemListPageDto<filesystemSynchronizationDetailsDto>>(pendingUrl, { start: skip, pageSize: take }, this.fs, resultsSelector);
    }

    getActiveActivity(skip: number, take: number): JQueryPromise<filesystemListPageDto<filesystemSynchronizationDetailsDto>> {
        var activeUrl = "/synchronization/active";
        var resultsSelector = (x: filesystemListPageDto<filesystemSynchronizationDetailsDto>) => { x.Items.map((item: filesystemSynchronizationDetailsDto) => item.Direction = synchronizationDirection.Outgoing); return x; };
        return this.query<filesystemListPageDto<filesystemSynchronizationDetailsDto>>(activeUrl, { start: skip, pageSize: take }, this.fs, resultsSelector);
    }
}

export = getSyncOutgoingActivitiesCommand; 
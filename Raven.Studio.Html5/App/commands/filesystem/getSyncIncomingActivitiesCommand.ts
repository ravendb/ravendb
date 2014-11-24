import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import synchronizationDetail = require("models/filesystem/synchronizationDetail");

class getSyncIncomingActivitiesCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<synchronizationDetail[]> {

        var doneTask = $.Deferred();
        var start = 0;
        var pageSize = 50;

        this.getIncomingActivity(start, pageSize)
            .done(x => doneTask.resolve(x.Items.map(x => new synchronizationDetail(x, "Pending", x.Type))))
            .fail((xhr) => {
                this.reportError("Failed to get synchronization incoming activities.");
                doneTask.reject(xhr);
            });

        return doneTask;
    }

    getIncomingActivity(skip: number, take: number): JQueryPromise<filesystemListPageDto<filesystemSynchronizationDetailsDto>> {
        var incomingUrl = "/synchronization/incoming";
        var resultsSelector = (x: filesystemListPageDto<filesystemSynchronizationDetailsDto>) => { x.Items.map((item: filesystemSynchronizationDetailsDto) => item.Direction = synchronizationDirection.Incoming); return x; };
        return this.query<filesystemListPageDto<filesystemSynchronizationDetailsDto>>(incomingUrl, { start: skip, pageSize: take }, this.fs, resultsSelector);
    }
}

export = getSyncIncomingActivitiesCommand; 
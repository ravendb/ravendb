import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import synchronizationDetails = require("models/filesystem/synchronizationDetails");
import synchronizationDetail = require("models/filesystem/synchronizationDetail");


class getSyncOutgoingActivitiesCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<any> {

        // Incoming: All the pending and active activities. 
        var pendingUrl = "/synchronization/pending";
        var activeUrl = "/synchronization/active";

        var pendingTask = this.query<filesystemListPageDto<filesystemSynchronizationDetailsDto>>(pendingUrl, null, this.fs);
        var activeTask = this.query<filesystemListPageDto<filesystemSynchronizationDetailsDto>>(activeUrl, null, this.fs);

        var doneTask = $.Deferred();

        var combinedTask = $.when(pendingTask, activeTask);
        combinedTask.done((
            pendingList: filesystemListPageDto<filesystemSynchronizationDetailsDto>,
            activeList: filesystemListPageDto<filesystemSynchronizationDetailsDto>) => {
            
            doneTask.resolve(synchronizationDetails.fromOutgoingActivities(pendingList.Items, activeList.Items));            
        });
        combinedTask.fail(xhr => doneTask.reject(xhr));

        return doneTask;        
    }
}

export = getSyncOutgoingActivitiesCommand; 
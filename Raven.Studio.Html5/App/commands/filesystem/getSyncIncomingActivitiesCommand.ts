import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import synchronizationDetails = require("models/filesystem/synchronizationDetails");
import synchronizationReport = require("models/filesystem/synchronizationReport");

class getSyncIncomingActivitiesCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<any> {

        // Incoming: All the finished activities. 
        var url = "/synchronization/finished";

        return this.query<filesystemListPageDto<filesystemSynchronizationReportDto>>(url, null, this.fs)
                 .then(x => synchronizationDetails.fromIncomingActivities(x.Items));
    }
}

export = getSyncIncomingActivitiesCommand;
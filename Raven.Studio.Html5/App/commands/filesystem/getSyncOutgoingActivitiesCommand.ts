import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import synchronizationDetail = require("models/filesystem/synchronizationDetail");

class getSyncOutgoingActivitiesCommand extends commandBase {

    constructor(private fs: filesystem, private activity: synchronizationActivity, private skip: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<filesystemListPageDto<synchronizationDetail>> {
    
        var doneTask = $.Deferred();
        this.getActivities()
            .done((list: filesystemListPageDto<synchronizationDetail>) => doneTask.resolve(list))
            .fail(xhr => {
                this.reportError(`Failed to get outgoing and ${this.activity} synchronization activities.`);
                doneTask.reject(xhr);
            });

        return doneTask;        
    }

    getActivities(): JQueryPromise<filesystemListPageDto<synchronizationDetail>> {
        var url: string;

        switch (this.activity) {
            case synchronizationActivity.Active:
                url = "/synchronization/active";
                break;
            case synchronizationActivity.Pending:
                url = "/synchronization/pending";
                break;
            default:
                throw new TypeError(`Not supported synchronization activity type when attempting to retrieve outgoing activities. Given type: ${this.activity}`);
        }

        var resultsSelector = (x: filesystemListPageDto<synchronizationDetailsDto>) => {

            return {
                TotalCount: x.TotalCount,
                Items: x.Items.map((item: synchronizationDetailsDto) => new synchronizationDetail(item, synchronizationDirection.Outgoing, this.activity))
            };
        };

        return this.query<filesystemListPageDto<synchronizationDetail>>(url, { start: this.skip, pageSize: this.take }, this.fs, resultsSelector, 15000);
    }
}

export = getSyncOutgoingActivitiesCommand; 

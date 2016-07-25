import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import synchronizationDetail = require("models/filesystem/synchronizationDetail");

class getSyncIncomingActivitiesCommand extends commandBase {

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
        var resultsSelector: (x: filesystemListPageDto<synchronizationDetailsDto | synchronizationReportDto>) => { TotalCount: number, Items: Array<synchronizationDetail> };

        switch (this.activity) {
            case synchronizationActivity.Active:
                url = "/synchronization/incoming";
                resultsSelector = (x: filesystemListPageDto<synchronizationDetailsDto>) => {

                    return {
                        TotalCount: x.TotalCount,
                        Items: x.Items.map((item: synchronizationDetailsDto) => new synchronizationDetail(item, synchronizationDirection.Incoming, this.activity))
                    };
                };
                break;
            case synchronizationActivity.Finished:
                url = "/synchronization/finished";
                resultsSelector = (x: filesystemListPageDto<synchronizationReportDto>) => {
                    return {
                        TotalCount: x.TotalCount,
                        Items: x.Items.map((item: synchronizationReportDto) => {

                            var detail = new synchronizationDetail({
                                FileName: item.FileName,
                                DestinationUrl: "",
                                FileETag: item.FileETag,
                                Type: item.Type
                            }, synchronizationDirection.Incoming, this.activity);

                            if (item.Exception != null) {
                                detail.AdditionalInfo(item.Exception);
                            }

                            return detail;
                        })
                    };
                };
                break;
            default:
                throw new TypeError(`Not supported synchronization activity type when attempting to retrieve incoming activities. Given type: ${this.activity}`);
        }

        return this.query<filesystemListPageDto<synchronizationDetail>>(url, { start: this.skip, pageSize: this.take }, this.fs, resultsSelector, 15000);
    }
}

export = getSyncIncomingActivitiesCommand; 

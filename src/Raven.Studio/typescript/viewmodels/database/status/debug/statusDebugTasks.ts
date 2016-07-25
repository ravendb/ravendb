import getStatusDebugTasksSummaryCommand = require("commands/getStatusDebugTasksSummaryCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");

class statusDebugTasks extends viewModelBase {
    data = ko.observable<taskMetadataSummaryDto[]>();

    detailsUrl: KnockoutComputed<string>;

    constructor() {
        super();
        this.detailsUrl = ko.computed(() => appUrl.forResourceQuery(this.activeDatabase()) + "/debug/tasks");
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('JHZ574');
        this.activeDatabase.subscribe(() => this.fetchTasks());
        return this.fetchTasks();
    }

    fetchTasks(): JQueryPromise<taskMetadataSummaryDto[]> {
        var db = this.activeDatabase();
        if (db) {
            return new getStatusDebugTasksSummaryCommand(db)
                .execute()
                .done((results: taskMetadataSummaryDto[]) => this.data(results));
        }

        return null;
    }
}

export = statusDebugTasks;

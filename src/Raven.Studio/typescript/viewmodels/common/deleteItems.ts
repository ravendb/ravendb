import document = require("models/database/documents/document");
import file = require("models/filesystem/file");
import counterSummary = require("models/counter/counterSummary");
import timeSeriesPoint = require("models/timeSeries/timeSeriesPoint");
import timeSeriesKey = require("models/timeSeries/timeSeriesKey");
import timeSeriesType = require("models/timeSeries/timeSeriesType");
import dialog = require("plugins/dialog");
import deleteDocumentsCommand = require("commands/database/documents/deleteDocumentsCommand");
import deleteFilesCommand = require("commands/filesystem/deleteFilesCommand");
import deleteCountersCommand = require("commands/counter/deleteCountersCommand");
import deletePointsCommand = require("commands/timeSeries/deletePointsCommand");
import deleteTypesCommand = require("commands/timeSeries/deleteTypesCommand");
import appUrl = require("common/appUrl");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import commandBase = require("commands/commandBase");

class deleteItems extends dialogViewModelBase {

    private items = ko.observableArray<documentBase>();
    private deletionStarted = false;
    public deletionTask = $.Deferred(); // Gives consumers a way to know when the async delete operation completes.

    constructor(items: Array<documentBase>, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);

        if (items.length === 0) {
            throw new Error("Must have at least one document to delete.");
        }

        this.items(items);
    }

    deleteItems() {
        var deleteItemsIds = this.items().map(i => i.getUrl());
        var deleteCommand: commandBase;
        var firstItem = this.items()[0];
        if (firstItem instanceof document) {
            deleteCommand = new deleteDocumentsCommand(deleteItemsIds, appUrl.getDatabase());
        }
        else if (firstItem instanceof file) {
            deleteCommand = new deleteFilesCommand(deleteItemsIds, appUrl.getFileSystem());
        }
        else if (firstItem instanceof counterSummary) {
            var counters: any = this.items();
            var groupAndNames: {groupName: string; counterName: string}[] = counters.map((x: counterSummary) => {
                return {
                    groupName: x.getEntityName(),
                    counterName: x.getId()
                }
            });
            deleteCommand = new deleteCountersCommand(groupAndNames, appUrl.getCounterStorage());
        } else if (firstItem instanceof timeSeriesPoint) {
            var points = this.items().map((x: timeSeriesPoint) => {
                return {
                    Type: x.type,
                    Key: x.key,
                    At: x.At
                };
            });
            deleteCommand = new deletePointsCommand(points, appUrl.getTimeSeries());
        } else if (firstItem instanceof timeSeriesKey) {
            debugger 
        } else if (firstItem instanceof timeSeriesType) {
            var types = this.items().map((type: timeSeriesType) => {
                return type.name;
            });
            deleteCommand = new deleteTypesCommand(types, appUrl.getTimeSeries());
        } else {
            debugger 
        }
        var deleteCommandTask = deleteCommand.execute();

        deleteCommandTask.done(() => this.deletionTask.resolve(this.items()));
        deleteCommandTask.fail((response: any) => this.deletionTask.reject(response));

        this.deletionStarted = true;
        dialog.close(this);
    }

    getDeletedItemName(): string {
        var item = this.items()[0];
        if (item instanceof counterSummary) {
            var summary: any = item;
            return " counter name: " + summary.getId() + ", group name: " + summary.getEntityName();
        }

        return item.getId();
    }

    cancel() {
        dialog.close(this);
    }

    deactivate(args:any) {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never carried it out.
        if (!this.deletionStarted) {
            this.deletionTask.reject();
        }
    }
}

export = deleteItems;

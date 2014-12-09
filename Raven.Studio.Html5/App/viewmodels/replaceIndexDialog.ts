import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/database");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import index = require("models/index");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import datePickerBindingHandler = require("common/datePickerBindingHandler");
import moment = require("moment");

class replaceIndexDialog extends dialogViewModelBase {

    static ModeStale = "stale";
    static ModeTime = "time";

    private indexes = ko.observableArray<index>();
    indexesExceptCurrent: KnockoutComputed<index[]>;
    private selectedIndex = ko.observable<string>();
    private replaceMode = ko.observable<string>(replaceIndexDialog.ModeStale);

    saveEnabled = ko.computed(() => {
        return !!this.selectedIndex();
    });

    private etag = ko.observable<string>();
    private replaceDate = ko.observable<Moment>(moment(new Date()));
    private replaceDateText = ko.computed(() => {
        return this.replaceDate() != null ? this.replaceDate().format("YYYY/MM/DD H:mm:ss") : "";
    });

    constructor(private indexName: string, private db: database, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);

        datePickerBindingHandler.install();

        this.indexesExceptCurrent = ko.computed(() => {
            return this.indexes().filter(index => index.name != this.indexName);
        });
    }

    canActivate(args: any): any {
        var deferred = $.Deferred();

        this.fetchIndexes()
            .done(() => deferred.resolve({ can: true }))
            .fail(() => deferred.resolve({ can: false }));

        return deferred.promise();
    }

    setSelectedIndex(name: string) {
        this.selectedIndex(name);
    }

    private fetchIndexes() {
        return new getDatabaseStatsCommand(this.db)
            .execute()
            .done((stats: databaseStatisticsDto) => this.processDbStats(stats));
    }

    changeReplaceMode(newMode: string) {
        this.replaceMode(newMode);
    }

    processDbStats(stats: databaseStatisticsDto) {
        this.indexes(stats.Indexes
            .map(i => new index(i)));
    }

    deleteScheduledReplace() {
        //TODO:
    }

    saveReplace() {
        //TODO:
        console.log('SAVING REPLACE');
    }

    close() {
        dialog.close(this);
    }
}

export = replaceIndexDialog; 
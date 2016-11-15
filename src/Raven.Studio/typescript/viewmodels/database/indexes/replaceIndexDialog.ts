import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import datePickerBindingHandler = require("common/bindingHelpers/datePickerBindingHandler");
import moment = require("moment");
import indexReplaceDocument = require("models/database/index/indexReplaceDocument");

class replaceIndexDialog extends dialogViewModelBase {

    replaceSettingsTask = $.Deferred();

    private etagMode = ko.observable<boolean>(false);
    private dateMode = ko.observable<boolean>(false);
    private lastIndexedEtag = ko.observable<string>();

    private replaceDate = ko.observable<moment.Moment>(moment(new Date()));
    private replaceDateText = ko.computed(() => {
        return this.replaceDate() != null ? this.replaceDate().format("YYYY/MM/DD H:mm:ss") : "";
    });

    constructor(private indexName: string, private db: database, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);
        datePickerBindingHandler.install();
    }

    canActivate(args: any): any {
        var deferred = $.Deferred();

        this.fetchIndexes()
            .done(() => deferred.resolve({ can: true }))
            .fail(() => deferred.resolve({ can: false }));

        return deferred.promise();
    }

    private fetchIndexes() {
        return new getDatabaseStatsCommand(this.db)
            .execute()
            .done(stats => this.processDbStats(stats));
    }

    processDbStats(stats: Raven.Client.Data.DatabaseStatistics) {
        var oldIndex = stats.Indexes.first(i => i.Name == this.indexName);
        //TODO: this.lastIndexedEtag(oldIndex.LastIndexedEtag);       
    }

    saveReplace() {
        var replaceDocument: indexReplaceDocument = new indexReplaceDocument({ IndexToReplace: this.indexName });

        if (this.etagMode()) {
            replaceDocument.minimumEtagBeforeReplace = this.lastIndexedEtag();
        }

        if (this.dateMode()) {
            replaceDocument.replaceTimeUtc = this.replaceDate().toISOString();
        }

        this.replaceSettingsTask.resolve(replaceDocument);
    }

    close() {
        this.replaceSettingsTask.reject();
        dialog.close(this);
    }

    toggleEtagMode() {
        this.etagMode(!this.etagMode());
    }

    toggleDateMode() {
        this.dateMode(!this.dateMode());
    }
}

export = replaceIndexDialog; 

import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/database");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import index = require("models/index");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import datePickerBindingHandler = require("common/datePickerBindingHandler");
import moment = require("moment");
import saveDocumentCommand = require("commands/saveDocumentCommand");
import indexReplaceDocument = require("models/indexReplaceDocument");
import messagePublisher = require("common/messagePublisher");

class replaceIndexDialog extends dialogViewModelBase {

    static ModeEtag = "etag";
    static ModeTime = "time";

    replaceSettingsTask = $.Deferred();

    private replaceMode = ko.observable<string>(replaceIndexDialog.ModeEtag);
    private lastIndexedEtag = ko.observable<string>();

    private extraMode = ko.observable<boolean>(false);

    private etag = ko.observable<string>();
    private replaceDate = ko.observable<Moment>(moment(new Date()));
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
            .done((stats: databaseStatisticsDto) => this.processDbStats(stats));
    }

    changeReplaceMode(newMode: string) {
        this.replaceMode(newMode);
    }

    processDbStats(stats: databaseStatisticsDto) {
        var oldIndex = stats.Indexes.first(i => i.Name == this.indexName);
        this.lastIndexedEtag(oldIndex.LastIndexedEtag);       
    }

    saveReplace() {
        var replaceDocument: indexReplaceDocument = null;
        if (this.extraMode()) {
            replaceDocument = new indexReplaceDocument({ IndexToReplace: this.indexName });
        } else {
            switch (this.replaceMode()) {
                case replaceIndexDialog.ModeTime:
                    replaceDocument = new indexReplaceDocument({ IndexToReplace: this.indexName, ReplaceTimeUtc: this.replaceDate().toISOString() });
                    break;
                case replaceIndexDialog.ModeEtag:
                    replaceDocument = new indexReplaceDocument({ IndexToReplace: this.indexName, MinimumEtagBeforeReplace: this.etag() });
                    break;
            }
        }
        this.replaceSettingsTask.resolve(replaceDocument);
    }

    close() {
        this.replaceSettingsTask.reject();
        dialog.close(this);
    }

    toggleExtraModes() {
        this.extraMode(!this.extraMode());
    }
}

export = replaceIndexDialog; 
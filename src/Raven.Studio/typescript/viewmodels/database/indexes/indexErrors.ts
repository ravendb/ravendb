import viewModelBase = require("viewmodels/viewModelBase");
import moment = require("moment");
import getIndexesErrorCommand = require("commands/database/debug/getIndexesErrorCommand");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import appUrl = require("common/appUrl");

class IndexErrors extends viewModelBase {

    allIndexErrors = ko.observableArray<IndexErrorPerDocument>();
    hasFetchedErrors = ko.observable(false);
    now = ko.observable<moment.Moment>();
    attachmentsColumns: virtualColumn[];
    private gridController = ko.observable<virtualGridController<IndexErrorPerDocument>>();
    private columnPreview = new columnPreviewPlugin<IndexErrorPerDocument>();

    constructor() {
        super();
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('ABUXGF');
    }

    compositionComplete() {
        const grid = this.gridController();
        grid.headerVisible(true);
        this.attachmentsColumns = [
            new hyperlinkColumn<IndexErrorPerDocument>(x => x.IndexName, x => appUrl.forQuery(this.activeDatabase(), x.IndexName), "Index name", "25%"),
            new hyperlinkColumn<IndexErrorPerDocument>(x => x.Document, x => appUrl.forEditDoc(x.Document, this.activeDatabase()), "Document id", "25%"),
            new textColumn<IndexErrorPerDocument>(x => x.Timestamp, "Timestamp", "25%"),
            new textColumn<IndexErrorPerDocument>(x => x.Error, "Error", "25%")
        ];
        grid.init((s, t) => this.fetchIndexErrors(s, t), () => {
            return this.attachmentsColumns;
        });

        this.columnPreview.install("virtual-grid", ".tooltip", (indexError: IndexErrorPerDocument, column: textColumn<IndexErrorPerDocument>, e: JQueryEventObject, onValue: (context: any) => void) => {
            const value = column.getCellValue(indexError);
            if (!_.isUndefined(value)) {
                onValue(value);
            }
        });
       
    }

    fetchIndexErrors(start: number, skip: number): JQueryPromise<pagedResult<IndexErrorPerDocument>> {
        const deferred = $.Deferred<pagedResult<IndexErrorPerDocument>>();
        new getIndexesErrorCommand(this.activeDatabase())
            .execute()
            .done((result: Raven.Client.Documents.Indexes.IndexErrors[]) => {
                const indexErrorArray: IndexErrorPerDocument[] = [];
                result.forEach((indexErrors: Raven.Client.Documents.Indexes.IndexErrors) => {
                    indexErrors.Errors.forEach((error: Raven.Client.Documents.Indexes.IndexingError) => {
                        const indexErrorPerDocument:IndexErrorPerDocument = {
                            Timestamp: this.createHumanReadableTime(error.Timestamp)(),
                            Document: error.Document,
                            Error: error.Error,
                            IndexName: indexErrors.Name

                    };
                        indexErrorArray.push(indexErrorPerDocument);
                    });
                });
                this.allIndexErrors(indexErrorArray);
                this.hasFetchedErrors(true);
                deferred.resolve({
                    items: this.allIndexErrors(),
                    totalResultCount: this.allIndexErrors().length
                });
            });
        return deferred.promise();
    }

    createHumanReadableTime(time: string): KnockoutComputed<string> {
        if (time) {
            // Return a computed that returns a humanized string based off the current time, e.g. "7 minutes ago".
            // It's a computed so that it updates whenever we update this.now field.
            return ko.computed(() => {
                const dateMoment = moment(time);
                const agoInMs = dateMoment.diff(this.now());
                return moment.duration(agoInMs).humanize(true) + dateMoment.format(" (MM/DD/YY, h:mma)");
            });
        }

        return ko.pureComputed(() => time);
    }
}

export = IndexErrors; 

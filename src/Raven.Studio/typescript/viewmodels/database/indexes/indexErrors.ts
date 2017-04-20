import viewModelBase = require("viewmodels/viewModelBase");
import moment = require("moment");
import getIndexesErrorCommand = require("commands/database/index/getIndexesErrorCommand");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import appUrl = require("common/appUrl");

class indexErrors extends viewModelBase {

    allIndexErrors: IndexErrorPerDocument[] = null; 
    private gridController = ko.observable<virtualGridController<IndexErrorPerDocument>>();
    private columnPreview = new columnPreviewPlugin<IndexErrorPerDocument>(); 
    searchText = ko.observable<string>();

    constructor() {
        super();
        this.initObservables();
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('ABUXGF');
    }

    compositionComplete() {
        const grid = this.gridController();
        grid.headerVisible(true);
        grid.init((s, t) => this.fetchIndexErrors(s, t), () => 
            [
                new hyperlinkColumn<IndexErrorPerDocument>(x => x.IndexName, x => appUrl.forQuery(this.activeDatabase(), x.IndexName), "Index name", "25%"),
                new hyperlinkColumn<IndexErrorPerDocument>(x => x.Document, x => appUrl.forEditDoc(x.Document, this.activeDatabase()), "Document id", "25%"),
                new textColumn<IndexErrorPerDocument>(x => x.Timestamp, "Timestamp", "25%"),
                new textColumn<IndexErrorPerDocument>(x => x.Error, "Error", "25%")
            ]
        );

        this.columnPreview.install("virtual-grid", ".tooltip", (indexError: IndexErrorPerDocument, column: textColumn<IndexErrorPerDocument>, e: JQueryEventObject, onValue: (context: any) => void) => {
            const value = column.getCellValue(indexError);
            if (!_.isUndefined(value)) {
                onValue(value);
            }
        });
       
    }
  
    private initObservables() {
        this.searchText.throttle(200).subscribe(() => this.filterIndexes());
    }

    fetchIndexErrors(start: number, skip: number): JQueryPromise<pagedResult<IndexErrorPerDocument>> {
        if (this.allIndexErrors === null) {
            return this.fetchRemoteIndexesError().then(list => this.filterItems(list));
        }

        return this.filterItems(this.allIndexErrors);
    }

    fetchRemoteIndexesError(): JQueryPromise<IndexErrorPerDocument[]> {
        const deferred = $.Deferred<IndexErrorPerDocument[]>();
        new getIndexesErrorCommand(this.activeDatabase())
            .execute()
            .fail((result: JQueryXHR) => this.reportError("getIndexesErrorCommand failed to get index errors",
                result.responseText,
                result.statusText))
            .done((result: Raven.Client.Documents.Indexes.IndexErrors[]) => {
                this.allIndexErrors = this.mapItems(result);
                return deferred.resolve(this.allIndexErrors);
            });

        return deferred.promise();
    }

    filterItems(list: IndexErrorPerDocument[]): JQueryPromise<pagedResult<IndexErrorPerDocument>> {
        const deferred = $.Deferred<pagedResult<IndexErrorPerDocument>>();
        let filteredItems = list;
        if (this.searchText()) {
            filteredItems = list.filter((error) => {
                return (error.Document.toLowerCase().indexOf(this.searchText().toLowerCase()) !== -1 || 
                    error.Error.toLowerCase().indexOf(this.searchText().toLowerCase()) !== -1);
            });
        }

        return deferred.resolve({
            items: filteredItems,
            totalResultCount: filteredItems.length
        });
    }

    private reportError(title: string, details?: string, httpStatusText?: string, displayInRecentErrors: boolean = true) {
        console.error(title, details, httpStatusText);
    }

    private mapItems(indexErrors: Raven.Client.Documents.Indexes.IndexErrors[]): IndexErrorPerDocument[] {
        return _.flatMap(indexErrors, (value) => {
            return value.Errors.map((error: Raven.Client.Documents.Indexes.IndexingError) =>
                ({
                    Timestamp: this.createHumanReadableTime(error.Timestamp)(),
                    Document: error.Document,
                    Error: error.Error,
                    IndexName: value.Name
                } as IndexErrorPerDocument));
        });
    }

    private filterIndexes() {
        this.gridController().reset();
    }

    createHumanReadableTime(time: string): KnockoutComputed<string> {
        if (time) {
            // Return a computed that returns a humanized string based off the current time, e.g. "7 minutes ago".
            // It's a computed so that it updates whenever we update this.now field.
            return ko.pureComputed(() => {
                const dateMoment = moment(time);
                const agoInMs = dateMoment.diff(moment.now());
                return moment.duration(agoInMs).humanize(true) + dateMoment.format(" (MM/DD/YY, h:mma)");
            });
        }

        return ko.pureComputed(() => time);
    }
}

export = indexErrors; 

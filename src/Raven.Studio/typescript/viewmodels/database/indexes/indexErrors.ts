import viewModelBase = require("viewmodels/viewModelBase");
import moment = require("moment");
import getIndexesErrorCommand = require("commands/database/index/getIndexesErrorCommand");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import appUrl = require("common/appUrl");
import timeHelpers = require("common/timeHelpers");

class indexErrors extends viewModelBase {

    private allIndexErrors: IndexErrorPerDocument[] = null; 
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
        super.compositionComplete();
        const grid = this.gridController();
        grid.headerVisible(true);
        grid.init((s, t) => this.fetchIndexErrors(s, t), () => 
            [
                new hyperlinkColumn<IndexErrorPerDocument>(x => x.IndexName, x => appUrl.forQuery(this.activeDatabase(), x.IndexName), "Index name", "25%"),
                new hyperlinkColumn<IndexErrorPerDocument>(x => x.Document, x => appUrl.forEditDoc(x.Document, this.activeDatabase()), "Document id", "25%"),
                new textColumn<IndexErrorPerDocument>(x => this.formatTimestampAsAgo(x.Timestamp), "Timestamp", "25%"),
                new textColumn<IndexErrorPerDocument>(x => x.Error, "Error", "25%")
            ]
        );

        this.columnPreview.install("virtual-grid", ".tooltip", (indexError: IndexErrorPerDocument, column: textColumn<IndexErrorPerDocument>, e: JQueryEventObject, onValue: (context: any) => void) => {
            if (column.header === "Timestamp") {
                // for timestamp show 'raw' date in tooltip
                onValue(indexError.Timestamp);
            } else {
                const value = column.getCellValue(indexError);
                if (!_.isUndefined(value)) {
                    onValue(value);
                }
            }
        });

        this.registerDisposable(timeHelpers.utcNowWithMinutePrecision.subscribe(() => this.onTick()));
    }
  
    private initObservables() {
        this.searchText.throttle(200).subscribe(() => this.filterIndexes());
    }

    private onTick() {
        // reset grid on tick - it neighter move scroll position not download data from remote, but it will render contents again, updating time 
        this.gridController().reset(false);
    }

    private fetchIndexErrors(start: number, skip: number): JQueryPromise<pagedResult<IndexErrorPerDocument>> {
        if (this.allIndexErrors === null) {
            return this.fetchRemoteIndexesError().then(list => {
                this.allIndexErrors = list;
                return this.filterItems(this.allIndexErrors);
            });
        }

        return this.filterItems(this.allIndexErrors);
    }

    private fetchRemoteIndexesError(): JQueryPromise<IndexErrorPerDocument[]> {
        return new getIndexesErrorCommand(this.activeDatabase())
            .execute()
            .then((result: Raven.Client.Documents.Indexes.IndexErrors[]) => this.mapItems(result));
    }

    private filterItems(list: IndexErrorPerDocument[]): JQueryPromise<pagedResult<IndexErrorPerDocument>> {
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

    private mapItems(indexErrors: Raven.Client.Documents.Indexes.IndexErrors[]): IndexErrorPerDocument[] {
        return _.flatMap(indexErrors, value => {
            return value.Errors.map((error: Raven.Client.Documents.Indexes.IndexingError) =>
                ({
                    Timestamp: error.Timestamp,
                    Document: error.Document,
                    Error: error.Error,
                    IndexName: value.Name
                } as IndexErrorPerDocument));
        });
    }

    private filterIndexes() {
        this.gridController().reset();
    }

    private formatTimestampAsAgo(time: string): string {
        const dateMoment = moment.utc(time).local();
        const ago = dateMoment.diff(moment());
        return moment.duration(ago).humanize(true) + dateMoment.format(" (MM/DD/YY, h:mma)");
    }
}

export = indexErrors; 

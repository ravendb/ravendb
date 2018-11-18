import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import moment = require("moment");
import getIndexesErrorCommand = require("commands/database/index/getIndexesErrorCommand");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import appUrl = require("common/appUrl");
import timeHelpers = require("common/timeHelpers");
import awesomeMultiselect = require("common/awesomeMultiselect");
import indexErrorDetails = require("viewmodels/database/indexes/indexErrorDetails");
import generalUtils = require("common/generalUtils");

type indexNameAndCount = {
    indexName: string;
    count: number;
}

class indexErrors extends viewModelBase {

    private allIndexErrors: IndexErrorPerDocument[] = null;
    private filteredIndexErrors: IndexErrorPerDocument[] = null;
    private gridController = ko.observable<virtualGridController<IndexErrorPerDocument>>();
    private columnPreview = new columnPreviewPlugin<IndexErrorPerDocument>();

    private allErroredIndexNames = ko.observableArray<indexNameAndCount>([]);
    private selectedIndexNames = ko.observableArray<string>([]);
    private ignoreSearchCriteriaUpdatesMode = false;
    searchText = ko.observable<string>();

    private localLatestIndexErrorTime = ko.observable<string>(null);
    private remoteLatestIndexErrorTime = ko.observable<string>(null);

    private isDirty: KnockoutComputed<boolean>;

    constructor() {
        super();
        this.initObservables();
    }

    private initObservables() {
        this.searchText.throttle(200).subscribe(() => this.onSearchCriteriaChanged());
        this.selectedIndexNames.subscribe(() => this.onSearchCriteriaChanged());

        this.isDirty = ko.pureComputed(() => {
            const local = this.localLatestIndexErrorTime();
            const remote = this.remoteLatestIndexErrorTime();

            return local !== remote;
        });
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('ABUXGF');
    }

    attached() {
        super.attached();

        awesomeMultiselect.build($("#visibleIndexesSelector"), opts => {
            opts.enableHTML = true;
            opts.optionLabel = (element: HTMLOptionElement) => {
                const indexName = $(element).text();
                const indexItem = this.allErroredIndexNames().find(x => x.indexName === indexName);
                return `<span class="name">${generalUtils.escape(indexName)}</span><span class="badge">${indexItem.count}</span>`;
            };
        });
    }

    private syncMultiSelect() {
        awesomeMultiselect.rebuild($("#visibleIndexesSelector"));
    }

    protected afterClientApiConnected() {
        this.addNotification(this.changesContext.databaseNotifications().watchAllDatabaseStatsChanged(stats => this.onStatsChanged(stats)));
    }

    compositionComplete() {
        super.compositionComplete();
        const grid = this.gridController();
        grid.headerVisible(true);
        grid.init((s, t) => this.fetchIndexErrors(s, t), () =>
            [
                new actionColumn<IndexErrorPerDocument>(grid, (error, index) => this.showErrorDetails(index), "Show", `<i class="icon-preview"></i>`, "72px",
                    {
                        title: () => 'Show indexing error details'
                    }),
                new hyperlinkColumn<IndexErrorPerDocument>(grid, x => x.IndexName, x => appUrl.forEditIndex(x.IndexName, this.activeDatabase()), "Index name", "25%"),
                new hyperlinkColumn<IndexErrorPerDocument>(grid, x => x.Document, x => appUrl.forEditDoc(x.Document, this.activeDatabase()), "Document Id", "20%"),
                new textColumn<IndexErrorPerDocument>(grid, x => generalUtils.formatUtcDateAsLocal(x.Timestamp), "Date", "20%"),
                new textColumn<IndexErrorPerDocument>(grid, x => x.Action, "Action", "10%"),
                new textColumn<IndexErrorPerDocument>(grid, x => x.Error, "Error", "15%")
            ]
        );

        this.columnPreview.install("virtual-grid", ".js-index-errors-tooltip", 
            (indexError: IndexErrorPerDocument, column: textColumn<IndexErrorPerDocument>, e: JQueryEventObject, 
             onValue: (context: any, valueToCopy?: string) => void) => {
            if (column.header === "Action" || column.header === "Show") {
                // do nothing
            } else if (column.header === "Date") {
                onValue(moment.utc(indexError.Timestamp), indexError.Timestamp);
            } else {
                const value = column.getCellValue(indexError);
                if (!_.isUndefined(value)) {
                    onValue(value);
                }
            }
        });
        this.registerDisposable(timeHelpers.utcNowWithMinutePrecision.subscribe(() => this.onTick()));
        this.syncMultiSelect();
    }

    private showErrorDetails(errorIdx: number) {
        const view = new indexErrorDetails(this.filteredIndexErrors, errorIdx);
        app.showBootstrapDialog(view);
    }

    refresh() {
        this.allIndexErrors = null;
        this.gridController().reset(true);
    }

    private onTick() {
        // reset grid on tick - it neighter move scroll position not download data from remote, but it will render contents again, updating time 
        this.gridController().reset(false);
    }

    private fetchIndexErrors(start: number, take: number): JQueryPromise<pagedResult<IndexErrorPerDocument>> {
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
            .then((result: Raven.Client.Documents.Indexes.IndexErrors[]) => {
                this.ignoreSearchCriteriaUpdatesMode = true;

                const indexNamesAndCount = this.extractIndexNamesAndCount(result);

                this.allErroredIndexNames(indexNamesAndCount);
                this.selectedIndexNames(this.allErroredIndexNames().map(x => x.indexName).slice());
                this.syncMultiSelect();

                this.ignoreSearchCriteriaUpdatesMode = false;

                const mappedItems = this.mapItems(result);
                const itemWithMax = _.maxBy<IndexErrorPerDocument>(mappedItems, x => x.Timestamp);
                this.localLatestIndexErrorTime(itemWithMax ? itemWithMax.Timestamp : null);
                this.remoteLatestIndexErrorTime(this.localLatestIndexErrorTime());
                return mappedItems;
            });
    }

    private filterItems(list: IndexErrorPerDocument[]): JQueryPromise<pagedResult<IndexErrorPerDocument>> {
        const deferred = $.Deferred<pagedResult<IndexErrorPerDocument>>();
        let filteredItems = list;
        if (this.selectedIndexNames().length !== this.allErroredIndexNames().length) {
            filteredItems = filteredItems.filter(error => _.includes(this.selectedIndexNames(), error.IndexName));
        }

        if (this.searchText()) {
            const searchText = this.searchText().toLowerCase();
            
            filteredItems = filteredItems.filter((error) => {
                return (error.Document && error.Document.toLowerCase().includes(searchText)) ||
                       error.Error.toLowerCase().includes(searchText)
           })
        }
        
        // save copy used for details viewer
        this.filteredIndexErrors = filteredItems;
        
        return deferred.resolve({
            items: filteredItems,
            totalResultCount: filteredItems.length
        });
    }

    private extractIndexNamesAndCount(indexErrors: Raven.Client.Documents.Indexes.IndexErrors[]): Array<indexNameAndCount> {
        return indexErrors.filter(error => error.Errors.length > 0).map(errors => {
            return {
                indexName: errors.Name,
                count: errors.Errors.length
            }
        });
    }

    private mapItems(indexErrors: Raven.Client.Documents.Indexes.IndexErrors[]): IndexErrorPerDocument[] {
        const mappedItems = _.flatMap(indexErrors, value => {
            return value.Errors.map((error: Raven.Client.Documents.Indexes.IndexingError) =>
                ({
                    Timestamp: error.Timestamp,
                    Document: error.Document,
                    Action: error.Action,
                    Error: error.Error,
                    IndexName: value.Name
                } as IndexErrorPerDocument));
        });
        
        return _.orderBy(mappedItems, [x => x.Timestamp], ["desc"]);
    }

    private onStatsChanged(stats: Raven.Server.NotificationCenter.Notifications.DatabaseStatsChanged) {
        this.remoteLatestIndexErrorTime(stats.LastIndexingErrorTime);
    }

    private onSearchCriteriaChanged() {
        if (!this.ignoreSearchCriteriaUpdatesMode) {
            this.gridController().reset();
        }
    }
}

export = indexErrors; 

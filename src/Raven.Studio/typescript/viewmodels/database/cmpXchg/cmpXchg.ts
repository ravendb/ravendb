import router = require("plugins/router");
import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import eventsCollector = require("common/eventsCollector");
import deleteCompareExchangeConfirm = require("viewmodels/database/documents/deleteCompareExchangeConfirm");
import deleteCompareExchangeProgress = require("viewmodels/database/documents/deleteCompareExchangeProgress");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import getCompareExchangeItemsCommand = require("commands/database/cmpXchg/getCompareExchangeItemsCommand");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import checkedColumn = require("widgets/virtualGrid/columns/checkedColumn");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import continueTest = require("common/shell/continueTest");

class cmpXchg extends viewModelBase {

    filter = ko.observable<string>();
    deleteEnabled: KnockoutComputed<boolean>;
    selectedItemsCount: KnockoutComputed<number>;
    private nextItemToFetchIndex = undefined as number;

    private gridController = ko.observable<virtualGridController<Raven.Server.Web.System.CompareExchangeHandler.CompareExchangeListItem>>();
    private columnPreview = new columnPreviewPlugin<Raven.Server.Web.System.CompareExchangeHandler.CompareExchangeListItem>();

    spinners = {
        delete: ko.observable<boolean>(false)
    };

    constructor() {
        super();

        this.initObservables();
        this.filter.throttle(500).subscribe(() => this.filterLogEntries());
    }
    
    private filterLogEntries() {
        this.nextItemToFetchIndex = 0;
        this.gridController().reset(true);
    }

    private initObservables() {
        this.selectedItemsCount = ko.pureComputed(() => {
            let selectedDocsCount = 0;
            const controll = this.gridController();
            if (controll) {
                selectedDocsCount = controll.selection().count;
            }
            return selectedDocsCount;
        });
        
        this.deleteEnabled = ko.pureComputed(() => {
            const deleteInProgress = this.spinners.delete();
            const selectedDocsCount = this.selectedItemsCount();

            return !deleteInProgress && selectedDocsCount > 0;
        });
    }

    activate(args: any) {
        super.activate(args);

        continueTest.default.init(args);
    }

    fetchItems(skip: number): JQueryPromise<pagedResult<Raven.Server.Web.System.CompareExchangeHandler.CompareExchangeListItem>> {
        const task = $.Deferred<pagedResult<Raven.Server.Web.System.CompareExchangeHandler.CompareExchangeListItem>>();

        new getCompareExchangeItemsCommand(this.activeDatabase(), this.filter(), this.nextItemToFetchIndex || 0, 101)
            .execute()
            .done(result => {
                const hasMore = result.items.length === 101;
                const totalCount = skip + result.items.length;
                if (hasMore) {
                    result.items.pop();
                    this.nextItemToFetchIndex = skip + result.items.length;
                }

                task.resolve({
                    totalResultCount: totalCount,
                    items: result.items
                });
            });

        return task;
    }
    
    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();

        grid.headerVisible(true);

        const checkColumn = new checkedColumn(true); 
        const keyColumn = new hyperlinkColumn<Raven.Server.Web.System.CompareExchangeHandler.CompareExchangeListItem>(grid, x => x.Key, x => appUrl.forEditCmpXchg(x.Key, this.activeDatabase()), "Key", "20%");
        const ValueColumn = new textColumn<Raven.Server.Web.System.CompareExchangeHandler.CompareExchangeListItem>(grid, x => x.Value.Object, "Value", "20%");
        const MetadataColumn = new textColumn<Raven.Server.Web.System.CompareExchangeHandler.CompareExchangeListItem>(grid, x => x.Value["@metadata"], "Metadata", "20%");

        const gridColumns = this.isReadOnlyAccess() ? [keyColumn, ValueColumn, MetadataColumn] : [checkColumn, keyColumn, ValueColumn, MetadataColumn];        
        grid.init((s, _) => this.fetchItems(s), () => gridColumns);
        
         this.columnPreview.install(".js-cmp-xchg-grid", ".js-cmp-xchg-tooltip", 
             (doc: Raven.Server.Web.System.CompareExchangeHandler.CompareExchangeListItem, column: virtualColumn, e: JQueryEventObject, onValue: (context: any, valueToCopy: string) => void) => {
            if (column instanceof textColumn) {
                const value = column.getCellValue(doc);
                if (!_.isUndefined(value)) {
                    const json = JSON.stringify(value, null, 4);
                    const html = Prism.highlight(json, (Prism.languages as any).javascript);
                    onValue(html, json);
                }
            }
        });
    }

    newItem($event: JQueryEventObject) {
        eventsCollector.default.reportEvent("cmpXchg", "new");
        const url = appUrl.forNewCmpXchg(this.activeDatabase());
        if ($event.ctrlKey) {
            window.open(url);
        } else {
            router.navigate(url);
        }
    }

    deleteSelected() {
        const selection = this.gridController().getSelectedItems();
        if (selection.length === 0) {
            throw new Error("No elements to delete");
        }
        
        const rawSelection = this.gridController().selection();
        if (rawSelection.mode === "exclusive" && !rawSelection.excluded.length && !rawSelection.included.length) {
            // this is special case - user select all values, with out any exclusions, suggest deleting all cmpXchg values
            // (including items which wasn't downloaded yet)
            this.confirmationMessage("Are you sure?", "Deleting <strong>ALL</strong> compare exchange items.", { html: true, buttons: ["Cancel", "Delete All"] })
                .done(result => {
                    if (result.can) {
                        this.spinners.delete(true);
                        
                        new getCompareExchangeItemsCommand(this.activeDatabase(), this.filter(), 0, 2147483647)
                            .execute()
                            .done(allValues => {
                                const deleteProgress = new deleteCompareExchangeProgress(allValues.items, this.activeDatabase());

                                deleteProgress.start()
                                    .always(() => this.onDeleteCompleted());
                            })
                    }
                })
        } else {
            eventsCollector.default.reportEvent("cmpXchg", "delete");

            const deleteDialog = new deleteCompareExchangeConfirm(selection.map(x => x.Key));

            app.showBootstrapDialog(deleteDialog)
                .done((deleting: boolean) => {
                    if (deleting) {
                        this.spinners.delete(true);

                        const deleteProgress = new deleteCompareExchangeProgress(selection, this.activeDatabase());

                        deleteProgress.start()
                            .always(() => this.onDeleteCompleted());
                    }
                });
        }
    }

    private onDeleteCompleted() {
        this.spinners.delete(false);
        this.nextItemToFetchIndex = 0;
        this.gridController().reset(true);
    }
}

export = cmpXchg;

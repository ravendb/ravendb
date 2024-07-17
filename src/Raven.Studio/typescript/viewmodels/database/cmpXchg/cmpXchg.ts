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
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import { highlight, languages } from "prismjs";
import shardViewModelBase from "viewmodels/shardViewModelBase";
import database from "models/resources/database";

class cmpXchg extends shardViewModelBase {

    view = require("views/database/cmpXchg/cmpXchg.html");

    filter = ko.observable<string>();
    deleteEnabled: KnockoutComputed<boolean>;
    selectedItemsCount: KnockoutComputed<number>;
    private nextItemToFetchIndex = undefined as number;

    private gridController = ko.observable<virtualGridController<Raven.Server.Web.System.Processors.CompareExchange.CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem>>();
    private columnPreview = new columnPreviewPlugin<Raven.Server.Web.System.Processors.CompareExchange.CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem>();

    spinners = {
        delete: ko.observable<boolean>(false)
    };

    clientVersion = viewModelBase.clientVersion;

    constructor(db: database) {
        super(db);

        this.initObservables();
        this.filter.throttle(500).subscribe(() => this.filterLogEntries());
    }
    
    private filterLogEntries(): void {
        this.nextItemToFetchIndex = 0;
        this.gridController().reset(true);
    }

    private initObservables(): void {
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

    fetchItems(skip: number): JQueryPromise<pagedResult<Raven.Server.Web.System.Processors.CompareExchange.CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem>> {
        const task = $.Deferred<pagedResult<Raven.Server.Web.System.Processors.CompareExchange.CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem>>();

        new getCompareExchangeItemsCommand(this.db, this.filter(), this.nextItemToFetchIndex || 0, 101)
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
        const keyColumn = new hyperlinkColumn<Raven.Server.Web.System.Processors.CompareExchange.CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem>(grid, x => x.Key, x => appUrl.forEditCmpXchg(x.Key, this.db), "Key", "20%", {
            sortable: "string"
        });
        const valueColumn = new textColumn<Raven.Server.Web.System.Processors.CompareExchange.CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem>(grid, x => x.Value.Object, "Value", "20%", {
            sortable: "string"
        });
        const metadataColumn = new textColumn<Raven.Server.Web.System.Processors.CompareExchange.CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem>(grid, x => x.Value["@metadata"], "Metadata", "20%", {
            sortable: "string"
        });
        const raftIndexColumn = new textColumn<Raven.Server.Web.System.Processors.CompareExchange.CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem>(grid, x => x.Index, "Raft Index", "20%", {
            sortable: "number"
        });

        const editColumn = new actionColumn<Raven.Server.Web.System.Processors.CompareExchange.CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem>(grid,
            x => this.editItem(x.Key),
            "Edit",
            `<i class="icon-edit"></i>`,
            "50px",
            { title: () => "Edit this compare exchange item" })
        
        const gridColumns = this.isReadOnlyAccess() ? [keyColumn, valueColumn, metadataColumn, raftIndexColumn] :
                                                      [checkColumn, editColumn, keyColumn, valueColumn, metadataColumn, raftIndexColumn];
        grid.init((s) => this.fetchItems(s), () => gridColumns);
        
         this.columnPreview.install(".js-cmp-xchg-grid", ".js-cmp-xchg-tooltip", 
             (doc: Raven.Server.Web.System.Processors.CompareExchange.CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem, column: virtualColumn, e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy: string) => void) => {
            if (column instanceof textColumn) {
                const value = column.getCellValue(doc);
                if (value !== undefined) {
                    const json = JSON.stringify(value, null, 4);
                    const html = highlight(json, languages.javascript, "js");
                    onValue(html, json);
                }
            }
        });
    }

    newItem($event: JQuery.TriggeredEvent): void {
        eventsCollector.default.reportEvent("cmpXchg", "new");
        const url = appUrl.forNewCmpXchg(this.db);
        if ($event.ctrlKey) {
            window.open(url);
        } else {
            router.navigate(url);
        }
    }

    deleteSelected(): void {
        const selection = this.gridController().getSelectedItems();
        if (selection.length === 0) {
            throw new Error("No elements to delete");
        }
        
        const rawSelection = this.gridController().selection();
        if (rawSelection.mode === "exclusive" && !rawSelection.excluded.length && !rawSelection.included.length) {
            // this is special case - user selects all values, without exclusions, suggest deleting all cmpXchg values
            // (including items which were not downloaded yet)
            this.confirmationMessage("Are you sure?", "Deleting <strong>ALL</strong> compare exchange items.", { html: true, buttons: ["Cancel", "Delete All"] })
                .done(result => {
                    if (result.can) {
                        this.spinners.delete(true);
                        
                        new getCompareExchangeItemsCommand(this.db, this.filter(), 0, 2147483647)
                            .execute()
                            .done(allValues => {
                                const deleteProgress = new deleteCompareExchangeProgress(allValues.items, this.db);

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

                        const deleteProgress = new deleteCompareExchangeProgress(selection, this.db);

                        deleteProgress.start()
                            .always(() => this.onDeleteCompleted());
                    }
                });
        }
    }

    private onDeleteCompleted(): void {
        this.spinners.delete(false);
        this.nextItemToFetchIndex = 0;
        this.gridController().reset(true);
    }
    
    private editItem(itemKey: string): void {
        router.navigate(appUrl.forEditCmpXchg(itemKey, this.db));
}
}

export = cmpXchg;

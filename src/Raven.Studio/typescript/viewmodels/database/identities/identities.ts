import router = require("plugins/router");
import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import eventsCollector = require("common/eventsCollector");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import getCompareExchangeItemsCommand = require("commands/database/cmpXchg/getCompareExchangeItemsCommand");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import checkedColumn = require("widgets/virtualGrid/columns/checkedColumn");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import continueTest = require("common/shell/continueTest");
import getIdentitiesCommand = require("commands/database/debug/getIdentitiesCommand");

class identities extends viewModelBase {

    filter = ko.observable<string>();
    private nextItemToFetchIndex = undefined as number;

    private gridController = ko.observable<virtualGridController<Raven.Server.Web.System.CompareExchangeHandler.CompareExchangeListItem>>();
    private columnPreview = new columnPreviewPlugin<Raven.Server.Web.System.CompareExchangeHandler.CompareExchangeListItem>();

    constructor() {
        super();

        this.initObservables();
        this.filter.throttle(500).subscribe(() => this.filterIdentities());
    }
    
    private filterIdentities() {
        this.nextItemToFetchIndex = 0;
        this.gridController().reset(true);
    }

    private initObservables() {
        // todo..
    }

    activate(args: any) {
        super.activate(args);

        continueTest.default.init(args); // ???
    }

    fetchIdentities(skip: number): JQueryPromise<pagedResult<Raven.Server.Web.System.CompareExchangeHandler.CompareExchangeListItem>> {
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

        grid.headerVisible(true); // todo maybe use this w/ false instead of the separate empty template ???

        
        const nameColumn = new hyperlinkColumn<Raven.Server.Web.System.CompareExchangeHandler.CompareExchangeListItem>(grid, x => x.Key, x => appUrl.forEditCmpXchg(x.Key, this.activeDatabase()), "Name", "30%");
        const valueColumn = new textColumn<Raven.Server.Web.System.CompareExchangeHandler.CompareExchangeListItem>(grid, x => x.Value.Object, "Value", "30%");
        const editColumn = new textColumn<Raven.Server.Web.System.CompareExchangeHandler.CompareExchangeListItem>(grid, x => x.Value["@metadata"], "Metadata", "20%");

        const gridColumns = [nameColumn, valueColumn, editColumn];
        grid.init((s, _) => this.fetchIdentities(s), () => gridColumns);
        
         this.columnPreview.install(".js-identites-grid", ".js-identities-tooltip", 
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

    newIdentity($event: JQueryEventObject) {
        eventsCollector.default.reportEvent("identity", "new");
        // const url = appUrl.forNewCmpXchg(this.activeDatabase());
        // if ($event.ctrlKey) {
        //     window.open(url);
        // } else {
        //     router.navigate(url);
        // }
    }
}

export = identities;

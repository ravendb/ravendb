import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import getCustomFunctionsCommand = require("commands/database/documents/getCustomFunctionsCommand");
import getZombiesCommand = require("commands/database/documents/getZombiesCommand");

import document = require("models/database/documents/document");

import eventsCollector = require("common/eventsCollector");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import evaluationContextHelper = require("common/helpers/evaluationContextHelper");

class zombies extends viewModelBase {

    dirtyResult = ko.observable<boolean>(false);
    dataChanged: KnockoutComputed<boolean>;

    private zombiesNextEtag = undefined as number;

    private customFunctionsContext: object;

    private gridController = ko.observable<virtualGridController<document>>();
    private columnPreview = new columnPreviewPlugin<document>();

    constructor() {
        super();

        this.initObservables();
    }

    private initObservables() {
        this.dataChanged = ko.pureComputed(() => {
            return this.dirtyResult();
        });
    }

    activate(args: any) {
        super.activate(args);
        //TODO: this.updateHelpLink("G8CDCP");

        return new getCustomFunctionsCommand(this.activeDatabase())
            .execute()
            .done(functions => {
                this.customFunctionsContext = evaluationContextHelper.createContext(functions.functions);
            });
    }

    refresh() {
        eventsCollector.default.reportEvent("zombies", "refresh");
        this.zombiesNextEtag = undefined;
        this.gridController().reset(true);
    }

    fetchZombies(skip: number): JQueryPromise<pagedResult<document>> {
        const task = $.Deferred<pagedResult<document>>();

        new getZombiesCommand(this.activeDatabase(), this.zombiesNextEtag, 101)
            .execute()
            .done(result => {
                //TODO: etag
                const hasMore = result.items.length === 101;
                const totalCount = skip + result.items.length;
                if (hasMore) {
                    const nextItem = result.items.pop();
                    this.zombiesNextEtag = nextItem.__metadata.etag();
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
        grid.withEvaluationContext(this.customFunctionsContext);

        grid.init((s, _) => this.fetchZombies(s), () => [
            new hyperlinkColumn<document>(grid, x => x.getId(), x => appUrl.forEditDoc(x.getId(), this.activeDatabase()), "Id", "300px"),
            new textColumn<document>(grid, x => x.__metadata.etag(), "ETag", "200px"),
            new textColumn<document>(grid, x => x.__metadata.lastModified(), "Deletion date", "300px")
        ]);

        grid.dirtyResults.subscribe(dirty => this.dirtyResult(dirty));

        this.columnPreview.install(".documents-grid", ".tooltip", (doc: document, column: virtualColumn, e: JQueryEventObject, onValue: (context: any) => void) => {
            if (column instanceof textColumn) {
                const value = column.getCellValue(doc);
                if (!_.isUndefined(value)) {
                    const json = JSON.stringify(value, null, 4);
                    const html = Prism.highlight(json, (Prism.languages as any).javascript);
                    onValue(html);
                }
            }
        });
    }


}

export = zombies;

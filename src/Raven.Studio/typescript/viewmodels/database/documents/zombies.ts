import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import deleteRevisionsForDocumentsCommand = require("commands/database/documents/deleteRevisionsForDocumentsCommand");
import getZombiesCommand = require("commands/database/documents/getZombiesCommand");

import document = require("models/database/documents/document");

import eventsCollector = require("common/eventsCollector");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import checkedColumn = require("widgets/virtualGrid/columns/checkedColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import evaluationContextHelper = require("common/helpers/evaluationContextHelper");

class zombies extends viewModelBase {

    dirtyResult = ko.observable<boolean>(false);
    dataChanged: KnockoutComputed<boolean>;
    selectedItemsCount: KnockoutComputed<number>;
    deleteEnabled: KnockoutComputed<boolean>;

    spinners = {
        delete: ko.observable<boolean>(false)
    }

    private zombiesNextEtag = undefined as number;

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
        this.deleteEnabled = ko.pureComputed(() => {
            const deleteInProgress = this.spinners.delete();
            const selectedDocsCount = this.selectedItemsCount();

            return !deleteInProgress && selectedDocsCount > 0;
        });
        this.selectedItemsCount = ko.pureComputed(() => {
            let selectedDocsCount = 0;
            const controll = this.gridController();
            if (controll) {
                selectedDocsCount = controll.selection().count;
            }
            return selectedDocsCount;
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

        grid.init((s, _) => this.fetchZombies(s), () => [
            new checkedColumn(false),
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

    deleteSelected() {
        const selectedIds = this.gridController().getSelectedItems().map(x => x.getId());

        this.confirmationMessage("Are you sure?", "Do you have to delete selected zombies and its revisions?", ["Cancel", "Yes, delete"])
            .done(result => {
                if (result.can) {

                    this.spinners.delete(true);

                    new deleteRevisionsForDocumentsCommand(selectedIds, this.activeDatabase())
                        .execute()
                        .always(() => {
                            this.spinners.delete(false);
                            this.gridController().reset(false);
                        });
                }
            });
    }


}

export = zombies;

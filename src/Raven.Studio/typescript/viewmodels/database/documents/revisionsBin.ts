import appUrl = require("common/appUrl");
import router = require("plugins/router");
import viewModelBase = require("viewmodels/viewModelBase");
import deleteRevisionsForDocumentsCommand = require("commands/database/documents/deleteRevisionsForDocumentsCommand");
import getRevisionsBinEntryCommand = require("commands/database/documents/getRevisionsBinEntryCommand");
import generalUtils = require("common/generalUtils");

import document = require("models/database/documents/document");

import eventsCollector = require("common/eventsCollector");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import checkedColumn = require("widgets/virtualGrid/columns/checkedColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");

class revisionsBin extends viewModelBase {

    dirtyResult = ko.observable<boolean>(false);
    dataChanged: KnockoutComputed<boolean>;
    selectedItemsCount: KnockoutComputed<number>;
    deleteEnabled: KnockoutComputed<boolean>;

    spinners = {
        delete: ko.observable<boolean>(false)
    };

    private revisionsBinEntryNextChangeVector = undefined as string;

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
        eventsCollector.default.reportEvent("revisions-bin", "refresh");
        this.revisionsBinEntryNextChangeVector = undefined;
        this.gridController().reset(true);
    }

    fetchRevisionsBinEntries(skip: number): JQueryPromise<pagedResult<document>> {
        const task = $.Deferred<pagedResult<document>>();

        new getRevisionsBinEntryCommand(this.activeDatabase(), this.revisionsBinEntryNextChangeVector, 101)
            .execute()
            .done(result => {
                const hasMore = result.items.length === 101;
                const totalCount = skip + result.items.length;
                if (hasMore) {
                    const nextItem = result.items.pop();
                    this.revisionsBinEntryNextChangeVector = nextItem.__metadata.changeVector();
                }

                task.resolve({
                    totalResultCount: totalCount,
                    items: result.items
                });
            })
            .fail((result: JQueryXHR) => {
                if (result.responseJSON) {
                    const errorType = result.responseJSON['Type'] || "";
                    
                    if (errorType.endsWith("RevisionsDisabledException")) {
                        router.navigate(appUrl.forDocuments(null,  this.activeDatabase()));
                    }
                }
            });

        return task;
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();

        grid.headerVisible(true);

        grid.init((s, _) => this.fetchRevisionsBinEntries(s), () => [
            new checkedColumn(false),
            new hyperlinkColumn<document>(grid, x => x.getId(), x => appUrl.forEditDoc(x.getId(), this.activeDatabase()), "Id", "300px"),
            new textColumn<document>(grid, x => x.__metadata.changeVector(), "Change Vector", "210px"), 
            new textColumn<document>(grid, x => generalUtils.formatUtcDateAsLocal(x.__metadata.lastModified()), "Deletion date", "300px")
        ]);

        grid.dirtyResults.subscribe(dirty => this.dirtyResult(dirty));

        this.columnPreview.install(".documents-grid", ".js-revisions-bin-tooltip", 
            (doc: document, column: virtualColumn, e: JQueryEventObject, onValue: (context: any, valueToCopy: string) => void) => {
            if (column instanceof textColumn) {
                
                if (column.header === "Deletion date") {
                    onValue(moment.utc(doc.__metadata.lastModified()), doc.__metadata.lastModified());
                } else {
                    const value = column.getCellValue(doc);
                    if (!_.isUndefined(value)) {
                        const json = JSON.stringify(value, null, 4);
                        const html = Prism.highlight(json, (Prism.languages as any).javascript);
                        onValue(html, json);
                    }
                }
            }
        });
    }

    deleteSelected() {
        const selectedIds = this.gridController().getSelectedItems().map(x => x.getId());

        eventsCollector.default.reportEvent("revisionsBin", "delete-selected");
        
        this.confirmationMessage("Are you sure?", "Do you want to delete selected items and its revisions?", {
            buttons: ["Cancel", "Yes, delete"]
        })
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

export = revisionsBin;

import appUrl = require("common/appUrl");
import router = require("plugins/router");
import deleteRevisionsForDocumentsCommand = require("commands/database/documents/deleteRevisionsForDocumentsCommand");
import getRevisionsBinEntryCommand = require("commands/database/documents/getRevisionsBinEntryCommand");
import generalUtils = require("common/generalUtils");
import moment = require("moment");
import document = require("models/database/documents/document");
import eventsCollector = require("common/eventsCollector");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import checkedColumn = require("widgets/virtualGrid/columns/checkedColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import { highlight, languages } from "prismjs";
import shardViewModelBase from "viewmodels/shardViewModelBase";
import database = require("models/resources/database");

class revisionsBin extends shardViewModelBase {

    view = require("views/database/documents/revisionsBin.html");
    
    dirtyResult = ko.observable<boolean>(false);
    dataChanged: KnockoutComputed<boolean>;
    selectedItemsCount: KnockoutComputed<number>;
    deleteEnabled: KnockoutComputed<boolean>;

    spinners = {
        delete: ko.observable<boolean>(false)
    };

    private gridController = ko.observable<virtualGridController<document>>();
    private columnPreview = new columnPreviewPlugin<document>();

    itemsSoFar = ko.observable<number>(0);
    continuationToken: string;

    constructor(db: database) {
        super(db);

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
        this.gridController().reset(true);
    }

    fetchRevisionsBinEntries(skip: number): JQueryPromise<pagedResultWithToken<document>> {
        const task = $.Deferred<pagedResultWithToken<document>>();

        new getRevisionsBinEntryCommand(this.db, skip, 101, this.continuationToken)
            .execute()
            .done(result => {
                let totalCount;
                this.continuationToken = result.continuationToken;

                if (result.continuationToken) {
                    this.itemsSoFar(this.itemsSoFar() + result.items.length);

                    if (this.itemsSoFar() === result.totalResultCount) {
                        totalCount = this.itemsSoFar()
                    } else {
                        totalCount = this.itemsSoFar() + 1;
                    }
                } else {
                    const hasMore = result.items.length === 101;
                    totalCount = skip + result.items.length;

                    if (hasMore) {
                        result.items.pop();
                    }
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
                        router.navigate(appUrl.forDocuments(null, this.db));
                    }
                }
            });

        return task;
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();

        grid.headerVisible(true);
        
        const checkColumn = new checkedColumn(false);
        const idColumn = new hyperlinkColumn<document>(grid, x => x.getId(), x => appUrl.forEditDoc(x.getId(), this.db), "Id", "300px");
        const changeVectorColumn = new textColumn<document>(grid, x => x.__metadata.changeVector(), "Change Vector", "210px");
        const deletionDateColumn = new textColumn<document>(grid, x => generalUtils.formatUtcDateAsLocal(x.__metadata.lastModified()), "Deletion date", "300px");

        const gridColumns = this.isAdminAccessOrAbove() ? [checkColumn, idColumn, changeVectorColumn, deletionDateColumn] : [idColumn, changeVectorColumn, deletionDateColumn];
        grid.init((s) => this.fetchRevisionsBinEntries(s), () => gridColumns);

        grid.dirtyResults.subscribe(dirty => this.dirtyResult(dirty));

        this.columnPreview.install(".documents-grid", ".js-revisions-bin-tooltip", 
            (doc: document, column: virtualColumn, e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy: string) => void) => {
            if (column instanceof textColumn) {
                
                if (column.header === "Deletion date") {
                    onValue(moment.utc(doc.__metadata.lastModified()), doc.__metadata.lastModified());
                } else {
                    const value = column.getCellValue(doc);
                    if (value !== undefined) {
                        const json = JSON.stringify(value, null, 4);
                        const html = highlight(json, languages.javascript, "js");
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

                    const parameters: Raven.Client.Documents.Operations.Revisions.DeleteRevisionsOperation.Parameters = {
                        DocumentIds: selectedIds,
                        RevisionsChangeVectors: [],
                        RemoveForceCreatedRevisions: true,
                    }

                    new deleteRevisionsForDocumentsCommand(this.db?.name, parameters)
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

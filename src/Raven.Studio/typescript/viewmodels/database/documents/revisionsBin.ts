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
import prismjs = require("prismjs");
import shardViewModelBase = require("viewmodels/shardViewModelBase");
import database = require("models/resources/database");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("revisionsBin");
const commonT = i18nModule.createTranslator("common");

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
    private deletionDateColumn: textColumn<document>;

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
        
        grid.init((s) => this.fetchRevisionsBinEntries(s), () => this.createColumns(grid));

        this.registerDisposable(i18nModule.currentLanguage.subscribe(() => {
            grid.markColumnsDirty();
            grid.reset(false);
        }));

        grid.dirtyResults.subscribe(dirty => this.dirtyResult(dirty));

        this.columnPreview.install(".documents-grid", ".js-revisions-bin-tooltip", 
            (doc: document, column: virtualColumn, e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy: string) => void) => {
            if (column instanceof textColumn) {
                
                if (column === this.deletionDateColumn) {
                    onValue(moment.utc(doc.__metadata.lastModified()), doc.__metadata.lastModified());
                } else {
                    const value = column.getCellValue(doc);
                    if (value !== undefined) {
                        const json = JSON.stringify(value, null, 4);
                        const html = prismjs.highlight(json, prismjs.languages.javascript, "js");
                        onValue(html, json);
                    }
                }
            }
        });
    }

    private createColumns(grid: virtualGridController<document>): virtualColumn[] {
        const checkColumn = new checkedColumn(false);
        const idColumn = new hyperlinkColumn<document>(grid, x => x.getId(), x => appUrl.forEditDoc(x.getId(), this.db), t("columns.id"), "300px");
        const changeVectorColumn = new textColumn<document>(grid, x => x.__metadata.changeVector(), t("columns.changeVector"), "210px");
        this.deletionDateColumn = new textColumn<document>(grid, x => generalUtils.formatUtcDateAsLocal(x.__metadata.lastModified()), t("columns.deletionDate"), "300px");

        const dataColumns = [idColumn, changeVectorColumn, this.deletionDateColumn];
        return this.isAdminAccessOrAbove() ? [checkColumn, ...dataColumns] : dataColumns;
    }

    deleteSelected() {
        const selectedIds = this.gridController().getSelectedItems().map(x => x.getId());

        eventsCollector.default.reportEvent("revisionsBin", "delete-selected");
        
        this.confirmationMessage(t("deleteConfirm.title"),
            t("deleteConfirm.message"),
            {
                buttons: [commonT("cancel"), t("deleteConfirm.confirmButton")],
                html: true
            })
            .done(result => {
                if (result.can) {
                    this.spinners.delete(true);

                    const parameters: Raven.Client.Documents.Operations.Revisions.DeleteRevisionsOperation.Parameters = {
                        DocumentIds: selectedIds,
                        RevisionsChangeVectors: [],
                        RemoveForceCreatedRevisions: true,
                    };

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

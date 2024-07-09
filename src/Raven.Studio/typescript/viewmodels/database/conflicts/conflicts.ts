import appUrl = require("common/appUrl");
import eventsCollector = require("common/eventsCollector");
import app = require("durandal/app");
import getConflictsCommand = require("commands/database/replication/getConflictsCommand");
import getConflictsForDocumentCommand = require("commands/database/replication/getConflictsForDocumentCommand");
import getSuggestedConflictResolutionCommand = require("commands/database/replication/getSuggestedConflictResolutionCommand");
import deleteDocuments = require("viewmodels/common/deleteDocuments");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import messagePublisher = require("common/messagePublisher");
import document = require("models/database/documents/document");
import saveDocumentCommand = require("commands/database/documents/saveDocumentCommand");
import changeVectorUtils = require("common/changeVectorUtils");
import generalUtils = require("common/generalUtils");
import copyToClipboard = require("common/copyToClipboard");
import moment = require("moment");
import { highlight, languages } from "prismjs";
import shardViewModelBase from "viewmodels/shardViewModelBase";
import database = require("models/resources/database");

class conflictItem {
    
    private static readonly dateFormat = "DD/MM/YYYY HH:mm:ss";

    originalValue = ko.observable<string>();
    lastModified = ko.observable<string>();
    formattedValue = ko.observable<string>();
    deletedMarker = ko.observable<boolean>();
    changeVector = ko.observable<changeVectorItem[]>();
    computedDocumentSize: KnockoutComputed<string>;

    constructor(dto: Raven.Client.Documents.Commands.GetConflictsResult.Conflict, useLongChangeVectorFormat: boolean) {
        if (dto.Doc) {
            const json = JSON.stringify(dto.Doc, null, 4);
            this.originalValue(json);
            this.formattedValue(highlight(json, languages.javascript, "js"));
            this.deletedMarker(false);
            this.changeVector(changeVectorUtils.formatChangeVector(dto.ChangeVector, useLongChangeVectorFormat));
        } else {
            this.deletedMarker(true);
        }

        this.lastModified(moment.utc(dto.LastModified).local().format(conflictItem.dateFormat));

        this.computedDocumentSize = ko.pureComputed(() => {
            try {
                const textSize: number = generalUtils.getSizeInBytesAsUTF8(this.originalValue());
                return generalUtils.formatBytesToSize(textSize);
            } catch (e) {
                return "cannot compute";
            }
        });
    }
}

class conflicts extends shardViewModelBase {

    view = require("views/database/conflicts/conflicts.html");

    hasDetailsLoaded = ko.observable<boolean>(false);
    detailsVisible = ko.observable<boolean>(false);

    private isSaving = ko.observable<boolean>(false);

    private gridController = ko.observable<virtualGridController<replicationConflictListItemDto>>();
    private columnPreview = new columnPreviewPlugin<replicationConflictListItemDto>();

    currentConflict = ko.observable<Raven.Client.Documents.Commands.GetConflictsResult>();
    conflictItems = ko.observableArray<conflictItem>([]);
    suggestedResolution = ko.observable<string>();
    documentId = ko.observable<string>();

    validationGroup = ko.validatedObservable({
        suggestedResolution: this.suggestedResolution
    });

    itemsSoFar = ko.observable<number>(0);
    nextStart = 0;
    continuationToken: string;

    constructor(db: database) {
        super(db);
        
        this.bindToCurrentInstance("useThis", "copyThis");

        aceEditorBindingHandler.install();
        this.initValidation();
    }

    private initValidation() {
        const conflictTokens = ['">>>> conflict start"', '"<<<< conflict end"', '">>>> auto merged array start"', '"<<<< auto merged array end"'];

        this.suggestedResolution.extend({
            required: true,
            aceValidation: true,
            validation: [{
                validator: (val: string) => conflictTokens.every(t => !val.includes(t)),
                message: "Document contains conflicts markers"
            }]
        });
    }

    activate(navigationArgs: { id: string }) {
        super.activate(navigationArgs);

        this.updateHelpLink('5FRRCA');

        if (navigationArgs && navigationArgs.id) {
            this.loadConflictForDocument(navigationArgs.id);
        }
    }

    compositionComplete(): void {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);
        grid.init((s, t) => this.fetchConflicts(t), () =>
            [
                new hyperlinkColumn<replicationConflictListItemDto>(grid, x => x.Id, x => appUrl.forConflicts(this.db, x.Id), "Document", "40%",
                    {
                        handler: (item, event) => this.handleLoadAction(item, event)
                    }),
                new textColumn<replicationConflictListItemDto>(grid, x => x.ConflictsPerDocument, "#", "10%", {
                    headerTitle: "Conflicts per document"
                }),
                new textColumn<replicationConflictListItemDto>(grid, x => x.LastModified, "Last Modified", "45%")
            ]
        );

        this.columnPreview.install(".conflicts-grid", ".js-conflict-details-tooltip",
            (details: replicationConflictListItemDto, column: virtualColumn, e: JQuery.TriggeredEvent,
             onValue: (context: any, valueToCopy?: string) => void) => {
                if (column instanceof textColumn) {
                    if (column.header === "Last Modified") {
                        onValue(moment.utc(details.LastModified), details.LastModified);
                    } else {
                        const value = column.getCellValue(details);
                        if (value) {
                            onValue(generalUtils.escapeHtml(value), value);
                        }
                    }
                }
            });

        // if we have some document at this point select this value
        // it is used during per url initialization
        if (this.documentId()) {
            this.selectCurrentItem(this.documentId());
        }

        this.columnPreview.install(".conflicts-grid", ".js-conflicts-grid-tooltip",
            (item: replicationConflictListItemDto, column: virtualColumn, e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy: string) => void) => {
                if (column instanceof textColumn) {
                    if (column.header === "Date") {
                        onValue(moment.utc(item.LastModified), item.LastModified);
                    } else {
                        const value = column.getCellValue(item);
                        if (!_.isUndefined(value)) {
                            const json = JSON.stringify(value, null, 4);
                            const html = highlight(json, languages.javascript, "js");
                            onValue(html, json);
                        }
                    }
                }
            });
    }

    private fetchConflicts(take: number): JQueryPromise<pagedResultWithToken<replicationConflictListItemDto>> {
        return new getConflictsCommand(this.db, this.nextStart, take, this.continuationToken)
            .execute()
            .done((results) => {
                this.continuationToken = results.continuationToken;

                if (results.continuationToken) {
                    this.itemsSoFar(this.itemsSoFar() + results.items.length);

                    if (this.itemsSoFar() === results.totalResultCount) {
                        results.totalResultCount = this.itemsSoFar();
                    } else {
                        results.totalResultCount = this.itemsSoFar() + 1;
                    }
                } else {
                    this.nextStart += results.scannedResults + 1;
                }

                this.syncSelection(results.items);
            })
    }

    // id requested in url might not be available in first chunk
    // watch for conflicts until we find item to highlight
    private syncSelection(items: Array<replicationConflictListItemDto>) {
        const alreadyHasSelection = this.gridController().getSelectedItems().length;
        if (!alreadyHasSelection && this.documentId() && items.find(x => x.Id === this.documentId())) {
            setTimeout(() => {
                const itemToSelect = this.gridController().findItem(x => x.Id === this.documentId());
                this.gridController().setSelectedItems([itemToSelect]);
            }, 0);
        }
    }

    private handleLoadAction(conflictToLoad: replicationConflictListItemDto, event: JQuery.TriggeredEvent) {
        event.preventDefault();

        const documentId = conflictToLoad.Id;
        this.updateUrl(appUrl.forConflicts(this.db, documentId));

        this.loadConflictForDocument(documentId);
    }

    private loadConflictForDocument(documentId: string) {
        this.suggestedResolution(null);

        $.when<any>(this.loadConflictItemsForDocument(documentId), this.loadSuggestedConflictResolution(documentId))
            .then(([conflictsDto]: [Raven.Client.Documents.Commands.GetConflictsResult], [mergeResult]: [Raven.Server.Utils.ConflictResolverAdvisor.MergeResult]) => {
                this.currentConflict(conflictsDto);

                const useLongFormat = changeVectorUtils.shouldUseLongFormat(conflictsDto.Results.map(x => x.ChangeVector));
                
                this.conflictItems(conflictsDto.Results.map(x => new conflictItem(x, useLongFormat)));

                (mergeResult.Document as any)["@metadata"] = mergeResult.Metadata;
                const serializedResolution = JSON.stringify(mergeResult.Document, null, 4);
                this.suggestedResolution(serializedResolution);
                this.documentId(documentId);

                this.selectCurrentItem(documentId);

                this.hasDetailsLoaded(true);
            });
    }

    private selectCurrentItem(documentId: string) {
        if (this.gridController()) {
            const item = this.gridController().findItem(x => x.Id === documentId);
            this.gridController().setSelectedItems([item]);
        }
    }

    private loadConflictItemsForDocument(documentId: string) {
        return new getConflictsForDocumentCommand(this.db, documentId)
            .execute()
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404) {
                    messagePublisher.reportError("Unable to find conflicted document: " + documentId + ". Maybe conflict was already resolved?");
                } else {
                    messagePublisher.reportError("Failed to load conflict!", xhr.responseText, xhr.statusText);
                }
            });
    }

    private loadSuggestedConflictResolution(documentId: string) {
        return new getSuggestedConflictResolutionCommand(this.db, documentId)
            .execute();
    }

    deleteDocument() {
        eventsCollector.default.reportEvent("conflicts", "delete");
        const viewModel = new deleteDocuments([this.documentId()], this.db);
        app.showBootstrapDialog(viewModel);
        viewModel.deletionTask.done(() => this.onResolved());
    }

    saveDocument() {
        if (this.isValid(this.validationGroup)) {
            
            eventsCollector.default.reportEvent("conflicts", "save-resolution");

            // don't catch here, as we assume input is valid (checked that few lines above)
            const updatedDto = JSON.parse(this.suggestedResolution());

            if (!updatedDto['@metadata']) {
                updatedDto["@metadata"] = {};
            }

            const meta = updatedDto['@metadata'];

            // force document id to support save as new
            meta['@id'] = this.documentId();
            delete meta['@change-vector'];

            const newDoc = new document(updatedDto);
            const saveCommand = new saveDocumentCommand(this.documentId(), newDoc, this.db);
            this.isSaving(true);
            saveCommand
                .execute()
                .done(() => this.onResolved())
                .fail(() => {
                    this.isSaving(false);
                });
        }
    }

    private onResolved() {
        this.nextStart = 0;
        this.gridController().reset(false);
        
        this.suggestedResolution("");
        this.conflictItems([]);
        this.documentId(null);
        this.currentConflict(null);
        this.hasDetailsLoaded(false);
    }
    
    useThis(itemToUse: conflictItem) {
        this.suggestedResolution(itemToUse.originalValue());
    }

    copyThis(itemToCopy: conflictItem) {
        copyToClipboard.copy(itemToCopy.originalValue(), "Document has been copied to clipboard");
}
}

export = conflicts;

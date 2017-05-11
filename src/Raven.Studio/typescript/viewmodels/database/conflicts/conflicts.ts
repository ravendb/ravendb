import appUrl = require("common/appUrl");
import eventsCollector = require("common/eventsCollector");

import viewModelBase = require("viewmodels/viewModelBase");
import app = require("durandal/app");

import getConflictsCommand = require("commands/database/replication/getConflictsCommand");
import getConflictsForDocumentCommand = require("commands/database/replication/getConflictsForDocumentCommand");
import getSuggestedConflictResolutionCommand = require("commands/database/replication/getSuggestedConflictResolutionCommand");

import deleteDocuments = require("viewmodels/common/deleteDocuments");

import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import messagePublisher = require("common/messagePublisher");

import document = require("models/database/documents/document");
import saveDocumentCommand = require("commands/database/documents/saveDocumentCommand");

class conflictItem {

    formattedValue = ko.observable<string>();
    deletedMarker = ko.observable<boolean>();

    constructor(dto: Raven.Client.Documents.Commands.GetConflictsResult.Conflict) {
        //TODO: use change vector? probably yes - latest db id from change vector allows us to get information on which node the modification was made. 
        if (dto.Doc) {
            const json = JSON.stringify(dto.Doc, null, 4);
            
            this.formattedValue(Prism.highlight(json, (Prism.languages as any).javascript));
            this.deletedMarker(false);
        } else {
            this.deletedMarker(true);
        }
    }
}

class conflicts extends viewModelBase {

    //TODO: map db is from change vector to some meaningful names? 
    //TODO: handle conflicts on deletion
    //TODO: detect change in grid
    //TODO: subscribe to changes api for conflicts (with throttle)
    //TODO: spiners - block ace editor when saving/deleting?

    hasDetailsLoaded = ko.observable<boolean>(false);

    private isSaving = ko.observable<boolean>(false);

    private gridController = ko.observable<virtualGridController<replicationConflictListItemDto>>();

    currentConflict = ko.observable<Raven.Client.Documents.Commands.GetConflictsResult>();
    conflictItems = ko.observableArray<conflictItem>([]);
    suggestedResolution = ko.observable<string>();
    documentId = ko.observable<string>();

    validationGroup = ko.validatedObservable({
        suggestedResolution: this.suggestedResolution
    });

    constructor() {
        super();

        aceEditorBindingHandler.install();
        this.initValidation();
    }

    private initValidation() {
        const conflictTokens = ['">>>> conflict start"', '"<<<< conflict end"', '">>>> auto merged array start"', '"<<<< auto merged array end"'];

        this.suggestedResolution.extend({
            required: true,
            validJson: true,
            validation: [{
                validator: (val: string) => _.every(conflictTokens, t => !val.includes(t)),
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
        grid.init((s, t) => this.fetchConflicts(s, t), () =>
            [
                new hyperlinkColumn<replicationConflictListItemDto>(x => x.Key, x => appUrl.forConflicts(this.activeDatabase(), x.Key), "Document", "50%",
                    {
                        handler: (item, event) => this.handleLoadAction(item, event)
                    }),
                new textColumn<replicationConflictListItemDto>(x => x.LastModified, "Date", "50%")
            ]
        );

        // if we have some document at this point select this value
        // it is used during per url initialization
        if (this.documentId()) {
            this.selectCurrentItem(this.documentId());
        }
    }

    private fetchConflicts(start: number, pageSize: number) {
        return new getConflictsCommand(this.activeDatabase(), start, pageSize)
            .execute()
            .done((result) => {
                this.syncSelection(result.items);
            });
    }

    // id requsted in url might not be available in first chunk
    // watch for conflicts until we find item to highlight
    private syncSelection(items: Array<replicationConflictListItemDto>) {
        const alreadyHasSelection = this.gridController().getSelectedItems().length;
        if (!alreadyHasSelection && this.documentId() && items.find(x => x.Key === this.documentId())) {
            setTimeout(() => {
                const itemToSelect = this.gridController().findItem(x => x.Key === this.documentId());
                this.gridController().setSelectedItems([itemToSelect]);
            }, 0);
        }
    }

    private handleLoadAction(conflictToLoad: replicationConflictListItemDto, event: JQueryEventObject) {
        event.preventDefault();

        const documentId = conflictToLoad.Key;
        this.updateUrl(appUrl.forConflicts(this.activeDatabase(), documentId));

        this.loadConflictForDocument(documentId);
    }

    private loadConflictForDocument(documentId: string) {
        this.suggestedResolution(null);

        $.when<any>(this.loadConflictItemsForDocument(documentId), this.loadSuggestedConflictResolution(documentId))
            .then(([conflictsDto]: [Raven.Client.Documents.Commands.GetConflictsResult], [mergeResult]: [Raven.Server.Utils.ConflictResolverAdvisor.MergeResult]) => {
                this.currentConflict(conflictsDto);
                this.conflictItems(conflictsDto.Results.map(x => new conflictItem((x))));

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
            const item = this.gridController().findItem(x => x.Key === documentId);
            this.gridController().setSelectedItems([item]);
        }
        
    }

    private loadConflictItemsForDocument(documentId: string) {
        return new getConflictsForDocumentCommand(this.activeDatabase(), documentId)
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
        return new getSuggestedConflictResolutionCommand(this.activeDatabase(), documentId)
            .execute();
    }

    deleteDocument() {
        eventsCollector.default.reportEvent("document", "delete");
        const viewModel = new deleteDocuments([this.documentId()], this.activeDatabase());
        app.showBootstrapDialog(viewModel);
        viewModel.deletionTask.done(() => this.onResolved());
    }

    saveDocument() {
        if (this.isValid(this.validationGroup)) {

            // don't catch here, as we assume input is valid (checked that few lines above)
            const updatedDto = JSON.parse(this.suggestedResolution());

            if (!updatedDto['@metadata']) {
                updatedDto["@metadata"] = {};
            }

            const meta = updatedDto['@metadata'];

            // force document id to support save as new
            meta['@id'] = this.documentId();
            meta['@etag'] = 0;

            const newDoc = new document(updatedDto);
            const saveCommand = new saveDocumentCommand(this.documentId(), newDoc, this.activeDatabase());
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
        this.gridController().reset(false);
        
        this.suggestedResolution("");
        this.conflictItems([]);
        this.documentId(null);
        this.currentConflict(null);
        this.hasDetailsLoaded(false);
    }
}

export = conflicts;

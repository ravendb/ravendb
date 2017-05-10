import router = require("plugins/router");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import getReplicationSourcesCommand = require("commands/database/replication/getReplicationSourcesCommand");
import getIndexDefinitionCommand = require("commands/database/index/getIndexDefinitionCommand");
import getSingleTransformerCommand = require("commands/database/transformers/getSingleTransformerCommand");
import conflictsResolveCommand = require("commands/database/replication/conflictsResolveCommand");
import getEffectiveConflictResolutionCommand = require("commands/database/globalConfig/getEffectiveConflictResolutionCommand");
import eventsCollector = require("common/eventsCollector");

import viewModelBase = require("viewmodels/viewModelBase");

import getConflictsCommand = require("commands/database/replication/getConflictsCommand");
import getConflictsForDocumentCommand = require("commands/database/replication/getConflictsForDocumentCommand");
import getSuggestedConflictResolutionCommand = require("commands/database/replication/getSuggestedConflictResolutionCommand");

import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import checkedColumn = require("widgets/virtualGrid/columns/checkedColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import messagePublisher = require("common/messagePublisher");

import document = require("models/database/documents/document");
import saveDocumentCommand = require("commands/database/documents/saveDocumentCommand");

class conflictItem {

    formattedValue = ko.observable<string>();
    deletedMarker = ko.observable<boolean>();

    constructor(dto: Raven.Client.Documents.Commands.GetConflictsResult.Conflict) {
        //TODO: use change vector?
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
    //TODO: resolve selected to newest etc.

    private isSaving = ko.observable<boolean>(false);

    private gridController = ko.observable<virtualGridController<replicationConflictListItemDto>>();

    currentConflict = ko.observable<Raven.Client.Documents.Commands.GetConflictsResult>();
    conflictItems = ko.observableArray<conflictItem>([]);
    suggestedResolution = ko.observable<string>(); //TODO: add validation  + block when loading / saving conflict resolution 
    documentId = ko.observable<string>();

    constructor() {
        super();

        aceEditorBindingHandler.install();
    }

    activate(navigationArgs: { id: string }) {
        super.activate(navigationArgs);

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
                new checkedColumn(true),
                //TODO: format date (as ago ? )
                new hyperlinkColumn<replicationConflictListItemDto>(x => x.Key, x => appUrl.forConflicts(this.activeDatabase(), x.Key), "Document", "50%",
                    {
                        handler: (item, event) => this.handleLoadAction(item, event)
                    }),
                new textColumn<replicationConflictListItemDto>(x => x.LastModified, "Date", "50%")
            ]
        );
    }

    private fetchConflicts(start: number, pageSize: number) {
        return new getConflictsCommand(this.activeDatabase(), start, pageSize)
            .execute();
    }

    private handleLoadAction(conflictToLoad: replicationConflictListItemDto, event: JQueryEventObject) {
        event.preventDefault();

        const documentId = conflictToLoad.Key;
        this.updateUrl(appUrl.forConflicts(this.activeDatabase(), documentId));

        this.loadConflictForDocument(documentId);
    }

    private loadConflictForDocument(documentId: string) {
        this.suggestedResolution(null);

        return new getConflictsForDocumentCommand(this.activeDatabase(), documentId)
            .execute()
            .done(result => {
                this.currentConflict(result);
                this.conflictItems(result.Results.map(x => new conflictItem((x))));
                this.loadSuggestedConflictResolution(documentId);
            })
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
            .execute()
            .done(result => {
                (result.Document as any)["@metadata"] = result.Metadata;
                const serializedResolution = JSON.stringify(result.Document, null, 4);
                this.suggestedResolution(serializedResolution);
                this.documentId(documentId);
            });
    }

    saveDocument() {
        //TODO: validation 

        let message = "";
        let updatedDto: any;

        try {
            updatedDto = JSON.parse(this.suggestedResolution());
        } catch (e) {
            if (updatedDto == undefined) {
                message = "The document data isn't a legal JSON expression!";
            }
        }

        if (message) {
            messagePublisher.reportError(message, undefined, undefined, false);
            return;
        }

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
            .done((saveResult: saveDocumentResponseDto) => this.onDocumentSaved(saveResult, updatedDto))
            .fail(() => {
                this.isSaving(false);
            });
    }

    private onDocumentSaved(saveResult: saveDocumentResponseDto, localDoc: any) {
        //TODO:
    }

    /* TODO
    sourcesLookup: dictionary<string> = {};

    private refreshConflictsObservable = ko.observable<number>();
    private conflictsSubscription: KnockoutSubscription;
    hasAnyConflict: KnockoutComputed<boolean>;

    static performedIndexChecks: Array<string> = [];

    serverConflictResolution = ko.observable<string>();

    afterClientApiConnected(): void {
        const changesApi = this.changesContext.databaseChangesApi();
        //TODO: this.addNotification changesApi.watchAllReplicationConflicts((e) => this.refreshConflictsObservable(new Date().getTime())) 
    }

    attached() {
        super.attached();
        this.conflictsSubscription = this.refreshConflictsObservable.throttle(3000).subscribe((e) => this.fetchConflicts(this.activeDatabase()));
    }

    detached() {
        super.detached();

        if (this.conflictsSubscription != null) {
            this.conflictsSubscription.dispose();
        }
    }

    activate(args: any) {
        super.activate(args);
        this.activeDatabase.subscribe((db: database) => this.databaseChanged(db));

        this.updateHelpLink('5FRRCA');

        this.hasAnyConflict = ko.computed(() => {
            var pagedItems = this.currentConflictsPagedItems();
            if (pagedItems) {
                return pagedItems.totalResultCount() > 0;
            }
            return false;
        });

        return this.performIndexCheck(this.activeDatabase()).then(() => {
            return this.loadReplicationSources(this.activeDatabase());
        }).done(() => {
            this.fetchAutomaticConflictResolution(this.activeDatabase());
            this.fetchConflicts(this.activeDatabase());
        });
    }

    fetchAutomaticConflictResolution(db: database): JQueryPromise<any> {
        return new getEffectiveConflictResolutionCommand(db)
            .execute()
            .done((repConfig: configurationDocumentDto<replicationConfigDto>) => {
                this.serverConflictResolution(repConfig.MergedDocument.DocumentConflictResolution);
            });
    }

    fetchConflicts(database: database) {
        //TODO: this.currentConflictsPagedItems(this.createPagedList(database));
    }

    loadReplicationSources(db: database): JQueryPromise<dictionary<string>> {
        return new getReplicationSourcesCommand(db)
            .execute()
            .done(results => this.replicationSourcesLoaded(results, db));
    }

    replicationSourcesLoaded(sources: dictionary<string> , db: database) {
        this.sourcesLookup = sources;
    }

    databaseChanged(db: database) {
        var conflictsUrl = appUrl.forConflicts(db);
        router.navigate(conflictsUrl, false);
        this.performIndexCheck(db).then(() => {
            return this.loadReplicationSources(db);
        }).done(() => {
                this.fetchConflicts(db);
        });
    }

    /* TODO
    private createPagedList(database: database): pagedList {
        var fetcher = (skip: number, take: number) => new getConflictsCommand(database, skip, take).execute();
        return new pagedList(fetcher);
    }*

    getUrlForConflict(conflictVersion: conflictVersion) {
        return appUrl.forEditDoc(conflictVersion.id, this.activeDatabase());
    }

    getTextForVersion(conflictVersion: conflictVersion) {
        var replicationSource = this.sourcesLookup[conflictVersion.sourceId];
        var text = "";
        if (replicationSource) {
            text = " (" + replicationSource + ")";
        }
        return text;
    }

    getServerUrlForVersion(conflictVersion: conflictVersion) {
        return this.sourcesLookup[conflictVersion.sourceId] || "";
    }

    resolveToLocal() {
        eventsCollector.default.reportEvent("conflicts", "resolve-to-local");
        this.confirmationMessage("Sure?", "You're resolving all conflicts to local.", ["No", "Yes"])
            .done(() => {
                this.performResolve("ResolveToLocal");
            });
    }

    resolveToNewestRemote() {
        eventsCollector.default.reportEvent("conflicts", "resolve-to-newest-remote");
        this.confirmationMessage("Sure?", "You're resolving all conflicts to newest remote.", ["No", "Yes"])
            .done(() => {
            this.performResolve("ResolveToRemote");
        });
    }

    resolveToLatest() {
        eventsCollector.default.reportEvent("conflicts", "resolve-to-latest");
        this.confirmationMessage("Sure?", "You're resolving all conflicts to latest.", ["No", "Yes"])
            .done(() => {
            this.performResolve("ResolveToLatest");
        });
    }
    
    private performResolve(resolution: string) {
        new conflictsResolveCommand(this.activeDatabase(), resolution)
            .execute()
            .done(() => {
                this.fetchConflicts(this.activeDatabase());
            });
    }*/

}

export = conflicts;

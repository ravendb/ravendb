import router = require("plugins/router");

import document = require("models/database/documents/document");
import database = require("models/resources/database");
import collection = require("models/database/documents/collection");
import collectionsStats = require("models/database/documents/collectionsStats");
import recentDocumentsCtr = require("models/database/documents/recentDocuments");

import verifyDocumentsIDsCommand = require("commands/database/documents/verifyDocumentsIDsCommand");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");

import appUrl = require("common/appUrl");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import documentHelpers = require("common/helpers/database/documentHelpers");
import starredDocumentsStorage = require("common/storage/starredDocumentsStorage");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");

class connectedDocuments {

    loadDocumentAction: (docId: string) => void;
    document: KnockoutObservable<document>;
    db: KnockoutObservable<database>;
    searchInput = ko.observable<string>("");
    columns: virtualColumn[] = [
        new hyperlinkColumn<connectedDocument>(x => x.id, x => x.href, "", "100%")
    ];
    currentDocumentIsStarred = ko.observable<boolean>(false);
    currentTab = ko.observable<string>(connectedDocuments.connectedDocsTabs.related);
    recentDocuments = new recentDocumentsCtr();
    isRelatedActive = ko.pureComputed(() => this.currentTab() === connectedDocuments.connectedDocsTabs.related);
    isCollectionActive = ko.pureComputed(() => this.currentTab() === connectedDocuments.connectedDocsTabs.collection);
    isRecentActive = ko.pureComputed(() => this.currentTab() === connectedDocuments.connectedDocsTabs.recent);
    isStarredActive = ko.pureComputed(() => this.currentTab() === connectedDocuments.connectedDocsTabs.starred);

    gridController = ko.observable<virtualGridController<connectedDocument>>();

    static connectedDocsTabs = {
        related: "related",
        collection: "collection",
        recent: "recent",
        starred: "starred"
    };

    constructor(document: KnockoutObservable<document>, db: KnockoutObservable<database>, loadDocument: (docId: string) => void) {
        _.bindAll(this, "toggleStar" as keyof this);

        this.document = document;
        this.db = db;
        this.document.subscribe((doc) => this.onDocumentLoaded(doc));
        this.loadDocumentAction = loadDocument;
    }

    compositionComplete() {
        const grid = this.gridController();
        grid.headerVisible(false);
        grid.init((s, t) => this.fetchCurrentTabDocs(s, t), () => this.columns);

        this.currentTab.subscribe(() => this.gridController().reset());
    }

    fetchCurrentTabDocs(skip: number, take: number): JQueryPromise<pagedResult<connectedDocument>> {
        switch (this.currentTab()) {
            case connectedDocuments.connectedDocsTabs.related:
                return this.fetchRelatedDocs(skip, take);
            case connectedDocuments.connectedDocsTabs.collection:
                return this.fetchCollectionDocs(skip, take);
            case connectedDocuments.connectedDocsTabs.recent:
                return this.fetchRecentDocs(skip, take);
            case connectedDocuments.connectedDocsTabs.starred:
                return this.fetchStarredDocs(skip, take);
            default: return this.emptyDocResult();
        }
    }

    fetchRelatedDocs(skip: number, take: number): JQueryPromise<pagedResult<connectedDocument>> {
        const deferred = $.Deferred<pagedResult<connectedDocument>>();

        const relatedDocumentsCandidates: string[] = documentHelpers.findRelatedDocumentsCandidates(this.document());
        const docIDsVerifyCommand = new verifyDocumentsIDsCommand(relatedDocumentsCandidates, this.db());
        docIDsVerifyCommand.execute()
            .done((verifiedIDs: string[]) => {
                const connectedDocs: connectedDocument[] = verifiedIDs.map(id => this.docIdToConnectedDoc(id));
                deferred.resolve({
                    items: connectedDocs,
                    totalResultCount: connectedDocs.length
                });
            });

        return deferred.promise();
    }

    fetchCollectionDocs(skip: number, take: number): JQueryPromise<pagedResult<connectedDocument>> {
        // Make sure we've got a collection to work with.
        const doc = this.document();
        if (!doc) {
            return this.emptyDocResult();
        }

        // Fetch collection size.
        // Why? Because calling collection.fetchDocuments returns a pagedResult with .totalResultCount = 0. :-(
        const collectionName = doc.getCollection();
        const collectionSizeTask = new getCollectionsStatsCommand(this.db())
            .execute()
            .then((stats: collectionsStats) => stats.getCollectionCount(collectionName));

        // Fetch the chunk of documents.        
        const newCollection = new collection(collectionName, this.db());
        const fetchDocsTask = newCollection.fetchDocuments(skip, take)
            .then((result: pagedResult<any>) => {
                // Convert the items from document to connectedDocument.
                result.items = result.items.map(doc => this.documentToConnectedDoc(doc));
                return result;
            });

        return $.when<any>(collectionSizeTask, fetchDocsTask)
            .then((collectionSize: number, docsResult: pagedResult<connectedDocument>) => {
                docsResult.totalResultCount = collectionSize;
                return docsResult;
            });
    }

    fetchRecentDocs(skip: number, take: number): JQueryPromise<pagedResult<connectedDocument>> {
        const doc = this.document();
        if (!doc) {
            return this.emptyDocResult();
        }

        const recentDocs = this.recentDocuments.getTopRecentDocuments(this.db(), doc.getId());
        return $.Deferred<pagedResult<connectedDocument>>().resolve({
            items: recentDocs,
            totalResultCount: recentDocs.length
        }).promise();
    }

    fetchStarredDocs(skip: number, take: number): JQueryPromise<pagedResult<connectedDocument>> {
        const starredDocsTask = $.Deferred<pagedResult<connectedDocument>>();

        starredDocumentsStorage.getStarredDocumentsWithDocumentIdsCheck(this.db())
            .done((verifiedIds: string[]) => {
                const starredDocs = verifiedIds.map(id => this.docIdToConnectedDoc(id));
                starredDocsTask.resolve({
                    items: starredDocs,
                    totalResultCount: starredDocs.length
                });
            });

        return starredDocsTask.promise();
    }

    emptyDocResult(): JQueryPromise<pagedResult<connectedDocument>> {
        return $.Deferred<pagedResult<connectedDocument>>().resolve({
            items: [],
            totalResultCount: 0
        }).promise();
    }

    activateRelated() {
        this.currentTab(connectedDocuments.connectedDocsTabs.related);
    }

    activateCollection() {
        this.currentTab(connectedDocuments.connectedDocsTabs.collection);
    }

    activateRecent() {
        this.currentTab(connectedDocuments.connectedDocsTabs.recent);
    }

    activateStarred() {
        this.currentTab(connectedDocuments.connectedDocsTabs.starred);
    }

    onDocumentDeleted() {
        this.recentDocuments.documentRemoved(this.db(), this.document().getId());
        var previous = this.recentDocuments.getPreviousDocument(this.db());
        if (previous) {
            this.loadDocumentAction(previous);
            router.navigate(appUrl.forEditDoc(previous, this.db()), false);
        } else {
            router.navigate(appUrl.forDocuments(null, this.db()));
        }
    }

    toggleStar() {
        this.currentDocumentIsStarred(!this.currentDocumentIsStarred());
        starredDocumentsStorage.markDocument(this.db(), this.document().getId(), this.currentDocumentIsStarred());

        // If we're showing the starred docs, update the list.
        if (this.isStarredActive()) {
            const gridItemsResetState = this.currentTab; // The view has bound the grid reset items state to .currentTab observable. 
            gridItemsResetState.valueHasMutated(); // Tell the grid the items list has changed.
        }
    }

    private onDocumentLoaded(document: document) {
        if (document) {
            this.recentDocuments.appendRecentDocument(this.db(), this.document().getId());
            this.currentDocumentIsStarred(starredDocumentsStorage.isStarred(this.db(), this.document().getId()));
        }
    }

    private documentToConnectedDoc(doc: document): connectedDocument {
        return {
            id: doc.getId(),
            href: appUrl.forEditDoc(doc.getId(), this.db())
        };
    }

    private docIdToConnectedDoc(docId: string): connectedDocument {
        return {
            id: docId,
            href: appUrl.forEditDoc(docId, this.db())
        }
    }
}

export = connectedDocuments;
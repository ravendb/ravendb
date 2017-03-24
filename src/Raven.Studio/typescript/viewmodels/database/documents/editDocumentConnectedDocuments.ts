import router = require("plugins/router");

import document = require("models/database/documents/document");
import database = require("models/resources/database");
import collection = require("models/database/documents/collection");
import collectionsStats = require("models/database/documents/collectionsStats");
import recentDocumentsCtr = require("models/database/documents/recentDocuments");

import verifyDocumentsIDsCommand = require("commands/database/documents/verifyDocumentsIDsCommand");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");
import getDocumentRevisionsCommand = require("commands/database/documents/getDocumentRevisionsCommand");

import appUrl = require("common/appUrl");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import documentHelpers = require("common/helpers/database/documentHelpers");
import starredDocumentsStorage = require("common/storage/starredDocumentsStorage");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");

type connectedDocsTabs = "related" | "collection" | "recent" | "revisions";

class connectedDocuments {

    loadDocumentAction: (docId: string) => void;
    document: KnockoutObservable<document>;
    db: KnockoutObservable<database>;
    searchInput = ko.observable<string>("");
    columns: virtualColumn[] = [
        new hyperlinkColumn<connectedDocument>(x => x.id, x => x.href, "", "100%")
    ];
    currentDocumentIsStarred = ko.observable<boolean>(false);
    currentTab = ko.observable<connectedDocsTabs>("related");
    recentDocuments = new recentDocumentsCtr();
    isRelatedActive = ko.pureComputed(() => this.currentTab() === "related");
    isCollectionActive = ko.pureComputed(() => this.currentTab() === "collection");
    isRecentActive = ko.pureComputed(() => this.currentTab() === "recent");
    isRevisionsActive = ko.pureComputed(() => this.currentTab() === "revisions");

    gridController = ko.observable<virtualGridController<connectedDocument>>();

    revisionsEnabled = ko.pureComputed(() => {
        const doc = this.document();

        if (!doc) {
            return false;
        }
        return doc.__metadata.hasFlag("Versioned");
    });

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
            case "related":
                return this.fetchRelatedDocs(skip, take);
            case "collection":
                return this.fetchCollectionDocs(skip, take);
            case "recent":
                return this.fetchRecentDocs(skip, take);
            case "revisions":
                return this.fetchRevisionDocs(skip, take);
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

    fetchRevisionDocs(skip: number, take: number): JQueryPromise<pagedResult<connectedDocument>> {
        //TODO: should endpoint return results in different order - newest first?
        const doc = this.document();

        if (doc.__metadata.hasFlag("Versioned")) {
            const fetchTask = $.Deferred<pagedResult<connectedDocument>>();
            new getDocumentRevisionsCommand(doc.getId(), this.db(), skip, take, true)
                .execute()
                .done(result => {
                    fetchTask.resolve({
                        items: result.items.map(x => this.revisionToConnectedDocument(x)),
                        totalResultCount: result.totalResultCount
                    });
                })
                .fail(xhr => fetchTask.reject(xhr));

            return fetchTask.promise();
        } else {
            return $.Deferred<pagedResult<connectedDocument>>().resolve({
                items: [],
                totalResultCount: 0
            });
        }
    }

    private revisionToConnectedDocument(doc: document): connectedDocument {
        return {
            href: appUrl.forViewDocumentAtRevision(doc.getId(), doc.__metadata.etag(), this.db()),
            id: doc.__metadata.lastModified()
        };
    }

    emptyDocResult(): JQueryPromise<pagedResult<connectedDocument>> {
        return $.Deferred<pagedResult<connectedDocument>>().resolve({
            items: [],
            totalResultCount: 0
        }).promise();
    }

    activateRelated() {
        this.currentTab("related");
    }

    activateCollection() {
        this.currentTab("collection");
    }

    activateRecent() {
        this.currentTab("recent");
    }

    activateRevisions() {
        this.currentTab("revisions");
    }

    onDocumentDeleted() {
        this.recentDocuments.documentRemoved(this.db(), this.document().getId());
        const previous = this.recentDocuments.getPreviousDocument(this.db());
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
    }

    private onDocumentLoaded(document: document) {
        if (document) {
            this.recentDocuments.appendRecentDocument(this.db(), this.document().getId());
            this.currentDocumentIsStarred(starredDocumentsStorage.isStarred(this.db(), this.document().getId()));
        }
    }

    onDocumentSaved() {
        if (this.currentTab() === "revisions") {
            this.gridController().reset();
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
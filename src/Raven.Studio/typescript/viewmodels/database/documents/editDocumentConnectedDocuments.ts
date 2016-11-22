import router = require("plugins/router");

import document = require("models/database/documents/document");
import database = require("models/resources/database");
import collection = require("models/database/documents/collection");
import collectionsStats = require("models/database/documents/collectionsStats");
import recentDocumentsCtr = require("models/database/documents/recentDocuments");

import verifyDocumentsIDsCommand = require("commands/database/documents/verifyDocumentsIDsCommand");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");

import appUrl = require("common/appUrl");
import pagedResultSet = require("common/pagedResultSet");
import pagedResult = require("widgets/virtualGrid/pagedResult"); // Paged result for the new virtual grid.
import virtualGrid = require("widgets/virtualGrid/virtualGrid");
import virtualColumn = require("widgets/virtualGrid/virtualColumn");
import hyperlinkColumn = require("widgets/virtualGrid/hyperlinkColumn");
import documentHelpers = require("common/helpers/database/documentHelpers");
import starredDocumentsStorage = require("common/starredDocumentsStorage");

class connectedDocuments {
    
    loadDocumentAction: (docId: string) => void;
    document: KnockoutObservable<document>;
    db: KnockoutObservable<database>;
    searchInput = ko.observable<string>("");
    columns: virtualColumn[] = [
        new hyperlinkColumn("id", "href", "", "100%")
    ];
    currentDocumentIsStarred = ko.observable<boolean>(false);
    currentResultSet = ko.pureComputed<Array<connectedDocument>>(() => { //TODO: implement virtual paging
        switch (connectedDocuments.currentTab()) {
            case connectedDocuments.connectedDocsTabs.related:
                return this.relatedDocumentHrefs();
            case connectedDocuments.connectedDocsTabs.collection:
                return this.collectionDocumentHrefs();
            case connectedDocuments.connectedDocsTabs.recent:
                return this.recentDocuments.getTopRecentDocuments(this.db(), this.document().getId());
            case connectedDocuments.connectedDocsTabs.starred:
                return this.starredDocumentsHrefs();
            default:
                return [];
        }
    });

    static connectedDocsTabs = {
        related: "related",
        collection: "collection",
        recent: "recent",
        starred: "starred"
    };
    static currentTab = ko.observable<string>(connectedDocuments.connectedDocsTabs.related);

    constructor(document: KnockoutObservable<document>, db: KnockoutObservable<database>, loadDocument: (docId: string) => void) {
        this.document = document;
        this.db = db;
        this.document.subscribe((doc) => this.onDocumentLoaded(doc));
        this.loadDocumentAction = loadDocument;
        virtualGrid.install();
    }

    fetchChunk(skip: number, take: number): JQueryPromise<pagedResult<connectedDocument>> {
        const deferred = $.Deferred<pagedResult<connectedDocument>>();

        const relatedDocumentsCandidates: string[] = documentHelpers.findRelatedDocumentsCandidates(this.document());
        const docIDsVerifyCommand = new verifyDocumentsIDsCommand(relatedDocumentsCandidates, this.db(), true, true);
        docIDsVerifyCommand.execute()
            .done((verifiedIDs: string[]) => {
                var connectedDocs: connectedDocument[] = verifiedIDs.map(verified => {
                    return {
                        id: verified.toString(),
                        href: appUrl.forEditDoc(verified.toString(), this.db())
                    } as connectedDocument;
                });
                deferred.resolve({
                    items: connectedDocs, 
                    skip: skip,
                    take: take,
                    totalCount: connectedDocs.length
                }); 
            });

        return deferred.promise();
    }

    filteredResultSet = ko.pureComputed<Array<connectedDocument>>(() => {
        var itemsToFilter = this.currentResultSet();
        var criteria = this.searchInput().toLowerCase();

        if (!criteria) {
            return itemsToFilter;
        }

        return itemsToFilter.filter(x => x.id.toLowerCase().contains(criteria));
    });

    relatedDocumentHrefs = ko.observable<Array<connectedDocument>>();
    collectionDocumentHrefs = ko.observable<Array<connectedDocument>>();
    recentDocuments = new recentDocumentsCtr();
    starredDocumentsHrefs = ko.observable<Array<connectedDocument>>();

    isRelatedActive = ko.pureComputed(() => connectedDocuments.currentTab() === connectedDocuments.connectedDocsTabs.related);
    isCollectionActive = ko.pureComputed(() => connectedDocuments.currentTab() === connectedDocuments.connectedDocsTabs.collection);
    isRecentActive = ko.pureComputed(() => connectedDocuments.currentTab() === connectedDocuments.connectedDocsTabs.recent);
    isStarredActive = ko.pureComputed(() => connectedDocuments.currentTab() === connectedDocuments.connectedDocsTabs.starred);

    activateRelated() {
        connectedDocuments.currentTab(connectedDocuments.connectedDocsTabs.related);
    }

    activateCollection() {
        connectedDocuments.currentTab(connectedDocuments.connectedDocsTabs.collection);
    }

    activateRecent() {
        connectedDocuments.currentTab(connectedDocuments.connectedDocsTabs.recent);
    }

    activateStarred() {
        connectedDocuments.currentTab(connectedDocuments.connectedDocsTabs.starred);
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
        starredDocumentsStorage.markDocument(this.db(), this.document().getId(), !this.currentDocumentIsStarred());
        this.initStarred(this.document());
    }

    private onDocumentLoaded(document: document) {
        if (document) {

            this.recentDocuments.appendRecentDocument(this.db(), document.getId());
            this.initRelatedDocuments(document);
            this.initCollection(document);
            this.initStarred(document);
        }
    }

    private initRelatedDocuments(document: document): void {
        const relatedDocumentsCandidates: string[] = documentHelpers.findRelatedDocumentsCandidates(document);
        const docIDsVerifyCommand = new verifyDocumentsIDsCommand(relatedDocumentsCandidates, this.db(), true, true);
        const response = docIDsVerifyCommand.execute();
        response.done((verifiedIDs: Array<string>) => {
            this.relatedDocumentHrefs(verifiedIDs.map((verified: string) => {
                return {
                    id: verified.toString(),
                    href: appUrl.forEditDoc(verified.toString(), this.db())
                } as connectedDocument;
            }));
        });
    }

    private initCollection(document: document): void { //TODO: this code should be evaulated in lazy fashion - and don't download all documents!
        const entityName = document.getEntityName();

        if (entityName) {
            new getCollectionsStatsCommand(this.db())
                .execute()
                .done((stats: collectionsStats) => {
                    const totalCount = stats.getCollectionCount(entityName);

                    const newCollection = new collection(entityName, this.db());
                    newCollection.fetchDocuments(0, totalCount)
                        .then((result: pagedResultSet<any>) => {
                            this.collectionDocumentHrefs(result.items.map(item => ({
                                id: item.getId(),
                                href: appUrl.forEditDoc(item.getId(), this.db())
                            }) as connectedDocument));
                        });
                });
        }
    }

    private initStarred(document: document) {
        let starred = starredDocumentsStorage.getStarredDocuments(this.db());
        this.starredDocumentsHrefs(starred.map(x => ({
            id: x,
            href: appUrl.forEditDoc(x, this.db())
        }) as connectedDocument));

        this.currentDocumentIsStarred(starredDocumentsStorage.isStarred(this.db(), document.getId()));
    }
}

export = connectedDocuments;
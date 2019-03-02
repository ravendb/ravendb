import router = require("plugins/router");

import document = require("models/database/documents/document");
import database = require("models/resources/database");
import recentDocumentsCtr = require("models/database/documents/recentDocuments");

import verifyDocumentsIDsCommand = require("commands/database/documents/verifyDocumentsIDsCommand");
import getDocumentRevisionsCommand = require("commands/database/documents/getDocumentRevisionsCommand");

import appUrl = require("common/appUrl");
import endpoints = require("endpoints");
import generalUtils = require("common/generalUtils");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import documentHelpers = require("common/helpers/database/documentHelpers");
import starredDocumentsStorage = require("common/storage/starredDocumentsStorage");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import downloader = require("common/downloader");
import viewHelpers = require("common/helpers/view/viewHelpers");
import editDocumentUploader = require("viewmodels/database/documents/editDocumentUploader");

type connectedDocsTabs = "attachments" | "counters" | "revisions" | "related" | "recent";



interface connectedDocumentItem { 
    id: string;
    href: string;
    deletedRevision: boolean;
}

interface connectedRevisionDocumentItem extends connectedDocumentItem {
    revisionChangeVector: string;
}

class connectedDocuments {

    // static field to remember current tab between navigation
    static currentTab = ko.observable<connectedDocsTabs>("attachments"); 

    loadDocumentAction: (docId: string) => void;
    compareWithRevisionAction: (revisionChangeVector: string) => JQueryPromise<document>;
    document: KnockoutObservable<document>;
    db: KnockoutObservable<database>;
    inReadOnlyMode: KnockoutObservable<boolean>;
    searchInput = ko.observable<string>("");
    searchInputVisible: KnockoutObservable<boolean>;
    clearSearchInputSubscription: KnockoutSubscription;
    gridResetSubscription: KnockoutSubscription;
    revisionsCount = ko.observable<number>();   

    crudActionsProvider: () => editDocumentCrudActions;
    
    docsColumns: virtualColumn[];
    revisionsColumns: virtualColumn[];
    attachmentsColumns: virtualColumn[];
    attachmentsInReadOnlyModeColumns: virtualColumn[];
    countersColumns: virtualColumn[];
    revisionCountersColumns: virtualColumn[];
    
    private downloader = new downloader();
    currentDocumentIsStarred = ko.observable<boolean>(false);

    recentDocuments = new recentDocumentsCtr();
    
    isRelatedActive = ko.pureComputed(() => connectedDocuments.currentTab() === "related");
    isAttachmentsActive = ko.pureComputed(() => connectedDocuments.currentTab() === "attachments");
    isRecentActive = ko.pureComputed(() => connectedDocuments.currentTab() === "recent");
    isRevisionsActive = ko.pureComputed(() => connectedDocuments.currentTab() === "revisions");
    isCountersActive = ko.pureComputed(() => connectedDocuments.currentTab() === "counters");        
    
    isUploaderActive: KnockoutComputed<boolean>;
    
    isArtificialDocument: KnockoutComputed<boolean>;
    isHiloDocument: KnockoutComputed<boolean>;  

    gridController = ko.observable<virtualGridController<connectedDocumentItem | attachmentItem | counterItem>>();
    uploader: editDocumentUploader;

    revisionsEnabled = ko.pureComputed(() => {
        const doc = this.document();

        if (!doc) {
            return false;
        }
        return doc.__metadata.hasFlag("HasRevisions") || doc.__metadata.hasFlag("DeleteRevision");
    });

    constructor(document: KnockoutObservable<document>,
        db: KnockoutObservable<database>,
        loadDocument: (docId: string) => void,
        compareWithRevision: (revisionChangeVector: string) => JQueryPromise<document>,
        isCreatingNewDocument: KnockoutObservable<boolean>,
        crudActionsProvider: () => editDocumentCrudActions,
        inReadOnlyMode: KnockoutObservable<boolean>) {

        _.bindAll(this, "toggleStar" as keyof this);

        this.document = document;
        this.db = db;
        this.inReadOnlyMode = inReadOnlyMode;
        this.document.subscribe((doc) => this.onDocumentLoaded(doc));
        this.loadDocumentAction = loadDocument;
        this.compareWithRevisionAction = compareWithRevision;
        this.uploader = new editDocumentUploader(document, db, () => this.afterUpload());
        this.crudActionsProvider = crudActionsProvider;

        this.isUploaderActive = ko.pureComputed(() => {
            const onAttachmentsPane = this.isAttachmentsActive();
            const newDoc = isCreatingNewDocument();
            const readOnly = inReadOnlyMode();
            return onAttachmentsPane && !newDoc && !readOnly;
        });

        this.searchInputVisible = ko.pureComputed(() => !this.isRevisionsActive() && !this.isRecentActive());
        this.searchInput.throttle(250).subscribe(() => {
            this.gridController().reset(false);
        });

        this.clearSearchInputSubscription = connectedDocuments.currentTab.subscribe(() => this.searchInput(""));

        this.isArtificialDocument = ko.pureComputed(() => {
            return this.document().__metadata.hasFlag("Artificial");
        });

        this.isHiloDocument = ko.pureComputed(() => {
            return this.document().__metadata.collection == "@hilo";
        });        
    }

    private initColumns() {
        this.docsColumns = [
            new hyperlinkColumn<connectedDocumentItem>(this.gridController() as virtualGridController<any>, x => x.id, x => x.href, "", "100%")
        ];

        const revisionColumn = new hyperlinkColumn<connectedRevisionDocumentItem>(this.gridController() as virtualGridController<any>, x => x.id, x => x.href, "", "75%",
            {
                extraClass: item => item.deletedRevision ? "deleted-revision" : ""
            });
        const revisionCompareColumn = new actionColumn<connectedRevisionDocumentItem>(this.gridController() as virtualGridController<any>, (x, idx, e) => this.compareRevision(x, idx, e), "Diff", x => `<i title="Compare document with this revision" class="icon-diff"></i>`, "25%",
            {
                extraClass: doc => doc.deletedRevision ? 'deleted-revision-compare' : '',
                title: (item: connectedRevisionDocumentItem) => "Compare current document with this revision"
            }); 
        
        const isDeleteRevision = this.document().__metadata.hasFlag("DeleteRevision");
        
        this.revisionsColumns = isDeleteRevision ? [revisionColumn] : [revisionColumn, revisionCompareColumn];
        
        this.attachmentsColumns = [
            new actionColumn<attachmentItem>(this.gridController() as virtualGridController<any>, x => this.downloadAttachment(x), "Name", x => x.name, "160px",
                {
                    extraClass: () => 'btn-link',
                    title: (item: attachmentItem) => "Download file: " + item.name
                }),
            new textColumn<attachmentItem>(this.gridController() as virtualGridController<any>, x => generalUtils.formatBytesToSize(x.size), "Size", "70px", { extraClass: () => 'filesize' }),
            new actionColumn<attachmentItem>(this.gridController() as virtualGridController<any>, x => this.crudActionsProvider().deleteAttachment(x),
                "Delete",
                `<i class="icon-trash"></i>`,
                "35px",
                { title: () => 'Delete attachment', extraClass: () => 'file-trash' })
        ];

        this.attachmentsInReadOnlyModeColumns = [
            new actionColumn<attachmentItem>(this.gridController() as virtualGridController<any>, x => this.downloadAttachment(x), "Name", x => x.name, "195px",
                {
                    extraClass: () => 'btn-link',
                    title: () => "Download attachment"
                }),
            new textColumn<attachmentItem>(this.gridController() as virtualGridController<any>, x => generalUtils.formatBytesToSize(x.size), "Size", "70px", { extraClass: () => 'filesize' })
        ];

        this.countersColumns = [
            new textColumn<counterItem>(this.gridController() as virtualGridController<any>, x => x.counterName, "Counter name", "160px", 
                { title: () => "Counter name" }),
            new textColumn<counterItem>(this.gridController() as virtualGridController<any>, x => x.totalCounterValue, "Counter total value", "100px", 
                { title: (x) => "Total value is: " + x.totalCounterValue.toLocaleString() }),
            new actionColumn<counterItem>(this.gridController() as virtualGridController<any>, 
                 x => this.crudActionsProvider().setCounter(x),
                "Edit",
                `<i class="icon-edit"></i>`,
                "35px",
                { title: () => 'Edit counter' }),  
            new actionColumn<counterItem>(this.gridController() as virtualGridController<any>, 
                 x => this.crudActionsProvider().deleteCounter(x),
                "Delete",
                `<i class="icon-trash"></i>`,
                "35px",
                { title: () => 'Delete counter' }),
        ];

        this.revisionCountersColumns = [
            new textColumn<counterItem>(this.gridController() as virtualGridController<any>, x => x.counterName, "Counter name", "60%",
                { title: () => "Counter name" }),
            new textColumn<counterItem>(this.gridController() as virtualGridController<any>, x => x.totalCounterValue, "Counter total value", "40%",
                { title: (x) => "Total value is: " + x.totalCounterValue.toLocaleString() })
        ];
    }

    compositionComplete() {
        const grid = this.gridController();
        this.initColumns();
        grid.headerVisible(false);
        grid.init((s, t) => this.fetchCurrentTabItems(s, t), () => {
            if (connectedDocuments.currentTab() === "attachments") {
                return this.inReadOnlyMode() ? this.attachmentsInReadOnlyModeColumns : this.attachmentsColumns;
            }
            if (connectedDocuments.currentTab() === "counters") {
                const doc = this.document();
                if (doc && doc.__metadata && doc.__metadata.hasFlag("Revision")) {
                    return this.revisionCountersColumns;
                }
                return this.countersColumns;
            }
            
            if (connectedDocuments.currentTab() === "revisions") {
                return this.revisionsColumns;
            }
            return this.docsColumns;
        });

        this.gridResetSubscription = connectedDocuments.currentTab.subscribe(() => this.gridController().reset());
    }

    dispose() {
        this.clearSearchInputSubscription.dispose();
        this.gridResetSubscription.dispose();
    }

    private fetchCurrentTabItems(skip: number, take: number): JQueryPromise<pagedResult<connectedDocumentItem | attachmentItem | counterItem>> {
        const doc = this.document();
        if (!doc) {
            return connectedDocuments.emptyDocResult<connectedDocumentItem | attachmentItem | counterItem>();
        }

        switch (connectedDocuments.currentTab()) {
            case "related":
                return this.fetchRelatedDocs(skip, take);
            case "attachments":
                return this.crudActionsProvider().fetchAttachments(this.searchInput().toLocaleLowerCase(), skip, take);
            case "recent":
                return this.fetchRecentDocs(skip, take);
            case "revisions":
                return this.fetchRevisionDocs(skip, take);
            case "counters": 
                return this.crudActionsProvider().fetchCounters(this.searchInput().toLocaleLowerCase(), skip, take); 
            default: return connectedDocuments.emptyDocResult<connectedDocumentItem | attachmentItem | counterItem>();
        }
    }

    fetchRelatedDocs(skip: number, take: number): JQueryPromise<pagedResult<connectedDocumentItem>> {
        const deferred = $.Deferred<pagedResult<connectedDocumentItem>>();
        const search = this.searchInput().toLocaleLowerCase();

        let relatedDocumentsCandidates: string[] = documentHelpers.findRelatedDocumentsCandidates(this.document());

        if (search) {
            relatedDocumentsCandidates = relatedDocumentsCandidates.filter(x => x.toLocaleLowerCase().includes(search));
        }

        const docIDsVerifyCommand = new verifyDocumentsIDsCommand(relatedDocumentsCandidates, this.db());
        docIDsVerifyCommand.execute()
            .done((verifiedIDs: string[]) => {
                const connectedDocs: connectedDocumentItem[] = verifiedIDs.map(id => this.docIdToConnectedDoc(id));
                deferred.resolve({
                    items: connectedDocs,
                    totalResultCount: connectedDocs.length
                });
            });

        return deferred.promise();
    }
    
    fetchRecentDocs(skip: number, take: number): JQueryPromise<pagedResult<connectedDocumentItem>> {
        const doc = this.document();

        const recentDocs = this.recentDocuments.getTopRecentDocuments(this.db(), doc.getId());
        return $.Deferred<pagedResult<connectedDocumentItem>>().resolve({
            items: recentDocs.map(x => ({ id: x.id, href: x.href, deletedRevision: false })),
            totalResultCount: recentDocs.length,
        }).promise();
    }

    fetchRevisionDocs(skip: number, take: number): JQueryPromise<pagedResult<connectedDocumentItem>> {
        const doc = this.document();

        if (!doc.__metadata.hasFlag("HasRevisions") && !doc.__metadata.hasFlag("DeleteRevision")) {
            return connectedDocuments.emptyDocResult<connectedDocumentItem>();
        }

        const fetchTask = $.Deferred<pagedResult<connectedDocumentItem>>();
        new getDocumentRevisionsCommand(doc.getId(), this.db(), skip, take, true)
            .execute()
            .done(result => {
                const mappedResults = result.items.map(x => this.revisionToConnectedDocument(x));
                this.revisionsCount(result.totalResultCount);
                fetchTask.resolve({
                    items: mappedResults,
                    totalResultCount: result.totalResultCount
                });

                if (doc.__metadata.hasFlag("Revision") || doc.__metadata.hasFlag("DeleteRevision")) {
                    const changeVector = doc.__metadata.changeVector();
                    const resultIdx = result.items.findIndex(x => x.__metadata.changeVector() === changeVector);
                    if (resultIdx >= 0) {
                        this.gridController().setSelectedItems([mappedResults[resultIdx]]);
                    }
                }
            })
            .fail(xhr => fetchTask.reject(xhr));

        return fetchTask.promise();
    }

    private revisionToConnectedDocument(doc: document): connectedRevisionDocumentItem {
        const changeVector = doc.__metadata.changeVector();
        return {
            href: appUrl.forViewDocumentAtRevision(doc.getId(), changeVector, this.db()),
            id: doc.__metadata.lastModified(),
            deletedRevision: doc.__metadata.hasFlag("DeleteRevision"),
            revisionChangeVector: changeVector
        };
    }

    static emptyDocResult<T>(): JQueryPromise<pagedResult<T>> {
        return $.Deferred<pagedResult<T>>().resolve({
            items: [],
            totalResultCount: 0
        }).promise();
    }

    private compareRevision(revision: connectedRevisionDocumentItem, idx: number, event: JQueryEventObject) {
        const $target = $(event.target);
        $target.addClass("btn-spinner");
        
        this.compareWithRevisionAction(revision.revisionChangeVector)
            .always(() => $target.removeClass("btn-spinner"));
    }
    
    private downloadAttachment(file: attachmentItem) {
        const args = {
            id: file.documentId,
            name: file.name
        };

        const doc = this.document();
        if (doc.__metadata.hasFlag("Revision")) {
            this.downloadAttachmentAtRevision(doc, args);
        } else {
            const url = endpoints.databases.attachment.attachments + appUrl.urlEncodeArgs(args);
            this.downloader.download(this.db(), url);    
        }
    }

    private downloadAttachmentAtRevision(doc: document, file: { id: string; name: string }) {
        const $form = $("#downloadAttachmentAtRevisionForm");
        const $changeVector = $("[name=ChangeVectorAndType]", $form);

        const payload = {
            ChangeVector: doc.__metadata.changeVector(),
            Type: "Revision"
        };

        const url = endpoints.databases.attachment.attachments + appUrl.urlEncodeArgs(file);

        $form.attr("action", appUrl.forDatabaseQuery(this.db()) + url);
        
        $changeVector.val(JSON.stringify(payload));
        $form.submit();
    }

    private afterUpload() {
        this.searchInput("");
        this.loadDocumentAction(this.document().getId());
    }

    activateRelated() {
        connectedDocuments.currentTab("related");
    }

    activateAttachments() {
        connectedDocuments.currentTab("attachments");
    }

    activateRecent() {
        connectedDocuments.currentTab("recent");
    }

    activateRevisions(blink: boolean) {
        if (blink) {
            viewHelpers.animate($("#revisions_pane"), "blink-style");
        }
        connectedDocuments.currentTab("revisions");
    }

    activateCounters() {
        connectedDocuments.currentTab("counters");
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

            if (connectedDocuments.currentTab() === "revisions" && (!document.__metadata.hasFlag("HasRevisions") && !document.__metadata.hasFlag("DeleteRevision"))) {
                // this will also reset grid
                connectedDocuments.currentTab("attachments");
            } else {
                if (this.gridController()) {
                    this.gridController().reset(true);
                }
            }
        }
    }

    onDocumentSaved() {
        if (connectedDocuments.currentTab() === "revisions") {
            this.gridController().reset();
        }
    }
    
    reload() {
        this.gridController().reset(false);
    }

    private docIdToConnectedDoc(docId: string): connectedDocumentItem {
        return {
            id: docId,
            href: appUrl.forEditDoc(docId, this.db()),
            deletedRevision: false
        }
    }
}

export = connectedDocuments;

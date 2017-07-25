import router = require("plugins/router");

import document = require("models/database/documents/document");
import database = require("models/resources/database");
import recentDocumentsCtr = require("models/database/documents/recentDocuments");

import verifyDocumentsIDsCommand = require("commands/database/documents/verifyDocumentsIDsCommand");
import getDocumentRevisionsCommand = require("commands/database/documents/getDocumentRevisionsCommand");
import deleteAttachmentCommand = require("commands/database/documents/attachments/deleteAttachmentCommand");

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
import messagePublisher = require("common/messagePublisher");
import editDocumentUploader = require("viewmodels/database/documents/editDocumentUploader");

type connectedDocsTabs = "attachments" | "revisions" | "related" | "recent";

interface attachmentItem {
    documentId: string;
    name: string;
    contentType: string;
    size: number;
}

interface connectedDocumentItem {
    id: string;
    href: string;
    deletedRevision: boolean;
}

class connectedDocuments {

    // static field to remember current tab between navigation
    static currentTab = ko.observable<connectedDocsTabs>("attachments"); 

    loadDocumentAction: (docId: string) => void;
    document: KnockoutObservable<document>;
    db: KnockoutObservable<database>;
    inReadOnlyMode: KnockoutObservable<boolean>;
    searchInput = ko.observable<string>("");
    searchInputVisible: KnockoutObservable<boolean>;
    clearSearchInputSubscription: KnockoutSubscription;
    gridResetSubscription: KnockoutSubscription;
    revisionsCount = ko.observable<number>();

    docsColumns: virtualColumn[];
    attachmentsColumns: virtualColumn[];
    attachmentsInReadOnlyModeColumns: virtualColumn[];

    private downloader = new downloader();
    currentDocumentIsStarred = ko.observable<boolean>(false);

    recentDocuments = new recentDocumentsCtr();
    isRelatedActive = ko.pureComputed(() => connectedDocuments.currentTab() === "related");
    isAttachmentsActive = ko.pureComputed(() => connectedDocuments.currentTab() === "attachments");
    isRecentActive = ko.pureComputed(() => connectedDocuments.currentTab() === "recent");
    isRevisionsActive = ko.pureComputed(() => connectedDocuments.currentTab() === "revisions");
    isUploaderActive: KnockoutComputed<boolean>;
    showUploadNotAvailable: KnockoutComputed<boolean>;

    gridController = ko.observable<virtualGridController<connectedDocumentItem | attachmentItem>>();
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
        isCreatingNewDocument: KnockoutObservable<boolean>,
        inReadOnlyMode: KnockoutObservable<boolean>) {

        _.bindAll(this, "toggleStar" as keyof this);

        this.document = document;
        this.db = db;
        this.inReadOnlyMode = inReadOnlyMode;
        this.document.subscribe((doc) => this.onDocumentLoaded(doc));
        this.loadDocumentAction = loadDocument;
        this.uploader = new editDocumentUploader(document, db, () => this.afterUpload());

        this.isUploaderActive = ko.pureComputed(() => {
            const onAttachmentsPane = this.isAttachmentsActive();
            const newDoc = isCreatingNewDocument();
            const readOnly = inReadOnlyMode();
            return onAttachmentsPane && !newDoc && !readOnly;
        });
        this.showUploadNotAvailable = ko.pureComputed(() => {
            const onAttachmentsPane = this.isAttachmentsActive();
            const readOnly = inReadOnlyMode();
            return onAttachmentsPane && readOnly;
        });

        this.searchInputVisible = ko.pureComputed(() => !this.isRevisionsActive() && !this.isRecentActive());
        this.searchInput.throttle(250).subscribe(() => {
            this.gridController().reset(false);
        });

        this.clearSearchInputSubscription = connectedDocuments.currentTab.subscribe(() => this.searchInput(""));
    }

    private initColumns() {
        this.docsColumns = [
            new hyperlinkColumn<connectedDocumentItem>(this.gridController() as virtualGridController<any>, x => x.id, x => x.href, "", "100%",
                {
                    extraClass: item => item.deletedRevision ? "deleted-revision" : ""
                })
        ];

        this.attachmentsColumns = [
            new actionColumn<attachmentItem>(this.gridController() as virtualGridController<any>, x => this.downloadAttachment(x), "Name", x => x.name, "160px",
                {
                    extraClass: () => 'btn-link',
                    title: (item: attachmentItem) => "Download file: " + item.name
                }),
            new textColumn<attachmentItem>(this.gridController() as virtualGridController<any>, x => generalUtils.formatBytesToSize(x.size), "Size", "70px", { extraClass: () => 'filesize' }),
            new actionColumn<attachmentItem>(this.gridController() as virtualGridController<any>, x => this.deleteAttachment(x),
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
    }

    compositionComplete() {
        const grid = this.gridController();
        this.initColumns();
        grid.headerVisible(false);
        grid.init((s, t) => this.fetchCurrentTabItems(s, t), () => {
            if (connectedDocuments.currentTab() === "attachments") {
                return this.inReadOnlyMode() ? this.attachmentsInReadOnlyModeColumns : this.attachmentsColumns;
            }
            return this.docsColumns;
        });

        this.gridResetSubscription = connectedDocuments.currentTab.subscribe(() => this.gridController().reset());
    }

    dispose() {
        this.clearSearchInputSubscription.dispose();
        this.gridResetSubscription.dispose();
    }

    private fetchCurrentTabItems(skip: number, take: number): JQueryPromise<pagedResult<connectedDocumentItem | attachmentItem>> {
        const doc = this.document();
        if (!doc) {
            return connectedDocuments.emptyDocResult<connectedDocumentItem | attachmentItem>();
        }

        if (connectedDocuments.currentTab() !== "revisions") {
            // going to different tab - stop displaying revisions count
            this.revisionsCount(null);
        }

        switch (connectedDocuments.currentTab()) {
            case "related":
                return this.fetchRelatedDocs(skip, take);
            case "attachments":
                return this.fetchAttachments(skip, take);
            case "recent":
                return this.fetchRecentDocs(skip, take);
            case "revisions":
                return this.fetchRevisionDocs(skip, take);
            default: return connectedDocuments.emptyDocResult<connectedDocumentItem | attachmentItem>();
        }
    }

    fetchRelatedDocs(skip: number, take: number): JQueryPromise<pagedResult<connectedDocumentItem>> {
        const deferred = $.Deferred<pagedResult<connectedDocumentItem>>();
        const search = this.searchInput().toLocaleLowerCase();

        let relatedDocumentsCandidates: string[] = documentHelpers.findRelatedDocumentsCandidates(this.document());

        if (search) {
            relatedDocumentsCandidates = relatedDocumentsCandidates.filter(x => x.includes(search));
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

    fetchAttachments(skip: number, take: number): JQueryPromise<pagedResult<attachmentItem>> { 
        const doc = this.document();
        const search = this.searchInput().toLocaleLowerCase();
        
        let attachments: documentAttachmentDto[] = doc.__metadata.attachments() || [];

        if (search) {
            attachments = attachments.filter(file => file.Name.includes(search));
        }

        const mappedFiles = attachments.map(file => ({
            documentId: doc.getId(),
            name: file.Name,
            contentType: file.ContentType,
            size: file.Size
        } as attachmentItem));

        return $.Deferred<pagedResult<attachmentItem>>().resolve({
            items: mappedFiles,
            totalResultCount: mappedFiles.length
        });
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

        const fetchTask = $.Deferred<pagedResult<connectedDocument>>();
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

    private revisionToConnectedDocument(doc: document): connectedDocumentItem {
        return {
            href: appUrl.forViewDocumentAtRevision(doc.getId(), doc.__metadata.changeVector(), this.db()),
            id: doc.__metadata.lastModified(),
            deletedRevision: doc.__metadata.hasFlag("DeleteRevision")
        };
    }

    private static emptyDocResult<T>(): JQueryPromise<pagedResult<T>> {
        return $.Deferred<pagedResult<T>>().resolve({
            items: [],
            totalResultCount: 0
        }).promise();
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
        //TODO: single auth token ?

        const $form = $("#downloadAttachmentAtRevisionForm");
        const $changeVector = $("[name=ChangeVectorAndType]", $form);
        const changeVector = (doc.__metadata as any)['@change-vector'];

        const payload = {
            ChangeVector: changeVector,
            Type: "Revision"
        };

        const url = endpoints.databases.attachment.attachments + appUrl.urlEncodeArgs(file);

        $form.attr("action", appUrl.forDatabaseQuery(this.db()) + url);
        
        $changeVector.val(JSON.stringify(payload));
        $form.submit();
    }

    private deleteAttachment(file: attachmentItem) {
        viewHelpers.confirmationMessage("Delete attachment", `Are you sure you want to delete attachment: ${file.name}?`, ["Cancel", "Delete"])
            .done((result) => {
                if (result.can) {
                    new deleteAttachmentCommand(file.documentId, file.name, this.db())
                        .execute()
                        .done(() => {
                            messagePublisher.reportSuccess("Attachment was deleted.");
                            this.onAttachmentDeleted(file);
                        });
                }
            });
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

    private onAttachmentDeleted(file: attachmentItem) {
        // refresh document, as it contains information about attachments in metadata
        this.loadDocumentAction(file.documentId);
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

    private documentToConnectedDoc(doc: document): connectedDocumentItem {
        return {
            id: doc.getId(),
            href: appUrl.forEditDoc(doc.getId(), this.db()),
            deletedRevision: doc.__metadata.hasFlag("DeleteRevision")
        };
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
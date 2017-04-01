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

class connectedDocuments {

    // static field to remember current tab between navigation
    static currentTab = ko.observable<connectedDocsTabs>("attachments"); 
    static readonly currentDocument = 'Current Document';

    loadDocumentAction: (docId: string) => void;
    document: KnockoutObservable<document>;
    db: KnockoutObservable<database>;
    inReadOnlyMode: KnockoutObservable<boolean>;
    searchInput = ko.observable<string>(""); //TODO: not yet working!

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

    gridController = ko.observable<virtualGridController<connectedDocument | attachmentItem>>();
    uploader: editDocumentUploader;

    revisionsEnabled = ko.pureComputed(() => {
        const doc = this.document();

        if (!doc) {
            return false;
        }
        return doc.__metadata.hasFlag("Versioned");
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

        this.initColumns();

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
    }

    private initColumns() {
        this.docsColumns = [
            new hyperlinkColumn<connectedDocument>(x => x.id, x => x.href, "", "100%",
            {
                extraClass: (item) => {
                    return item.id === connectedDocuments.currentDocument ? 'current-document' : '';
                }
            })
        ];

        this.attachmentsColumns = [
            new actionColumn<attachmentItem>(x => this.downloadAttachment(x), "Name", x => x.name, "160px",
                {
                    extraClass: () => 'btn-link',
                    title: (item: attachmentItem) => "Download file: " + item.name
                }),
            new textColumn<attachmentItem>(x => generalUtils.formatBytesToSize(x.size), "Size", "70px", { extraClass: () => 'filesize' }),
            new actionColumn<attachmentItem>(x => this.deleteAttachment(x),
                "Delete",
                `<i class="icon-trash"></i>`,
                "35px",
                { title: () => 'Delete attachment', extraClass: () => 'file-trash' })
        ];

        this.attachmentsInReadOnlyModeColumns = [
            new actionColumn<attachmentItem>(x => this.downloadAttachment(x), "Name", x => x.name, "195px",
                {
                    extraClass: () => 'btn-link',
                    title: () => "Download attachment"
                }),
            new textColumn<attachmentItem>(x => generalUtils.formatBytesToSize(x.size), "Size", "70px", { extraClass: () => 'filesize' })
        ];
    }

    compositionComplete() {
        const grid = this.gridController();
        grid.headerVisible(false);
        grid.init((s, t) => this.fetchCurrentTabItems(s, t), () => {
            if (connectedDocuments.currentTab() === "attachments") {
                return this.inReadOnlyMode() ? this.attachmentsInReadOnlyModeColumns : this.attachmentsColumns;
            }
            return this.docsColumns;
        });

        connectedDocuments.currentTab.subscribe(() => this.gridController().reset());
    }

    private fetchCurrentTabItems(skip: number, take: number): JQueryPromise<pagedResult<connectedDocument | attachmentItem>> {
        const doc = this.document();
        if (!doc) {
            return connectedDocuments.emptyDocResult<connectedDocument | attachmentItem>();
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
            default: return connectedDocuments.emptyDocResult<connectedDocument | attachmentItem>();
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

    fetchAttachments(skip: number, take: number): JQueryPromise<pagedResult<attachmentItem>> {
        const doc = this.document();

        const attachments: Raven.Client.Documents.Operations.AttachmentResult[] = (doc.__metadata as any)["@attachments"] || [];
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

    fetchRecentDocs(skip: number, take: number): JQueryPromise<pagedResult<connectedDocument>> {
        const doc = this.document();

        const recentDocs = this.recentDocuments.getTopRecentDocuments(this.db(), doc.getId());
        return $.Deferred<pagedResult<connectedDocument>>().resolve({
            items: recentDocs,
            totalResultCount: recentDocs.length
        }).promise();
    }

    fetchRevisionDocs(skip: number, take: number): JQueryPromise<pagedResult<connectedDocument>> {
        //TODO: should endpoint return results in different order - newest first?
        const doc = this.document();

        if (!doc.__metadata.hasFlag("Versioned")) {
            return connectedDocuments.emptyDocResult<connectedDocument>();
        }

        const includeLatestFakeItem = skip === 0;

        const fakeCurrentItem = {
            id: connectedDocuments.currentDocument,
            href: appUrl.forEditDoc(doc.getId(), this.db())
        } as connectedDocument;

        // shift by one item, to serve fake item properly
        if (skip > 0) { skip--; }
        take--;

        const fetchTask = $.Deferred<pagedResult<connectedDocument>>();
        new getDocumentRevisionsCommand(doc.getId(), this.db(), skip, take, true)
            .execute()
            .done(result => {
                const mappedResults = result.items.map(x => this.revisionToConnectedDocument(x));
                fetchTask.resolve({
                    items: includeLatestFakeItem ? [fakeCurrentItem].concat(...mappedResults) : mappedResults,
                    totalResultCount: result.totalResultCount + 1 // include latest item
                });
            })
            .fail(xhr => fetchTask.reject(xhr));

        return fetchTask.promise();
    }

    private revisionToConnectedDocument(doc: document): connectedDocument {
        return {
            href: appUrl.forViewDocumentAtRevision(doc.getId(), doc.__metadata.etag(), this.db()),
            id: doc.__metadata.lastModified()
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

        const url = endpoints.databases.attachment.attachments + appUrl.urlEncodeArgs(args);
        this.downloader.download(this.db(), url);
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

    activateRevisions() {
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

            if (connectedDocuments.currentTab() === "revisions" && !document.__metadata.hasFlag("Versioned")) {
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
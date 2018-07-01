import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/resources/database");
import document = require("models/database/documents/document");
import documentMetadata = require("models/database/documents/documentMetadata");
import viewHelpers = require("common/helpers/view/viewHelpers");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import messagePublisher = require("common/messagePublisher");
import eventsCollector = require("common/eventsCollector");
import docsIdsBasedOnQueryFetcher = require("viewmodels/database/patch/docsIdsBasedOnQueryFetcher");
import showDataDialog = require("viewmodels/common/showDataDialog");
import patchCommand = require("commands/database/patch/patchCommand");
import validationHelpers = require("viewmodels/common/validationHelpers");
import queryUtil = require("common/queryUtil");

class patchTester extends viewModelBase {

    private db: KnockoutObservable<database>;

    testMode = ko.observable<boolean>(false);
    query: KnockoutObservable<string>;
    documentId = ko.observable<string>();

    beforeDoc = ko.observable<any>();
    afterDoc = ko.observable<any>();

    actions = {
        loadDocument: ko.observableArray<string>(),
        putDocument: ko.observableArray<any>(),
        deleteDocument: ko.observableArray<string>(),
        info: ko.observableArray<string>()
    };

    showObjectsInPutSection = ko.observable<boolean>(false);

    spinners = {
        testing: ko.observable<boolean>(false),
        loadingDocument: ko.observable<boolean>(false),
        autocomplete: ko.observable<boolean>(false),
        preview: ko.observable<boolean>(false)
    };

    private $body = $('body');

    docsIdsAutocompleteResults = ko.observableArray<string>([]);
    docsIdsAutocompleteSource: docsIdsBasedOnQueryFetcher;

    validationGroup: KnockoutValidationGroup;
    testDocumentValidationGroup: KnockoutValidationGroup;

    constructor(query: KnockoutObservable<string>, db: KnockoutObservable<database>) {
        super();
        this.query = query;
        this.db = db;
        this.initObservables();

        this.bindToCurrentInstance(
            "closeTestMode", "enterTestMode", "runTest", "onAutocompleteOptionSelected");

        this.validationGroup = ko.validatedObservable({
            script: this.query,
            documentId: this.documentId
        });

        this.testDocumentValidationGroup = ko.validatedObservable({
            documentId: this.documentId
        });

        this.docsIdsAutocompleteSource = new docsIdsBasedOnQueryFetcher(this.db);
    }

    formatAsJson(input: KnockoutObservable<any> | any) {
        return ko.pureComputed(() => {
            const value = ko.unwrap(input);
            if (_.isUndefined(value)) {
                return "";
            } else {
                const json = JSON.stringify(value, null, 4);
                return Prism.highlight(json, (Prism.languages as any).javascript);
            }
        });
    }

    private initObservables() {
        this.testMode.subscribe(testMode => {
            this.$body.toggleClass('show-test', testMode);
        });

        this.documentId.extend({
            required: true
        });

        validationHelpers.addDocumentIdValidation(
            this.documentId, this.activeDatabase, ko.pureComputed(() => !!this.docsIdsAutocompleteResults()));

        this.query.subscribe(x => 
            this.docsIdsAutocompleteResults([]));

        this.documentId.throttle(250).subscribe(item => {
           if (!item) {
               return;
           }

           this.spinners.autocomplete(true);
           this.docsIdsAutocompleteSource.fetch(this.query())
                .done(results => {
                    const term = item.toLowerCase();
                    results = _.take(results.filter(x => x.toLowerCase().indexOf(term) !== -1), 10);
                    this.docsIdsAutocompleteResults(results);
                })
                .fail((err: JQueryXHR) => messagePublisher.reportError("Error fetching documents IDs", err.responseText, err.statusText))
                .always(() => this.spinners.autocomplete(false));
        });
    }

    closeTestMode() {
        this.testMode(false);
    }

    enterTestMode(documentIdToUse: string) {
        this.testMode(true);

        documentIdToUse = documentIdToUse;
        this.documentId(documentIdToUse);

        this.validationGroup.errors.showAllMessages(false);

        if (documentIdToUse) {
            this.loadDocument();

            if (this.isValid(this.validationGroup, false)) {
                this.runTest();
            }
        }
    }

    resetForm() {
        this.actions.loadDocument([]);
        this.actions.putDocument([]);
        this.actions.deleteDocument([]);
        this.actions.info([]);
        this.afterDoc(undefined);
        this.beforeDoc(undefined);
    }

    loadDocument() {
        this.resetForm();

        this.spinners.loadingDocument(true);

        viewHelpers.asyncValidationCompleted(this.testDocumentValidationGroup)
        .then(() => {
            return new getDocumentWithMetadataCommand(this.documentId(), this.db())
            .execute()
            .done((doc: document) => {
                if (doc) {
                    const docDto = doc.toDto(true);
                    const metaDto = docDto["@metadata"];
                    documentMetadata.filterMetadata(metaDto);
                    this.beforeDoc(docDto);
                }
            })
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404) {
                    messagePublisher.reportWarning("Document doesn't exist.");
                } else {
                    messagePublisher.reportError("Failed to load document.", xhr.responseText, xhr.statusText);
                }
            })
            .always(() => this.spinners.loadingDocument(false));
        });
    }

    onAutocompleteOptionSelected(item: string) {
        this.documentId(item);
        this.loadDocument();
    }

    runTest(): void {
        eventsCollector.default.reportEvent("patch", "test");

        viewHelpers.asyncValidationCompleted(this.validationGroup, () => {
            if (this.isValid(this.validationGroup)) {
                this.spinners.testing(true);
                this.resetForm();

                const query = this.query();

                new patchCommand(query, this.db(), { test: true, documentId: this.documentId() })
                    .execute()
                    .done((result: any) => {
                        const modifiedDocument = new document(result.ModifiedDocument).toDto(true);
                        const originalDocument = new document(result.OriginalDocument).toDto(true);
                        this.beforeDoc(originalDocument);
                        this.afterDoc(modifiedDocument);
                        const debug = result.Debug;
                        const actions = debug.Actions as Raven.Server.Documents.Patch.PatchDebugActions;
                        this.actions.loadDocument(actions.LoadDocument);
                        this.actions.putDocument(actions.PutDocument);
                        this.actions.deleteDocument(actions.DeleteDocument);
                        this.actions.info(debug.Info);

                        if (result.Status === "Patched") {
                            messagePublisher.reportSuccess("Test completed");
                        }
                    })
                    .always(() => this.spinners.testing(false));
            }
        });
    }

    previewDocument() {
        const spinner = this.spinners.preview;
        const documentId: KnockoutObservable<string> = this.documentId;
        const documentIdValidationGroup = this.validationGroup;
        const db = this.activeDatabase;

        documentPreviewer.preview(documentId, db, documentIdValidationGroup, spinner);
    }
}

class documentPreviewer {
    static preview(documentId: KnockoutObservable<string>, db: KnockoutObservable<database>, validationGroup: KnockoutValidationGroup, spinner?: KnockoutObservable<boolean>){
        if (spinner) {
            spinner(true);
        }
        viewHelpers.asyncValidationCompleted(validationGroup)
        .then(() => {
            if (viewHelpers.isValid(validationGroup)) {
                new getDocumentWithMetadataCommand(documentId(), db())
                    .execute()
                    .done((doc: document) => {
                        const docDto = doc.toDto(true);
                        const metaDto = docDto["@metadata"];
                        documentMetadata.filterMetadata(metaDto);
                        const text = JSON.stringify(docDto, null, 4);
                        app.showBootstrapDialog(new showDataDialog("Document: " + doc.getId(), text, "javascript"));
                    })
                    .always(() => spinner(false));
            } else {
                if (spinner) {
                    spinner(false);
                }
            }
        });
    }
}


export = patchTester;

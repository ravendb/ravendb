import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import patchDocument = require("models/database/patch/patchDocument");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import document = require("models/database/documents/document");
import database = require("models/resources/database");
import messagePublisher = require("common/messagePublisher");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import savePatchCommand = require('commands/database/patch/savePatchCommand');
import patchCommand = require("commands/database/patch/patchCommand");
import getPatchesCommand = require('commands/database/patch/getPatchesCommand');
import eventsCollector = require("common/eventsCollector");
import notificationCenter = require("common/notifications/notificationCenter");
import documentBasedColumnsProvider = require("widgets/virtualGrid/columns/providers/documentBasedColumnsProvider");
import popoverUtils = require("common/popoverUtils");
import deleteDocumentsCommand = require("commands/database/documents/deleteDocumentsCommand");
import documentPropertyProvider = require("common/helpers/database/documentPropertyProvider");
import getDocumentsPreviewCommand = require("commands/database/documents/getDocumentsPreviewCommand");
import defaultAceCompleter = require("common/defaultAceCompleter");
import queryCompleter = require("common/queryCompleter");
import patchSyntax = require("viewmodels/database/patch/patchSyntax");
import viewHelpers = require("common/helpers/view/viewHelpers");
import patchTester = require("viewmodels/database/patch/patchTester");
import validationHelpers = require("viewmodels/common/validationHelpers");

type fetcherType = (skip: number, take: number, previewCols: string[], fullCols: string[]) => JQueryPromise<pagedResult<document>>;

class patchList {

    previewItem = ko.observable<patchDocument>();

    private allPatches = ko.observableArray<patchDocument>([]);

    private readonly useHandler: (patch: patchDocument) => void;
    private readonly removeHandler: (patch: patchDocument) => void;

    hasAnySavedPatch = ko.pureComputed(() => this.allPatches().length > 0);

    previewCode = ko.pureComputed(() => {
        const item = this.previewItem();
        if (!item) {
            return "";
        }

        return item.query();
    });

    constructor(useHandler: (patch: patchDocument) => void, removeHandler: (patch: patchDocument) => void) {
        _.bindAll(this, ...["previewPatch", "removePatch", "usePatch"] as Array<keyof this>);
        this.useHandler = useHandler;
        this.removeHandler = removeHandler;
    }

    filteredPatches = ko.pureComputed(() => {
        let text = this.filters.searchText();

        if (!text) {
            return this.allPatches();
        }

        text = text.toLowerCase();

        return this.allPatches().filter(x => x.name().toLowerCase().includes(text));
    });

    filters = {
        searchText: ko.observable<string>()
    };

    previewPatch(item: patchDocument) {
        this.previewItem(item);
    }

    usePatch() {
        this.useHandler(this.previewItem());
    }

    removePatch(item: patchDocument) {
        if (this.previewItem() === item) {
            this.previewItem(null);
        }
        this.removeHandler(item);
    }

    loadAll(db: database) {
        return new getPatchesCommand(db)
            .execute()
            .done((patches: patchDocument[]) => {
                this.allPatches(patches);

                if (this.filteredPatches().length) {
                    this.previewItem(this.filteredPatches()[0]);
                }
            });
    }
}

class patch extends viewModelBase {

    static readonly $body = $("body");
    static readonly ContainerSelector = "#patchContainer";

    inSaveMode = ko.observable<boolean>();
    patchSaveName = ko.observable<string>();

    spinners = {
        save: ko.observable<boolean>(false),
    };

    jsCompleter = defaultAceCompleter.completer();
    private indexes = ko.observableArray<Raven.Client.Documents.Operations.IndexInformation>();
    queryCompleter: queryCompleter;
    
    private documentsProvider: documentBasedColumnsProvider;
    private fullDocumentsProvider: documentPropertyProvider;

    patchDocument = ko.observable<patchDocument>(patchDocument.empty());

    runPatchValidationGroup: KnockoutValidationGroup;
    savePatchValidationGroup: KnockoutValidationGroup;

    savedPatches = new patchList(item => this.usePatch(item), item => this.removePatch(item));

    test = new patchTester(this.patchDocument().query, this.activeDatabase);

    private hideSavePatchHandler = (e: Event) => {
        if ($(e.target).closest(".patch-save").length === 0) {
            this.inSaveMode(false);
        }
    };

    constructor() {
        super();
        aceEditorBindingHandler.install();

        this.queryCompleter = queryCompleter.remoteCompleter(this.activeDatabase, this.indexes, "Update");

        this.initValidation();

        this.initObservables();
    }

    private initValidation() {
        const doc = this.patchDocument();

        doc.query.extend({
            required: true,
            aceValidation: true
        });
        
        this.patchSaveName.extend({
            required: true
        });

        this.runPatchValidationGroup = ko.validatedObservable({
            query: doc.query,
        });

        this.savePatchValidationGroup = ko.validatedObservable({
            patchSaveName: this.patchSaveName
        });
    }

    private initObservables() {
        this.inSaveMode.subscribe(enabled => {
            const $input = $(".patch-save .form-control");
            if (enabled) {
                $input.show();
                window.addEventListener("click", this.hideSavePatchHandler, true);
            } else {
                this.savePatchValidationGroup.errors.showAllMessages(false);
                window.removeEventListener("click", this.hideSavePatchHandler, true);
                setTimeout(() => $input.hide(), 200);
            }
        });
    }

    activate(recentPatchHash?: string) {
        super.activate(recentPatchHash);
        this.updateHelpLink("QGGJR5");

        this.fullDocumentsProvider = new documentPropertyProvider(this.activeDatabase());

        return $.when<any>(this.fetchAllIndexes(this.activeDatabase()), this.savedPatches.loadAll(this.activeDatabase()));
    }

    attached() {
        super.attached();

        this.createKeyboardShortcut("ctrl+enter", () => {
            if (this.test.testMode()) {
                this.test.runTest();
            } else {
                this.runPatch();
            }
        }, patch.ContainerSelector);
        
        const jsCode = Prism.highlight("this.NewProperty = this.OldProperty + myParameter;\r\n" +
            "delete this.UnwantedProperty;\r\n" +
            "this.Comments.RemoveWhere(function(comment){\r\n" +
            "  return comment.Spam;\r\n" +
            "});",
            (Prism.languages as any).javascript);

        popoverUtils.longWithHover($(".patch-title small"),
            {
                content: `<p>Patch Scripts are written in JavaScript. <br />Examples: <pre>${jsCode}</pre></p>`
                + `<p>You can use following functions in your patch script:</p>`
                + `<ul>`
                + `<li><code>PutDocument(documentId, document)</code> - puts document with given name and data</li>`
                + `<li><code>LoadDocument(documentIdToLoad)</code> - loads document by id`
                + `<li><code>output(message)</code> - allows to output debug info when testing patches</li>`
                + `</ul>`
            });
    }

    compositionComplete() {
        super.compositionComplete();
        /* TODO

        const grid = this.gridController();
        this.documentsProvider = new documentBasedColumnsProvider(this.activeDatabase(), grid, {
            showRowSelectionCheckbox: false,
            showSelectAllCheckbox: false,
            createHyperlinks: false,
            customInlinePreview: (doc: document) => this.showPreview(doc),
            enableInlinePreview: true
        });

        const fakeFetcher: fetcherType = () => $.Deferred<pagedResult<document>>().resolve({
            items: [],
            totalResultCount: -1
        });

        grid.headerVisible(true);

        const allColumnsProvider = (results: pagedResultWithAvailableColumns<document>) => {
            const selectedItem = this.patchDocument().selectedItem();
            if (!selectedItem || this.patchDocument().patchOnOption() === "Document" || !this.fetcher()) {
                return [];
            }

            switch (this.patchDocument().patchOnOption()) {
                case "Document":
                    return [];
                case "Query":
                    return documentBasedColumnsProvider.extractUniquePropertyNames(results);
            }
        };

        this.columnsSelector.init(grid, (s, t, previewCols, fullCols) => this.fetcher() ? this.fetcher()(s, t, previewCols, fullCols) : fakeFetcher(s, t, [], []),
            (w, r) => this.documentsProvider.findColumns(w, r),
            allColumnsProvider);

        this.columnPreview.install(".patch-grid", ".tooltip", (doc: document, column: virtualColumn, e: JQueryEventObject, onValue: (context: any) => void) => {
            if (column instanceof textColumn) {
                this.fullDocumentsProvider.resolvePropertyValue(doc, column, (v: any) => {
                    if (!_.isUndefined(v)) {
                        const json = JSON.stringify(v, null, 4);
                        const html = Prism.highlight(json, (Prism.languages as any).javascript);
                        onValue(html);    
                    }
                }, error => {
                    const html = Prism.highlight("Unable to generate column preview: " + error.toString(), (Prism.languages as any).javascript);
                    onValue(html);
                });
            }
        });

        this.fetcher.subscribe(() => grid.reset());
        */
    }

    private showPreview(doc: document) {
        // if document doesn't have all properties fetch them and then display preview

        const meta = doc.__metadata as any;
        const hasCollapsedFields = meta[getDocumentsPreviewCommand.ObjectStubsKey] || meta[getDocumentsPreviewCommand.ArrayStubsKey] || meta[getDocumentsPreviewCommand.TrimmedValueKey];

        if (hasCollapsedFields) {
            new getDocumentWithMetadataCommand(doc.getId(), this.activeDatabase(), true)
                .execute()
                .done((fullDocument: document) => {
                    documentBasedColumnsProvider.showPreview(fullDocument);
                });
        } else {
            // document has all properties - fallback to default method
            documentBasedColumnsProvider.showPreview(doc);
        }
    }

    usePatch(item: patchDocument) {
        const patchDoc = this.patchDocument();
        patchDoc.copyFrom(item);
    }

    removePatch(item: patchDocument) {
        this.confirmationMessage("Patch", `Are you sure you want to delete patch '${item.name()}'?`, ["Cancel", "Delete"])
            .done(result => {
                if (result.can) {
                    new deleteDocumentsCommand([item.getId()], this.activeDatabase())
                        .execute()
                        .done(() => {
                            messagePublisher.reportSuccess("Deleted patch " + item.name());
                            this.savedPatches.loadAll(this.activeDatabase());
                        })
                        .fail(response => messagePublisher.reportError("Failed to delete " + item.name(), response.responseText, response.statusText));
                }
            });
    }

    runPatch() {
        if (this.isValid(this.runPatchValidationGroup)) {
            const patchDoc = this.patchDocument();
            this.patchOnQuery();
        }
    }

    savePatch() {
        if (this.inSaveMode()) {
            eventsCollector.default.reportEvent("patch", "save");

            if (this.isValid(this.savePatchValidationGroup)) {
                this.spinners.save(true);
                new savePatchCommand(this.patchSaveName(), this.patchDocument(), this.activeDatabase())
                    .execute()
                    .always(() => this.spinners.save(false))
                    .done(() => {
                        this.inSaveMode(false);
                        this.patchSaveName("");
                        this.savePatchValidationGroup.errors.showAllMessages(false);
                        this.savedPatches.loadAll(this.activeDatabase());
                    });
            }
        } else {
            if (this.isValid(this.runPatchValidationGroup)) {
                this.inSaveMode(true);    
            }
        }
    }

    private patchOnQuery() {
        eventsCollector.default.reportEvent("patch", "run");
        const message = `Are you sure you want to apply this patch to matching documents?`;

        this.confirmationMessage("Patch", message, ["Cancel", "Patch all"])
            .done(result => {
                if (result.can) {
                    new patchCommand(this.patchDocument().query(), this.activeDatabase())
                        .execute()
                        .done((operation: operationIdDto) => {
                            notificationCenter.instance.openDetailsForOperationById(this.activeDatabase(), operation.OperationId);
                        });
                }
            });
    }

    private fetchAllIndexes(db: database): JQueryPromise<any> {
        return new getDatabaseStatsCommand(db)
            .execute()
            .done((results: Raven.Client.Documents.Operations.DatabaseStatistics) => {
                this.indexes(results.Indexes);
            });
    }

    enterTestMode() {
        this.test.enterTestMode('');
    }

    syntaxHelp() {
        const viewModel = new patchSyntax();
        app.showBootstrapDialog(viewModel);
    }
}

export = patch;

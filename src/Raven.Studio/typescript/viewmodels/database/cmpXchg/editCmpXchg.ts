import app = require("durandal/app");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");
import messagePublisher = require("common/messagePublisher");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import copyToClipboard = require("common/copyToClipboard");
import documentHelpers = require("common/helpers/database/documentHelpers");
import getCompareExchangeItemCommand = require("commands/database/cmpXchg/getCompareExchangeItemCommand");
import saveCompareExchangeItemCommand = require("commands/database/cmpXchg/saveCompareExchangeItemCommand");
import deleteCompareExchangeConfirm = require("viewmodels/database/documents/deleteCompareExchangeConfirm");
import deleteCompareExchangeProgress = require("viewmodels/database/documents/deleteCompareExchangeProgress");
import editorWarningsConfirm = require("viewmodels/database/documents/editorWarningsConfirm");
import viewModelBase = require("viewmodels/viewModelBase");
import eventsCollector = require("common/eventsCollector");
import popoverUtils = require("common/popoverUtils");

type contentType = "Value" | "Metadata";

class editorInfo {

    type: contentType;
    selector: string;
    
    contentEditor: AceAjax.Editor;
    
    content = ko.observable<any>();
    contentText = ko.observable<string>("");
    
    isContentCollapsed = ko.observable<boolean>(false);
    isNewLineFriendlyMode = ko.observable<boolean>(false);
    
    constructor(type: contentType, selector: string, content: any) {
        this.initializeObservables();
        this.initValidation();

        this.type = type;
        this.selector = selector;
        this.content(content);
    }

    private initializeObservables() {
        this.content.subscribe(content => {
            if (!_.isUndefined(content)) {
                const text = this.stringify(content);
                this.contentText(text);
            }
        });

        this.isNewLineFriendlyMode.subscribe(val => {
            this.updateNewlineLayout(val);
        });
    }

    private initValidation() {
        this.contentText.extend({
            aceValidation: true
        });
    }
    
    initEditor() {
        this.contentEditor = aceEditorBindingHandler.getEditorBySelection($(this.selector)); 
    }

    private stringify(obj: any) {
        const prettifySpacing = 4;
        return JSON.stringify(obj, null, prettifySpacing);
    }
    
    private updateNewlineLayout(unescapeNewline: boolean) {
        if (unescapeNewline) {
            this.contentText(documentHelpers.unescapeNewlinesAndTabsInTextFields(this.contentText()));
            this.contentEditor.getSession().setMode('ace/mode/json_newline_friendly');
        } else {
            this.contentText(documentHelpers.escapeNewlinesAndTabsInTextFields(this.contentText()));
            this.contentEditor.getSession().setMode('ace/mode/raven_document');
            this.formatContent();
        }
    }

    private formatContent() {
        eventsCollector.default.reportEvent("cmpXchg", "format");
        try {
            const editorText = this.contentEditor.getSession().getValue();
            const tempValue = JSON.parse(editorText);
            const formatted = this.stringify(tempValue);
            this.contentText(formatted);
        } catch (e) {
            messagePublisher.reportError("Could not format json", undefined, undefined, false);
        }
    }

    focusOnEditor() {
        this.contentEditor.focus();
    }

    toClipboard() {
        copyToClipboard.copy(this.contentText(), `${this.type} has been copied to clipboard`);
    }

    toggleNewlineMode() {
        this.isNewLineFriendlyMode.toggle();
    }

    toggleCollapseContent() {
        if (this.isContentCollapsed()) {
            this.unfoldContent();
        } else {
            this.collapseContent();
        }
    }

    private collapseContent() {
        this.foldAll();
        this.isContentCollapsed(true);
    }

    private unfoldContent() {
        this.contentEditor.getSession().unfold(null, true);
        this.isContentCollapsed(false);
    }

    private foldAll() {
        const AceRange = ace.require("ace/range").Range;
        this.contentEditor.getSession().foldAll();
        const folds = <any[]> this.contentEditor.getSession().getFoldsInRange(new AceRange(0, 0, this.contentEditor.getSession().getLength(), 0));
        folds.map(f => this.contentEditor.getSession().expandFold(f));
    }

    tryRestoreSelection() {
        const currentSelection = this.contentEditor.getSelectionRange();
        this.updateNewlineLayout(this.isNewLineFriendlyMode());
        this.contentEditor.selection.setRange(currentSelection, false);
    }
    
    toDto() {
        let dto: any;
        
        try {
            if (this.isNewLineFriendlyMode()) {
                dto = JSON.parse(documentHelpers.escapeNewlinesAndTabsInTextFields(this.contentText()));
            } else {
                dto = JSON.parse(this.contentText());
            }
        } catch (e) {
            this.focusOnEditor();
            
            if (dto == undefined)
                throw(`${this.type} content is not a legal JSON expression`);
        }
        
        return dto;
    }
}

class editCmpXchg extends viewModelBase {

    cmpXchUrl = ko.pureComputed(() => this.appUrls.cmpXchg());

    key = ko.observable<string>("");
    loadedIndex = ko.observable<number>(0);
    
    valueEditor: KnockoutObservable<editorInfo>;
    metadataEditor: KnockoutObservable<editorInfo>;

    isSaveEnabled: KnockoutComputed<boolean>;
    isCreatingNewItem = ko.observable(false);

    spinners = {
        delete: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false)
    };

    displayExternalChange = ko.observable<boolean>(false);
    globalValidationGroup: KnockoutValidationGroup;
    
    static readonly valueEditorSelector = "#valueEditor";
    static readonly metadataEditorSelector = "#metadataEditor";
    
    hasMetadata = ko.observable<boolean>(false);

    constructor() {
        super();
        this.bindToCurrentInstance("removeMetadata");
        aceEditorBindingHandler.install();
    }

    canActivate(args: any) {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                if (args && args.key) {
                    return this.activateByKey(args.key);
                } else {
                    this.isCreatingNewItem(true);
                    this.createEditors();
                    return $.Deferred().resolve({ can: true });
                }
            });
    }
    
    private activateByKey(key: string) {
        const canActivateResult = $.Deferred<canActivateResultDto>();

        this.loadValue(key)
            .done(() => canActivateResult.resolve({ can: true }))
            .fail(() => canActivateResult.resolve({ redirect: appUrl.forCmpXchg(this.activeDatabase()) }));

        return canActivateResult;
    }

    activate(navigationArgs: { database: string, key: string }) {
        super.activate(navigationArgs);

        this.initializeObservables();
        this.initValidation();
    }

    private initializeObservables(): void {
        this.dirtyFlag = new ko.DirtyFlag([this.key,
                                           this.hasMetadata,
                                           this.valueEditor().contentText,
                                           this.metadataEditor().contentText],
            false, jsonUtil.newLineNormalizingHashFunction);

        this.isSaveEnabled = ko.pureComputed(() => {
            const isSaving = this.spinners.save();
            const isDirty = this.dirtyFlag().isDirty();

            return !isSaving && isDirty;
        });
    }

    private initValidation() {
        const rg1 = /^[^\\]*$/; // forbidden character - backslash

        this.key.extend({
            required: true,
            validation: [
                {
                    validator: (val: string) => rg1.test(val),
                    message: "Key Cannot contain a backslash"
                }]
        });

        this.valueEditor().contentText.extend({
            required: true
        });
        
        this.metadataEditor().contentText.extend({
            required: {
                onlyIf: () => this.hasMetadata()
            }
        });

        this.globalValidationGroup = ko.validatedObservable({
            key: this.key,
            valueText: this.valueEditor().contentText,
            metadataText: this.metadataEditor().contentText
        });
    }

    attached() {
        super.attached();

        popoverUtils.longWithHover($(".key-info"),
            {
                content: `<small>A unique identifier that is <strong>reserved accross the cluster</strong></small>`,
                html: true,
                placement: "bottom"
            });
        
        popoverUtils.longWithHover($(".value-info"),
            {
                content: `<small>A Value that is associated with the Key</small>`,
                html: true,
                placement: "bottom"
            });
        
        popoverUtils.longWithHover($(".metadata-info"),
            {
                content: `<ul class="margin-top margin-top-xs margin-right">
                              <li><small>The Metadata is associated with the Key, similar to a document's metadata.</small></li>
                              <li>
                                  <small>Can be used to set the <strong>Compare Exchange item expiration.</strong></small><br>
                                  <small>Set the <i>@expires</i> field to schedule expiration.</small><br>
                                  <small>e.g. <code>{ "@expires": "2021-02-26T13:09:37.4040256Z" }</code></small>
                              </li>
                          </ul>`,
                html: true,
                placement: "bottom"
            });
    }

    compositionComplete() {
        super.compositionComplete();

        // preload json newline friendly mode to avoid issues with document save
        (ace as any).config.loadModule("ace/mode/json_newline_friendly");

        this.valueEditor().initEditor();
        this.metadataEditor().initEditor();

        this.valueEditor().focusOnEditor();
    }

    editNewItem(): void {
        this.isCreatingNewItem(true);
        this.createEditors();
    }

    saveCompareExchangeItem() {
        if (this.isValid(this.globalValidationGroup)) {
            $.when<boolean>(this.maybeConfirmWarnings())
                .then((canSave: boolean) => {
                    if (canSave) {
                        eventsCollector.default.reportEvent("cmpXchg", "save");
                        this.saveInternal();
                    }
                });
        }
    }

    private maybeConfirmWarnings(): JQueryPromise<boolean> | boolean {
        const valueWarnings = this.valueEditor().contentEditor.getSession().getAnnotations()
            .filter((x: AceAjax.Annotation) => x.type === "warning");
        
        const metadataWarnings = this.metadataEditor().contentEditor.getSession().getAnnotations()
            .filter((x: AceAjax.Annotation) => x.type === "warning");

        if (valueWarnings.length || metadataWarnings.length) {
            const viewModel = new editorWarningsConfirm("Compare Exchange",
                [{ source: "Value", warnings: valueWarnings }, { source: "Metadata", warnings: metadataWarnings }],
                (warning, source)  => {
                    const editor = source === "Value" ? this.valueEditor() : this.metadataEditor();
                    // gotoLine is not zero based so we add 1
                    editor.contentEditor.gotoLine(warning.row + 1, warning.column, true);
                    editor.contentEditor.focus();
                });
            
            return app.showBootstrapDialog(viewModel);
        }
        
        return true;
    }

    private saveInternal() {
        let valueDto: any;
        let metadataDto: any;

        try {
            valueDto = this.valueEditor().toDto();
            if (this.hasMetadata()) {
                metadataDto = this.metadataEditor().toDto();
            }
        } catch (e) {
            if (e.message) {
                messagePublisher.reportError(e.message, undefined, undefined, false);
                return;
            }
        }

        this.spinners.save(true);

        new saveCompareExchangeItemCommand(this.activeDatabase(), this.key(), this.loadedIndex(), valueDto, metadataDto)
            .execute()
            .done(saveResult => this.onValueSaved(saveResult))
            .fail(() => this.spinners.save(false));
    }

    private loadValue(key: string): JQueryPromise<any> {
        this.isBusy(true);

        const db = this.activeDatabase();
        const loadTask = $.Deferred<any>();

        new getCompareExchangeItemCommand(db, key)
            .execute()
            .done(cmpXchngItem => {
                this.key(cmpXchngItem.Key);
                this.loadedIndex(cmpXchngItem.Index);
                this.createEditors(cmpXchngItem.Value.Object, cmpXchngItem.Value["@metadata"]);
                loadTask.resolve(cmpXchngItem.Value.Object);
            })
            .fail(() => loadTask.reject())
            .always(() => {
                this.dirtyFlag().reset();
                this.isBusy(false); 
            });

        return loadTask;
    }

    private onValueSaved(saveResult: Raven.Client.Documents.Operations.CompareExchange.CompareExchangeResult<any>) {
        if (saveResult.Successful) {
            this.loadedIndex(saveResult.Index);

            const savedValueDto = saveResult.Value.Object;
            this.valueEditor().content(savedValueDto);

            const savedMetadataDto = saveResult.Value["@metadata"];
            this.metadataEditor().content(savedMetadataDto);

            this.valueEditor().tryRestoreSelection();
            if (this.hasMetadata()) {
                this.metadataEditor().tryRestoreSelection();
            }

            this.spinners.save(false);
            this.updateUrl(this.key());
            this.dirtyFlag().reset();
            this.isCreatingNewItem(false);
            
            messagePublisher.reportSuccess(`Compare exchange item with key: ${this.key()} was saved successfully`);
            router.navigate(appUrl.forCmpXchg(this.activeDatabase()));
        } else {
            this.displayExternalChange(true);
            this.spinners.save(false);
            messagePublisher.reportError(`Failed to save compare exchange item. Save was called with index: ${this.loadedIndex()},
                                          but key: ${this.key()} has index: ${saveResult.Index}.`);
        }
    }
    
    private createEditors(valueObj: any = undefined, metadataObj: any = undefined) {
        this.valueEditor = ko.observable<editorInfo>(new editorInfo("Value", editCmpXchg.valueEditorSelector, valueObj));
        this.metadataEditor = ko.observable<editorInfo>(new editorInfo("Metadata", editCmpXchg.metadataEditorSelector, metadataObj));

        this.hasMetadata(!!metadataObj);
    }

    deleteItem() {
        eventsCollector.default.reportEvent("cmpXchg", "delete");
        
        const deleteDialog = new deleteCompareExchangeConfirm([this.key()]);

        app.showBootstrapDialog(deleteDialog)
            .done((deleting: boolean) => {
                if (deleting) {
                    this.spinners.delete(true);
                    
                    const deleteProgress = new deleteCompareExchangeProgress([{ Key: this.key(), Index: this.loadedIndex() }], this.activeDatabase());
                 
                    deleteProgress.start()
                        .done(() => {
                            this.dirtyFlag().reset();
                            router.navigate(appUrl.forCmpXchg(this.activeDatabase()));
                        })
                        .fail(() => this.displayExternalChange(true))
                        .always(() => this.spinners.delete(false));
                }
            });
    }
    
    refresh() {
        eventsCollector.default.reportEvent("cmpXchg", "refresh");
        this.canContinueIfNotDirty("Refresh", "Reload compare exchange item?")
            .done(() => {
                const key = this.key();
                this.key("");
                this.loadValue(key);
                this.displayExternalChange(false);
            });
    }

    updateUrl(valueKey: string) {
        const editUrl = appUrl.forEditCmpXchg(valueKey, this.activeDatabase());
        router.navigate(editUrl, false);
    }

    cloneCmpXch() {
        eventsCollector.default.reportEvent("cmpXchg", "clone");
        this.isCreatingNewItem(true);
        this.key("");
        this.globalValidationGroup.errors.showAllMessages(false);
    }

    addMetadata() {
        eventsCollector.default.reportEvent("cmpXchg", "add-metadata");
        if (!this.hasMetadata()) {
            this.hasMetadata(true);
            this.metadataEditor().contentText(null);
        }
    }

    removeMetadata() {
        eventsCollector.default.reportEvent("cmpXchg", "remove-metadata");
        this.hasMetadata(false);
        this.metadataEditor().contentText(null);
    }
}

export = editCmpXchg;

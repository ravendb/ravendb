import viewModelBase = require("viewmodels/viewModelBase");
import transformer = require("models/database/index/transformer");
import saveTransformerCommand = require("commands/database/transformers/saveTransformerCommand");
import getSingleTransformerCommand = require("commands/database/transformers/getSingleTransformerCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import deleteTransformerConfirm = require("viewmodels/database/transformers/deleteTransformerConfirm");
import dialog = require("plugins/dialog");
import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");
import router = require("plugins/router");
import messagePublisher = require("common/messagePublisher");
import formatIndexCommand = require("commands/database/index/formatIndexCommand");
import eventsCollector = require("common/eventsCollector");
import renameTransformerCommand = require("commands/database/transformers/renameTransformerCommand");
import getIndexNamesCommand = require("commands/database/index/getIndexNamesCommand");
import getTransformersCommand = require("commands/database/transformers/getTransformersCommand");
import popoverUtils = require("common/popoverUtils");

class editTransformer extends viewModelBase {

    static readonly containerSelector = ".edit-transformer";

    loadedTransformerName = ko.observable<string>();
    editedTransformer = ko.observable<transformer>(transformer.empty());
    
    editorHasFocus = ko.observable<boolean>(false);

    isSaveEnabled: KnockoutComputed<boolean>;
    isEditingExistingTransformer: KnockoutComputed<boolean>;
    isSaving = ko.observable<boolean>(false);
    renameMode = ko.observable<boolean>(false);
    renameInProgress = ko.observable<boolean>(false);
    nameChanged: KnockoutComputed<boolean>;
    canEditTransformerName: KnockoutComputed<boolean>;
    transformerNameHasFocus = ko.observable<boolean>(false);

    private indexesNames = ko.observableArray<string>();
    private transformersNames = ko.observableArray<string>();

    transformersUrl = ko.pureComputed(() => this.appUrls.transformers());

    globalValidationGroup: KnockoutValidationGroup;
    renameValidationGroup: KnockoutValidationGroup;
    
    constructor() {
        super();

        aceEditorBindingHandler.install();

        this.initializeObservables();
    }

    canActivate(unescapedTransformerToEditName: string): JQueryPromise<canActivateResultDto> {
        const transformerToEditName = unescapedTransformerToEditName ? decodeURIComponent(unescapedTransformerToEditName) : undefined;
        super.canActivate(transformerToEditName);

        const db = this.activeDatabase();

        if (transformerToEditName) {
            const canActivateResult = $.Deferred<canActivateResultDto>();
            this.editExistingTransformer(transformerToEditName)
                .done(() => canActivateResult.resolve({ can: true }))
                .fail(() => {
                    messagePublisher.reportError("Could not find " + transformerToEditName + " transformer");
                    canActivateResult.resolve({ redirect: appUrl.forTransformers(db) });
                });

            return canActivateResult;
        } else {
            return $.Deferred().resolve({ can: true });
        }
    }

    activate(transformerToEditName: string) {
        super.activate(transformerToEditName);
        this.updateHelpLink('S467UO');

        this.initValidation();
        this.initializeDirtyFlag();
        this.fetchIndexes();
        this.fetchTransformers();
    }

    attached() {
        super.attached();
        this.addTransformerHelpPopover();
        this.createKeyboardShortcut("alt+c", () => this.editorHasFocus(true), editTransformer.containerSelector);
        this.createKeyboardShortcut("alt+shift+del", () => this.deleteTransformer(), editTransformer.containerSelector);
    }

    cloneTransformer() {
        this.loadedTransformerName(null);

        this.editedTransformer().cleanForClone();
        this.dirtyFlag().reset();
        this.globalValidationGroup.errors.showAllMessages(false);
    }

    private initValidation() {
        const rg1 = /^[^\\]*$/; // forbidden character - backslash
        this.editedTransformer().name.extend({
            required: true,
            validation: [
                {
                    validator: (val: string) => rg1.test(val),
                    message: "Can't use backslash in transformer name"
                },
                {
                    validator: (val: string) => {
                        return val === this.loadedTransformerName() || !_.includes(this.transformersNames(), val);
                    },
                    message: "Already being used by an existing transformer."
                },
                {
                    validator: (val: string) => {
                        return !_.includes(this.indexesNames(), val);
                    },
                    message: "Already being used by an existing index."
                }]
        });

        this.editedTransformer().transformResults.extend({
            required: true
        });

        this.globalValidationGroup = ko.validatedObservable({
            userTransformerName: this.editedTransformer().name,
            userTransformerContent: this.editedTransformer().transformResults
        });

        this.renameValidationGroup = ko.validatedObservable({
            userTransformerName: this.editedTransformer().name
        });
    }

    private initializeDirtyFlag() {
        this.dirtyFlag = new ko.DirtyFlag([this.editedTransformer().name, this.editedTransformer().transformResults], false, jsonUtil.newLineNormalizingHashFunction);
    }

    private initializeObservables() {
        this.isSaveEnabled = ko.pureComputed(() => {
            const editIndex = this.isEditingExistingTransformer();
            const isDirty = this.dirtyFlag().isDirty();

            return !editIndex || isDirty;
        });

        this.isEditingExistingTransformer = ko.pureComputed(() => !!this.loadedTransformerName());

        this.canEditTransformerName = ko.pureComputed(() => {
            const renameMode = this.renameMode();
            const editMode = this.isEditingExistingTransformer();
            return !editMode || renameMode;
        });

        this.nameChanged = ko.pureComputed(() => {
            const newName = this.editedTransformer().name();
            const oldName = this.loadedTransformerName();

            return newName !== oldName;
        });
    }

    private addTransformerHelpPopover() {
        $("#transform-title small").popover({
            html: true,
            container: "body",
            template: popoverUtils.longPopoverTemplate,
            trigger: "hover",
            content: 'The Transform function allows you to change the shape<br /> of individual result documents before the server returns them. <br />It uses C# LINQ query syntax. <br />' +
                'Example: <pre><span class="token keyword">from</span> result <span class="token keyword">in</span> results <br/> <span class="token keyword">let</span> category = LoadDocument(result.Category) <br/> <span class="token keyword">select new</span> { <br/>    result.Name, <br/>    result.PricePerUnit, <br/>    Category = category.Name, <br/>    CategoryDescription = category.Description <br/>}</pre>',
        });
    }

    private editExistingTransformer(transformerName: string): JQueryPromise<Raven.Client.Documents.Transformers.TransformerDefinition> {
        this.loadedTransformerName(transformerName);
        return this.fetchTransformerToEdit(transformerName)
            .done((trans: Raven.Client.Documents.Transformers.TransformerDefinition) => this.editedTransformer().updateUsing(trans)); 
    }

    private fetchTransformerToEdit(transformerName: string): JQueryPromise<Raven.Client.Documents.Transformers.TransformerDefinition> {
        return new getSingleTransformerCommand(transformerName, this.activeDatabase()).execute();
    }

    saveTransformer() {
        if (this.isValid(this.globalValidationGroup)) {
            this.isSaving(true);
            eventsCollector.default.reportEvent("transformer", "save");

            const transformerName = this.editedTransformer().name().trim();

            this.editedTransformer().name(transformerName);
            new saveTransformerCommand(this.editedTransformer(), this.activeDatabase())
                .execute()
                .done(() => {
                    this.dirtyFlag().reset();

                    this.loadedTransformerName(transformerName);
                    this.updateUrl(this.editedTransformer().name());
                })
                .always(() => this.isSaving(false));                    
        }
    }

    private fetchIndexes() {
        const db = this.activeDatabase();
        new getIndexNamesCommand(db)
            .execute()
            .done((indexesNames) => {
                this.indexesNames(indexesNames);
            });
    }

    private fetchTransformers() {
        const db = this.activeDatabase();
        return new getTransformersCommand(db)
            .execute()
            .done((transformers: Raven.Client.Documents.Transformers.TransformerDefinition[]) => {
                this.transformersNames(transformers.map(t => t.Name));
            });
    }

    updateUrl(transformerName: string) {
        router.navigate(appUrl.forEditTransformer(transformerName, this.activeDatabase()), false);
    }

    formatTransformer() {
        eventsCollector.default.reportEvent("transformer", "format");

        const editedTransformer: transformer = this.editedTransformer();

        //TODO: we will have new endpoint for doing this - it will consume single map/reduce instead of array
        new formatIndexCommand(this.activeDatabase(), [editedTransformer.transformResults()])
            .execute()
            .done((result: string[]) => {
                const formatedTransformer = result[0];
                if (formatedTransformer.indexOf("Could not format:") == -1) {
                    editedTransformer.transformResults(formatedTransformer);
                } else {
                    messagePublisher.reportError("Failed to format transformer!", formatedTransformer);
                }
            });
    }

    deleteTransformer() {
        eventsCollector.default.reportEvent("transformer", "delete");

        const transformer = this.editedTransformer();

        if (transformer) {
            const db = this.activeDatabase();
            const deleteViewmodel = new deleteTransformerConfirm([transformer.name()], db);
            deleteViewmodel.deleteTask.done(() => {
                router.navigate(appUrl.forTransformers(db));
            });
            dialog.show(deleteViewmodel);
        }
    }

    enterRenameMode() {
        this.renameMode(true);
        this.transformerNameHasFocus(true);
    }

    renameTransformer() {
        if (this.isValid(this.renameValidationGroup)) {
            const newName = this.editedTransformer().name();
            const oldName = this.loadedTransformerName();

            this.renameInProgress(true);

            new renameTransformerCommand(oldName, newName, this.activeDatabase())
                .execute()
                .always(() => this.renameInProgress(false))
                .done(() => {
                    this.dirtyFlag().reset();

                    this.loadedTransformerName(newName);
                    this.updateUrl(this.editedTransformer().name());
                    this.renameMode(false);
                });
        }
    }

    cancelRename() {
        this.renameMode(false);
        this.editedTransformer().name(this.loadedTransformerName());
        this.renameValidationGroup.errors.showAllMessages(false);
    }
  
}

export = editTransformer;

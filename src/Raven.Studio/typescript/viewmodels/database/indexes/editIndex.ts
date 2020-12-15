import app = require("durandal/app");
import router = require("plugins/router");
import viewModelBase = require("viewmodels/viewModelBase");
import dialog = require("plugins/dialog");
import indexDefinition = require("models/database/index/indexDefinition");
import autoIndexDefinition = require("models/database/index/autoIndexDefinition");
import getIndexDefinitionCommand = require("commands/database/index/getIndexDefinitionCommand");
import getCSharpIndexDefinitionCommand = require("commands/database/index/getCSharpIndexDefinitionCommand");
import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import messagePublisher = require("common/messagePublisher");
import autoCompleteBindingHandler = require("common/bindingHelpers/autoCompleteBindingHandler");
import indexAceAutoCompleteProvider = require("models/database/index/indexAceAutoCompleteProvider");
import deleteIndexesConfirm = require("viewmodels/database/indexes/deleteIndexesConfirm");
import saveIndexDefinitionCommand = require("commands/database/index/saveIndexDefinitionCommand");
import detectIndexTypeCommand = require("commands/database/index/detectIndexTypeCommand");
import indexFieldOptions = require("models/database/index/indexFieldOptions");
import getIndexFieldsFromMapCommand = require("commands/database/index/getIndexFieldsFromMapCommand");
import configurationItem = require("models/database/index/configurationItem");
import getIndexNamesCommand = require("commands/database/index/getIndexNamesCommand");
import eventsCollector = require("common/eventsCollector");
import showDataDialog = require("viewmodels/common/showDataDialog");
import formatIndexCommand = require("commands/database/index/formatIndexCommand");
import additionalSource = require("models/database/index/additionalSource");
import additionalAssembly = require("models/database/index/additionalAssemblyModel");
import index = require("models/database/index/index");
import viewHelpers = require("common/helpers/view/viewHelpers");
import mapIndexSyntax = require("viewmodels/database/indexes/mapIndexSyntax");
import fileDownloader = require("common/fileDownloader");
import mapReduceIndexSyntax = require("viewmodels/database/indexes/mapReduceIndexSyntax");
import additionalSourceSyntax = require("viewmodels/database/indexes/additionalSourceSyntax");
import additionalAssemblySyntax = require("viewmodels/database/indexes/additionalAssemblySyntax");
import fileImporter = require("common/fileImporter");
import popoverUtils = require("common/popoverUtils");

class editIndex extends viewModelBase {

    static readonly $body = $("body");
    static readonly ContainerSelector = ".edit-index";

    isEditingExistingIndex = ko.observable<boolean>(false);
    editedIndex = ko.observable<indexDefinition>();
    isAutoIndex = ko.observable<boolean>(false);

    originalIndexName: string;
    isSaveEnabled: KnockoutComputed<boolean>;
    saveInProgress = ko.observable<boolean>(false);
    indexAutoCompleter: indexAceAutoCompleteProvider;
    nameChanged: KnockoutComputed<boolean>;
    canEditIndexName: KnockoutComputed<boolean>;

    fieldNames = ko.observableArray<string>([]);
    indexNameHasFocus = ko.observable<boolean>(false);

    private indexesNames = ko.observableArray<string>();

    queryUrl = ko.observable<string>();
    termsUrl = ko.observable<string>();
    indexesUrl = ko.pureComputed(() => this.appUrls.indexes());
    
    selectedSourcePreview = ko.observable<additionalSource>();
    additionalSourcePreviewHtml: KnockoutComputed<string>;

    constructor() {
        super();

        this.bindToCurrentInstance("removeMap", 
            "removeField", 
            "createFieldNameAutocompleter", 
            "removeConfigurationOption", 
            "formatIndex", 
            "deleteAdditionalSource", 
            "previewAdditionalSource",
            "shouldDropupMenu",
            "formatReduce",
            "removeReduce",
            "addReduce",
            "removeAssembly",
            "addNamespaceToAssemblyWithBlink");

        aceEditorBindingHandler.install();
        autoCompleteBindingHandler.install();

        this.initializeObservables();
    }

    private initializeObservables() {
        this.editedIndex.subscribe(indexDef => {
            const firstMap = indexDef.maps()[0].map;

            firstMap.throttle(1000).subscribe(() => {
                this.updateIndexFields();
            });
        });

        this.canEditIndexName = ko.pureComputed(() => {
            return !this.isEditingExistingIndex();
        });

        this.nameChanged = ko.pureComputed(() => {
            const newName = this.editedIndex().name();
            const oldName = this.originalIndexName;

            return newName !== oldName;
        });
        
        this.additionalSourcePreviewHtml = ko.pureComputed(() => {
            const source = this.selectedSourcePreview();
            
            if (source) {
                return '<pre class="form-control sourcePreview">' + Prism.highlight(source.code(), (Prism.languages as any).csharp) + '</pre>';
            }
            
            const hasAdditionalSources = this.editedIndex().additionalSources().length > 0;
            const text = hasAdditionalSources ? "Click source file to view" : "No additional sources uploaded";
            const icon = hasAdditionalSources ? "" : `<i class="icon-lg icon-empty-set"></i>`

            return `<div class="sourcePreview text-center text-muted margin-top">
                        ${icon}
                        <h2 class="margin-top margin-top-sm">${text}</h2>
                    </div>`;
        });
    }

    canActivate(indexToEdit: string): JQueryPromise<canActivateResultDto> {
        const indexToEditName = indexToEdit || undefined;
        
        return $.when<any>(super.canActivate(indexToEditName))
            .then(() => {
                const db = this.activeDatabase();

                if (indexToEditName) {
                    this.isEditingExistingIndex(true);
                    const canActivateResult = $.Deferred<canActivateResultDto>();
                    this.fetchIndexToEdit(indexToEditName)
                        .done(() => canActivateResult.resolve({ can: true }))
                        .fail(() => {
                            messagePublisher.reportError("Could not find " + indexToEditName + " index");
                            canActivateResult.resolve({ redirect: appUrl.forIndexes(db) });
                        });
                    return canActivateResult;
                } else {
                    this.editedIndex(indexDefinition.empty());
                }

                return $.Deferred<canActivateResultDto>().resolve({ can: true });
            })
    }

    activate(indexToEditName: string) {
        super.activate(indexToEditName);

        if (this.isEditingExistingIndex()) {
            this.editExistingIndex(indexToEditName);
        }

        this.updateHelpLink('CQ5AYO');

        this.initializeDirtyFlag();
        this.indexAutoCompleter = new indexAceAutoCompleteProvider(this.activeDatabase(), this.editedIndex);

        this.initValidation();
        this.fetchIndexes();
    }

    attached() {
        super.attached();
        
        this.createKeyboardShortcut("ctrl+s", () => {
            this.save();
        }, editIndex.ContainerSelector);
        
        popoverUtils.longWithHover($("#reduce-output-info"),
            {
                content: 
                    "<small>" +
                        "Reduce results will be saved into documents that will be created under the provided collection name." +
                    "</small>"
            });

        popoverUtils.longWithHover($("#reference-docs-info"),
            {               
                content:
                    "<small>" +
                       "<ul>" +
                           "<li>" +
                                "A <i>Reference Collection</i> with documents that reference the above <i>Reduce Results Collection</i> will be created.<br>" +
                            "</li>" +
                            "<li>" +
                                "The collection name and the referencing document IDs pattern can be customized here below." +
                            "</li>" +
                        "</ul> " +
                    "</small>"
            });
        
        popoverUtils.longWithHover($("#reference-docs-pattern-info"),
            {
                content:
                    "<small>" +
                        "<ul>" +
                            "<li>" +
                                "<strong>The pattern</strong> entered is used to create the IDs for the Reference Collection documents.<br>" +
                            "</li>" +
                            "<li>" +
                                "Use any combination of index-field(s) and fixed text in the pattern.<br>" +
                                "i.e. <strong>fixed-text / {index-field-1} / {index-field-2}'</strong>" +
                            "</li>" +
                        "</ul> " +
                    "</small>"
            });

        popoverUtils.longWithHover($("#reference-docs-collection-name-info"),
            {
                content:
                    "<small>" +
                        "<ul>" +
                            "<li>" +
                                "Enter a name for the Referencing Documents Collection." +
                            "</li>" +
                            "<li>" +
                                "<strong>Default name</strong> is: <strong>'{reduce-results-collection-name} / References.'</strong>" +
                            "</li>" +
                        "</ul> " +
                    "</small>"
            });
    }

    compositionComplete() {
        super.compositionComplete();
        this.setupDisableReasons();
    }
    
    private initValidation() {
        this.editedIndex().name.extend({
            validation: [
                {
                    validator: (val: string) => {
                        return val === this.originalIndexName || !_.includes(this.indexesNames(), val);
                    },
                    message: "Already being used by an existing index."
                }]
        });
    }

    private fetchIndexes() {
        const db = this.activeDatabase();
        new getIndexNamesCommand(db)
            .execute()
            .done((indexesNames) => {
                this.indexesNames(indexesNames);
            });
    }

    private updateIndexFields() {
        const map = this.editedIndex().maps()[0].map();

        const additionalSourcesDto = {} as dictionary<string>;
        this.editedIndex().additionalSources().forEach(x => additionalSourcesDto[x.name()] = x.code());
        
        const additionalAssembliesDto = this.editedIndex().additionalAssemblies().map(x => x.toDto());

        new getIndexFieldsFromMapCommand(this.activeDatabase(), map, additionalSourcesDto, additionalAssembliesDto)
            .execute()
            .done((fields: resultsDto<string>) => {
                this.fieldNames(fields.Results);
            });
    }

    private initializeDirtyFlag() {
        const indexDef: indexDefinition = this.editedIndex();
        
        const hasAnyDirtyConfiguration = ko.pureComputed(() => {
           let anyDirty = false;
           indexDef.configuration().forEach(config => {
               if (config.dirtyFlag().isDirty()) {
                   anyDirty = true;
               } 
           });
           return anyDirty;
        });
        
        const hasAnyDirtyField = ko.pureComputed(() => {
            let anyDirty = false;
            indexDef.fields().forEach(field => {
                if (field.dirtyFlag().isDirty()) {
                    anyDirty = true;
                }
            });
            return anyDirty;
        });

        const hasDefaultFieldOptions = ko.pureComputed(() => !!indexDef.defaultFieldOptions());
        const hasAnyDirtyDefaultFieldOptions = ko.pureComputed(() => {
           if (hasDefaultFieldOptions() && indexDef.defaultFieldOptions().dirtyFlag().isDirty()) {
               return true;
           }
           return false;
        });

        const hasAnyDirtyAdditionalAssembly = ko.pureComputed(() => {
            let anyDirty = false;
            indexDef.additionalAssemblies().forEach(assembly => {
                if (assembly.dirtyFlag().isDirty()) {
                    anyDirty = true;
                }
            });
            return anyDirty;
        });
        
        this.dirtyFlag = new ko.DirtyFlag([
            indexDef.name, 
            indexDef.maps, 
            indexDef.reduce, 
            indexDef.numberOfFields,
            indexDef.numberOfConfigurationFields,
            indexDef.outputReduceToCollection,
            indexDef.createReferencesToResultsCollection,
            indexDef.reduceOutputCollectionName,
            indexDef.patternForReferencesToReduceOutputCollection,
            indexDef.collectionNameForReferenceDocuments,
            indexDef.additionalSources,
            indexDef.additionalAssemblies,
            hasAnyDirtyField,
            hasAnyDirtyConfiguration,
            hasDefaultFieldOptions,
            hasAnyDirtyDefaultFieldOptions,
            hasAnyDirtyAdditionalAssembly,
        ], false, jsonUtil.newLineNormalizingHashFunction);

        this.isSaveEnabled = ko.pureComputed(() => {
            const editIndex = this.isEditingExistingIndex();
            const isDirty = this.dirtyFlag().isDirty();

            return !editIndex || isDirty;
        });
    }

    private editExistingIndex(indexName: string) {
        this.originalIndexName = indexName;
        this.termsUrl(appUrl.forTerms(indexName, this.activeDatabase()));
        this.queryUrl(appUrl.forQuery(this.activeDatabase(), indexName));
    }

    mapIndexSyntaxHelp() {
        const viewmodel = new mapIndexSyntax();
        app.showBootstrapDialog(viewmodel);
    }

    mapReduceIndexSyntaxHelp() {
        const viewmodel = new mapReduceIndexSyntax();
        app.showBootstrapDialog(viewmodel);
    }

    additionalSourceSyntaxHelp() {
        const viewmodel = new additionalSourceSyntax();
        app.showBootstrapDialog(viewmodel);
    }

    additionalAssemblySyntaxHelp() {
        const viewmodel = new additionalAssemblySyntax();
        app.showBootstrapDialog(viewmodel);
    }

    addMap() {
        eventsCollector.default.reportEvent("index", "add-map");
        this.editedIndex().addMap();
    }

    addReduce() {
        eventsCollector.default.reportEvent("index", "add-reduce");
        const editedIndex = this.editedIndex();
        if (!editedIndex.hasReduce()) {
            editedIndex.hasReduce(true);
            editedIndex.reduce("");
            editedIndex.reduce.isModified(false);
        }
    }

    removeMap(mapIndex: number) {
        eventsCollector.default.reportEvent("index", "remove-map");
        this.editedIndex().maps.splice(mapIndex, 1);
    }

    removeReduce() {
        eventsCollector.default.reportEvent("index", "remove-reduce");
        this.editedIndex().reduce(null);
        this.editedIndex().hasReduce(false);
        this.editedIndex().reduce.clearError();
    }

    addField() {
        eventsCollector.default.reportEvent("index", "add-field");
        this.editedIndex().addField();
        this.setupDisableReasons();
    }

    removeField(field: indexFieldOptions) {
        eventsCollector.default.reportEvent("index", "remove-field");
        if (field.isDefaultOptions()) {
            this.editedIndex().removeDefaultFieldOptions();
        } else {
            this.editedIndex().fields.remove(field);
        }
    }

    addDefaultField() {
        eventsCollector.default.reportEvent("index", "add-field");
        this.editedIndex().addDefaultField();
        this.setupDisableReasons();
    }

    addConfigurationOption() {
        eventsCollector.default.reportEvent("index", "add-configuration-option");
        this.editedIndex().addConfigurationOption();
    }

    removeConfigurationOption(item: configurationItem) {
        eventsCollector.default.reportEvent("index", "remove-configuration-option");
        this.editedIndex().removeConfigurationOption(item);
    }

    createConfigurationOptionAutocompleter(item: configurationItem) {
        return ko.pureComputed(() => {
            const key = item.key();
            const options = configurationItem.PerDatabaseIndexingConfigurationOptions;
            const usedOptions = this.editedIndex().configuration().filter(f => f !== item).map(x => x.key());

            const filteredOptions = _.difference(options, usedOptions);

            if (key) {
                return filteredOptions.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                return filteredOptions;
            }
        });
    }

    createFieldNameAutocompleter(field: indexFieldOptions): KnockoutComputed<string[]> {
        return ko.pureComputed(() => {
            const name = field.name();
            const fieldNames = this.fieldNames();
            const otherFieldNames = this.editedIndex().fields().filter(f => f !== field).map(x => x.name());

            const filteredFieldNames = _.difference(fieldNames, otherFieldNames);

            if (name) {
                return filteredFieldNames.filter(x => x.toLowerCase().includes(name.toLowerCase()));
            } else {
                return filteredFieldNames;
            }
        });
    }

    private fetchIndexToEdit(indexName: string): JQueryPromise<Raven.Client.Documents.Indexes.IndexDefinition> {
        return new getIndexDefinitionCommand(indexName, this.activeDatabase())
            .execute()
            .done(result => {

                if (result.Type.startsWith("Auto")) {
                    // Auto Index
                    this.isAutoIndex(true);
                    this.editedIndex(new autoIndexDefinition(result));
                } else {
                    // Regular Index
                    this.editedIndex(new indexDefinition(result));
                    this.updateIndexFields();
                }

                this.originalIndexName = this.editedIndex().name();
                this.editedIndex().hasReduce(!!this.editedIndex().reduce());
            });
    }

    private validate(): boolean {
        let valid = true;

        const editedIndex = this.editedIndex();

        if (!this.isValid(editedIndex.validationGroup))
            valid = false;

        editedIndex.maps().forEach(map => {
            if (!this.isValid(map.validationGroup)) {
                valid = false;
            }
        });
        
        let fieldsTabInvalid = false;
        editedIndex.fields().forEach(field => {
            if (!this.isValid(field.validationGroup)) {
                valid = false;
                fieldsTabInvalid = true;
            }

            if (field.hasSpatialOptions()) {
                if (!this.isValid(field.spatial().validationGroup)) {
                    valid = false;
                    fieldsTabInvalid = true;
                }
            }
        });
        
        if (editedIndex.defaultFieldOptions()) {
            if (!this.isValid(editedIndex.defaultFieldOptions().validationGroup)) {
                valid = false;
                fieldsTabInvalid = true;
            }

            if (editedIndex.defaultFieldOptions().hasSpatialOptions()) {
                if (!this.isValid(editedIndex.defaultFieldOptions().spatial().validationGroup)) {
                    valid = false;
                    fieldsTabInvalid = true;
                }
            }
        }

        let configurationTabInvalid = false;
        editedIndex.configuration().forEach(config => {
            if (!this.isValid(config.validationGroup)) {
                valid = false;
                configurationTabInvalid = true;
            }
        });

        let additionalAssembliesTabInvalid = false;
        editedIndex.additionalAssemblies().forEach(assembly => {
            if (!this.isValid(assembly.validationGroup)) {
                valid = false;
                additionalAssembliesTabInvalid = true;
            }
        });

        // Navigate to invalid tab
        if (fieldsTabInvalid) {
            $('#tabsId a[href="#fields"]').tab('show');
        } else if (configurationTabInvalid) {
            $('#tabsId a[href="#configure"]').tab('show');
        } else if (additionalAssembliesTabInvalid) {
            $('#tabsId a[href="#additionalAssemblies"]').tab('show');
        }
        
        return valid;
    }

    save() {
        const editedIndex = this.editedIndex();
        
        viewHelpers.asyncValidationCompleted(editedIndex.validationGroup, () => {
            if (!this.validate()) {
                return;
            }

            this.saveInProgress(true);

            //if index name has changed it isn't the same index
            /* TODO
            if (this.originalIndexName === this.indexName() && editedIndex.lockMode === "LockedIgnore") {
                messagePublisher.reportWarning("Can not overwrite locked index: " + editedIndex.name() + ". " + 
                                                "Any changes to the index will be ignored.");
                return;
            }*/

            const indexDto = editedIndex.toDto();

            this.saveIndex(indexDto)
                .always(() => this.saveInProgress(false));
        });
    }

    private saveIndex(indexDto: Raven.Client.Documents.Indexes.IndexDefinition): JQueryPromise<string> {
        eventsCollector.default.reportEvent("index", "save");

        if (indexDto.Name.startsWith(index.SideBySideIndexPrefix)) {
            // trim side by side prefix
            indexDto.Name = indexDto.Name.substr(index.SideBySideIndexPrefix.length);
        }

        const db = this.activeDatabase();
        
        return new detectIndexTypeCommand(indexDto, db)
            .execute()
            .then((typeInfo) => {
                indexDto.SourceType = typeInfo.IndexSourceType;
                return new saveIndexDefinitionCommand(indexDto, typeInfo.IndexType === "JavaScriptMap" || typeInfo.IndexType === "JavaScriptMapReduce", db)
                    .execute()
                    .done((savedIndexName) => {
                        this.resetDirtyFlag();
                        router.navigate(appUrl.forIndexes(db));
                    });
            });
    }
    
    private resetDirtyFlag() {
        const indexDef: indexDefinition = this.editedIndex();
        
        if (indexDef.defaultFieldOptions()) {
            indexDef.defaultFieldOptions().dirtyFlag().reset();
        }

        indexDef.fields().forEach((field) => {
            field.spatial().dirtyFlag().reset();
            field.dirtyFlag().reset();
        });

        indexDef.configuration().forEach((config) => {
            config.dirtyFlag().reset();
        });

        indexDef.additionalAssemblies().forEach((assembly) => {
            assembly.dirtyFlag().reset();
        });
        
        this.dirtyFlag().reset();
    }

    deleteIndex() {
        eventsCollector.default.reportEvent("index", "delete");
        const indexName = this.originalIndexName;
        if (indexName) {
            const db = this.activeDatabase();
            const deleteViewModel = new deleteIndexesConfirm([this.editedIndex()], db);
            deleteViewModel.deleteTask.done((can: boolean) => {
                if (can) {
                    this.dirtyFlag().reset(); // Resync Changes
                    router.navigate(appUrl.forIndexes(db));
                }
            });

            dialog.show(deleteViewModel);
        }
    }

    cloneIndex() {
        this.isEditingExistingIndex(false);
        this.editedIndex().name(null);
        this.editedIndex().reduceOutputCollectionName(null);
        this.editedIndex().patternForReferencesToReduceOutputCollection(null);
        this.editedIndex().collectionNameForReferenceDocuments(null);
        this.editedIndex().validationGroup.errors.showAllMessages(false);
    }

    getCSharpCode() {
        eventsCollector.default.reportEvent("index", "generate-csharp-code");
        new getCSharpIndexDefinitionCommand(this.editedIndex().name(), this.activeDatabase())
            .execute()
            .done((data: string) => app.showBootstrapDialog(new showDataDialog("C# Index Definition", data, "csharp")));
    }

    formatIndex(mapIndex: number) {
        eventsCollector.default.reportEvent("index", "format-index");
        const index: indexDefinition = this.editedIndex();
        const mapToFormat = index.maps()[mapIndex].map;

        this.setFormattedText(mapToFormat);
    }

    formatReduce() {
        eventsCollector.default.reportEvent("index", "format-index");
        const index: indexDefinition = this.editedIndex();

        const reduceToFormat = index.reduce;

        this.setFormattedText(reduceToFormat);
    }

    private setFormattedText(textToFormat: KnockoutObservable<string>) {
        new formatIndexCommand(this.activeDatabase(), textToFormat())
            .execute()
            .done((formattedText) => {
                textToFormat(formattedText.Expression);
            });
    }

    fileSelected(fileInput: HTMLInputElement) {
        eventsCollector.default.reportEvent("index", "additional-source");
        fileImporter.readAsText(fileInput, (data, fileName) => this.onFileAdded(fileName, data));
    }
    
    private onFileAdded(fileName: string, contents: string) {
        const newItem = additionalSource.create(this.findUniqueNameForAdditionalSource(fileName), contents);
        this.editedIndex().additionalSources.push(newItem);
        this.selectedSourcePreview(newItem);
    }
    
    private findUniqueNameForAdditionalSource(fileName: string) {
        const sources = this.editedIndex().additionalSources;
        const existingItem = sources().find(x => x.name() === fileName);
        if (existingItem) {
            const extensionPosition = fileName.lastIndexOf(".");
            const fileNameWoExtension = fileName.substr(0, extensionPosition);
            
            let idx = 1;
            while (true) {
                const suggestedName = fileNameWoExtension + idx + ".cs";
                if (_.every(sources(), x => x.name() !== suggestedName)) {
                    return suggestedName;
                }
                idx++;
            }
        } else {
            return fileName;
        }
    }

    downloadAdditionalSource(source: additionalSource) {
        const code = source.code();

        fileDownloader.downloadAsTxt(code, source.name());
    }

    deleteAdditionalSource(sourceToDelete: additionalSource) {
        if (this.selectedSourcePreview() === sourceToDelete) {
            this.selectedSourcePreview(null);
        }
        this.editedIndex().additionalSources.remove(sourceToDelete);
    }

    previewAdditionalSource(source: additionalSource) {
        this.selectedSourcePreview(source);
    }

    shouldDropupMenu(field: indexFieldOptions, placeInList: number) {
        return ko.pureComputed(() => {

            // todo: calculate dropup menu according to location in view port..

            if (field.isDefaultFieldOptions() && this.editedIndex().fields().length)
                return false; // both default + a field is showing

            if (!field.isDefaultFieldOptions() && placeInList < this.editedIndex().fields().length - 1)
                return false; // field is not the last one

            return true;
        });
    }
    
    addAssembly() {
        eventsCollector.default.reportEvent("index", "add-assembly");
        this.editedIndex().addAssembly();
    }

    removeAssembly(assemblyItem: additionalAssembly) {
        eventsCollector.default.reportEvent("index", "remove-assembly");
        this.editedIndex().removeAssembly(assemblyItem);
    }

    addNamespaceToAssemblyWithBlink(assemblyItemToUpdate: additionalAssembly) {
        const namespaceToAdd = assemblyItemToUpdate.namespaceText();
        
        if (assemblyItemToUpdate.addNamespaceToUsings(namespaceToAdd)) {
            $(".usings .collection-list li").first().addClass("blink-style");
        }
    }
}

export = editIndex;

import router = require("plugins/router");
import viewModelBase = require("viewmodels/viewModelBase");
import index = require("models/database/index/index");
import document = require("models/database/documents/document");
import indexDefinition = require("models/database/index/indexDefinition");
import getIndexDefinitionCommand = require("commands/database/index/getIndexDefinitionCommand");
import appUrl = require("common/appUrl");
import dialog = require("plugins/dialog");
import jsonUtil = require("common/jsonUtil");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import messagePublisher = require("common/messagePublisher");
import autoCompleteBindingHandler = require("common/bindingHelpers/autoCompleteBindingHandler");
import app = require("durandal/app");
import indexAceAutoCompleteProvider = require("models/database/index/indexAceAutoCompleteProvider");
import getScriptedIndexesCommand = require("commands/database/index/getScriptedIndexesCommand");
import scriptedIndexModel = require("models/database/index/scriptedIndex");
import autoCompleterSupport = require("common/autoCompleterSupport");
import mergedIndexesStorage = require("common/mergedIndexesStorage");
import indexMergeSuggestion = require("models/database/index/indexMergeSuggestion");
import deleteIndexesConfirm = require("viewmodels/database/indexes/deleteIndexesConfirm");
import replaceIndexDialog = require("viewmodels/database/indexes/replaceIndexDialog");
import saveDocumentCommand = require("commands/database/documents/saveDocumentCommand");
import indexReplaceDocument = require("models/database/index/indexReplaceDocument");
import saveIndexDefinitionCommand = require("commands/database/index/saveIndexDefinitionCommand");
import renameIndexCommand = require("commands/database/index/renameIndexCommand");
import saveScriptedIndexesCommand = require("commands/database/documents/saveScriptedIndexesCommand");
import deleteIndexCommand = require("commands/database/index/deleteIndexCommand");
import cancelSideBySizeConfirm = require("viewmodels/database/indexes/cancelSideBySizeConfirm");
import copyIndexDialog = require("viewmodels/database/indexes/copyIndexDialog");
import getCSharpIndexDefinitionCommand = require("commands/database/index/getCSharpIndexDefinitionCommand");
import showDataDialog = require("viewmodels/common/showDataDialog");
import formatIndexCommand = require("commands/database/index/formatIndexCommand");
import renameOrDuplicateIndexDialog = require("viewmodels/database/indexes/renameOrDuplicateIndexDialog");
import indexFieldOptions = require("models/database/index/indexFieldOptions");
import getIndexFieldsCommand = require("commands/database/index/getIndexFieldsCommand");
import configurationItem = require("models/database/index/configurationItem");
import getDatabaseSettingsCommand = require("commands/resources/getDatabaseSettingsCommand");
import configuration = require("configuration");

import eventsCollector = require("common/eventsCollector");

class editIndex extends viewModelBase { 

    static readonly DefaultIndexStoragePath = "~/{Database Name}/Indexes";

    isEditingExistingIndex = ko.observable<boolean>(false);
    editedIndex = ko.observable<indexDefinition>();
    originalIndexName: string;
    isSaveEnabled: KnockoutComputed<boolean>;
    saveInProgress = ko.observable<boolean>(false);

    fieldNames = ko.observableArray<string>([]);
    defaultIndexPath = ko.observable<string>();
    additionalStoragePaths = ko.observableArray<string>([]);
    selectedIndexPath = ko.pureComputed(() => {
        const defaultPath = this.defaultIndexPath();
        const selectedPath = this.editedIndex().indexStoragePath();

        return selectedPath || defaultPath + " (default)";
    });

    queryUrl = ko.observable<string>();
    termsUrl = ko.observable<string>();

    /* TODO
    indexAutoCompleter: indexAceAutoCompleteProvider;
    canSaveSideBySideIndex: KnockoutComputed<boolean>;
     mergeSuggestion = ko.observable<indexMergeSuggestion>(null);
    */

    /*TODO scripted index
        isScriptedIndexBundleActive = ko.observable<boolean>(false);
        scriptedIndex = ko.observable<scriptedIndexModel>(null);
        indexScript = ko.observable<string>("");
        deleteScript = ko.observable<string>("");
    */
    
    constructor() {
        super();

        this.bindToCurrentInstance("removeMap", "removeField", "createFieldNameAutocompleter", "removeConfigurationOption");
      
        aceEditorBindingHandler.install();
        autoCompleteBindingHandler.install();

        this.editedIndex.subscribe(indexDef => {
            const firstMap = indexDef.maps()[0].map;

            firstMap.throttle(1000).subscribe(map => {
                this.updateIndexFields();
            });
        });

        /* TODO scripted index
        this.isScriptedIndexBundleActive.subscribe((active: boolean) => {
            if (active) {
                this.fetchOrCreateScriptedIndex();
            }
        });

        this.indexName.subscribe(name => {
            if (this.scriptedIndex() !== null) {
                this.scriptedIndex().indexName(name);
            }
        });

        this.scriptedIndex.subscribe(scriptedIndex => {
            this.indexScript = scriptedIndex.indexScript;
            this.deleteScript = scriptedIndex.deleteScript;
            this.initializeDirtyFlag();
            this.editedIndex().name.valueHasMutated();
        });*/

        
        /* TODO: side by side
        this.canSaveSideBySideIndex = ko.computed(() => {
            if (!this.isEditingExistingIndex()) {
                return false;
            }
            var loadedIndex = this.loadedIndexName(); // use loaded index name
            var editedName = this.editedIndex().name();
            return loadedIndex === editedName;
        });*/
    }

    canActivate(unescapedIndexToEditName: string): JQueryPromise<canActivateResultDto> {
        const indexToEditName = unescapedIndexToEditName ? decodeURIComponent(unescapedIndexToEditName) : undefined;
        super.canActivate(indexToEditName);
        
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

        /* TODO merge suggesions
        var mergeSuggestion: indexMergeSuggestion = mergedIndexesStorage.getMergedIndex(db, indexToEditName);
        if (mergeSuggestion != null) {
            this.mergeSuggestion(mergeSuggestion);
            this.editedIndex(mergeSuggestion.mergedIndexDefinition);
        }*/
    }

    activate(indexToEditName: string) {
        super.activate(indexToEditName);

        if (this.isEditingExistingIndex()) {
            this.editExistingIndex(indexToEditName);
        }

        this.updateHelpLink('CQ5AYO');

        this.initializeDirtyFlag();
        //TODO: this.indexAutoCompleter = new indexAceAutoCompleteProvider(this.activeDatabase(), this.editedIndex);
        //TODO: scripted index this.checkIfScriptedIndexBundleIsActive();
    }

    private updateIndexPaths() {
        new getDatabaseSettingsCommand(this.activeDatabase())
            .execute()
            .done((databaseDocument: document) => {
                const settings = (<any>databaseDocument)["Settings"] as dictionary<string>;
                const indexStoragePath = settings[configuration.indexing.indexStoragePath];
                //TODO: don't use .string here: but additionalIndexStoragePaths - waiting for RavenDB-5665)
                const additionalPaths = settings[configuration.indexing.string] ? settings[configuration.indexing.string].split(";") : [];
                this.additionalStoragePaths(additionalPaths);
                this.defaultIndexPath(indexStoragePath || editIndex.DefaultIndexStoragePath);
            })
            .fail((response: JQueryXHR) => messagePublisher.reportError("Failed to load database settings.", response.responseText, response.statusText))
    }

    private updateIndexFields() {
        const map = this.editedIndex().maps()[0].map();
        new getIndexFieldsCommand(this.activeDatabase(), map)
            .execute()
            .done((fields: string[]) => {
                this.fieldNames(fields);
            });
    }

    private initializeDirtyFlag() {
        const indexDef: indexDefinition = this.editedIndex();
        const checkedFieldsArray: Array<KnockoutObservable<any>> = [indexDef.name, indexDef.maps, indexDef.reduce, indexDef.numberOfFields, indexDef.indexStoragePath];

        const configuration = indexDef.configuration();
        if (configuration) {
            checkedFieldsArray.push(indexDef.numberOfConfigurationFields);

            configuration.forEach(configItem => {
                checkedFieldsArray.push(configItem.key);
                checkedFieldsArray.push(configItem.value);
            });
        }
        checkedFieldsArray.push(indexDef.maxIndexOutputsPerDocument);

        indexDef.fields().forEach(field => {
            checkedFieldsArray.push(field.name);
            checkedFieldsArray.push(field.analyzer);
            checkedFieldsArray.push(field.indexing);
            checkedFieldsArray.push(field.sort);
            checkedFieldsArray.push(field.storage);
            checkedFieldsArray.push(field.suggestions);
            checkedFieldsArray.push(field.termVector);
            checkedFieldsArray.push(field.hasSpatialOptions);

            const spatial = field.spatial();
            if (spatial) {
                checkedFieldsArray.push(spatial.type);
                checkedFieldsArray.push(spatial.strategy);
                checkedFieldsArray.push(spatial.maxTreeLevel);
                checkedFieldsArray.push(spatial.minX);
                checkedFieldsArray.push(spatial.maxX);
                checkedFieldsArray.push(spatial.minY);
                checkedFieldsArray.push(spatial.maxY);
                checkedFieldsArray.push(spatial.units);
            }
        });

        /* TODO: scripted index part
            checkedFieldsArray.push(this.indexScript);
            checkedFieldsArray.push(this.deleteScript);
        */

        this.dirtyFlag = new ko.DirtyFlag(checkedFieldsArray, false, jsonUtil.newLineNormalizingHashFunction);

        this.isSaveEnabled = ko.pureComputed(() => {
            const editIndex = this.isEditingExistingIndex();
            const isDirty = this.dirtyFlag().isDirty();

            return !editIndex || isDirty;
        });
    }

    private editExistingIndex(unescapedIndexName: string) {
        const indexName = decodeURIComponent(unescapedIndexName);
        this.originalIndexName = indexName;
        this.termsUrl(appUrl.forTerms(indexName, this.activeDatabase()));
        this.queryUrl(appUrl.forQuery(this.activeDatabase(), indexName));
    }

    addMap() {
        eventsCollector.default.reportEvent("index", "add-map");
        this.editedIndex().addMap();
    }

    addReduce() {
        eventsCollector.default.reportEvent("index", "add-reduce");
        if (!this.editedIndex().hasReduce()) {
            this.editedIndex().hasReduce(true);
            this.editedIndex().reduce("");
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
    }

    addField() {
        eventsCollector.default.reportEvent("index", "add-field");
        this.editedIndex().addField();
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
    }

    addConfigurationOption() {
        this.editedIndex().addConfigurationOption();
    }

    removeConfigurationOption(item: configurationItem) {
        this.editedIndex().removeConfigurationOption(item);
    }

    createConfigurationOptionAutocompleter(item: configurationItem) {
        return ko.pureComputed(() => {
            const key = item.key();
            const options = configurationItem.ConfigurationOptions;
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

    private fetchIndexToEdit(indexName: string): JQueryPromise<Raven.Client.Indexing.IndexDefinition> {
        return new getIndexDefinitionCommand(indexName, this.activeDatabase())
            .execute()
            .done(result => {
                this.editedIndex(new indexDefinition(result));
                this.originalIndexName = this.editedIndex().name();
                this.editedIndex().hasReduce(!!this.editedIndex().reduce());
                this.updateIndexFields();
                this.updateIndexPaths();
            })
    }

    private validate(): boolean {
        let valid = true;

        const editedIndex = this.editedIndex();   

        if (!this.isValid(this.editedIndex().validationGroup))
            valid = false;

        editedIndex.fields().forEach(field => {
            if (!this.isValid(field.validationGroup)) {
                valid = false;
            }
        });

        editedIndex.maps().forEach(map => {
            if (!this.isValid(map.validationGroup)) {
                valid = false;
            }
        });

        editedIndex.configuration().forEach(config => {
            if (!this.isValid(config.validationGroup)) {
                valid = false;
            }
        });

        return valid;
    }

    save() {
        const editedIndex = this.editedIndex();         

        if (!this.validate()) {
            return;
        }

        this.saveInProgress(true);

        

        //if index name has changed it isn't the same index
        /*
        if (this.originalIndexName === this.indexName() && editedIndex.lockMode === "LockedIgnore") {
            messagePublisher.reportWarning("Can not overwrite locked index: " + editedIndex.name() + ". " + 
                                            "Any changes to the index will be ignored.");
            return;
        }*/

        const indexDto = editedIndex.toDto();

        /* TODO
        if (this.isEditingExistingIndex() && index.Name !== this.loadedIndexName()) {
            // user changed index name on edit page, ask him what to do: rename or duplicate
            var dialog = new renameOrDuplicateIndexDialog(this.loadedIndexName(), this.editedIndex().name());

            dialog.getSaveAsNewTask()
                .done(() => this.saveIndex(indexDto));

            dialog.getRenameTask()
                .done(() => this.renameIndex(this.loadedIndexName(), index.Name as string));

            app.showBootstrapDialog(dialog);
        } else {*/
        this.saveIndex(indexDto)
            .always(() => this.saveInProgress(false));
        //TODO: }
    }

    private saveIndex(indexDto: Raven.Client.Indexing.IndexDefinition): JQueryPromise<any> { //TODO: use type
        eventsCollector.default.reportEvent("index", "save");
        const commands: Array<JQueryPromise<any>> = [];

        commands.push(new saveIndexDefinitionCommand(indexDto, this.activeDatabase()).execute());
        /* TODO scripted index
        if (this.scriptedIndex() !== null) {
            commands.push(new saveScriptedIndexesCommand([this.scriptedIndex()], this.activeDatabase()).execute());
        }*/

        return $.when.apply($, commands).done(() => {

            /* TODO
            if (this.scriptedIndex()) {
                this.fetchOrCreateScriptedIndex(); // reload scripted index to obtain fresh etag and metadata
            }*/
            this.dirtyFlag().reset();
            this.editedIndex().name.valueHasMutated();
            //TODO: merge suggestion: var isSavingMergedIndex = this.mergeSuggestion() != null;

            if (!this.isEditingExistingIndex()) {
                this.isEditingExistingIndex(true);
                this.editExistingIndex(indexDto.Name);
            }
            /* TODO merge suggestion
            if (isSavingMergedIndex) {
                var indexesToDelete = this.mergeSuggestion().canMerge.filter((indexName: string) => indexName != this.editedIndex().name());
                this.deleteMergedIndexes(indexesToDelete);
                this.mergeSuggestion(null);
            }*/

            this.updateUrl(indexDto.Name, false /* TODO isSavingMergedIndex */);
        });
    }

    updateUrl(indexName: string, isSavingMergedIndex: boolean = false) {
        const url = appUrl.forEditIndex(indexName, this.activeDatabase());
        if (this.originalIndexName !== indexName) {
            this.navigate(url);
        }
        /* TODO:merged index
        else if (isSavingMergedIndex) {
            super.updateUrl(url);
        }*/
    }

    deleteIndex() {
        eventsCollector.default.reportEvent("index", "delete");
        var indexName = this.originalIndexName;
        if (indexName) {
            var db = this.activeDatabase();
            var deleteViewModel = new deleteIndexesConfirm([indexName], db);
            deleteViewModel.deleteTask.done(() => {
                //prevent asking for unsaved changes
                this.dirtyFlag().reset(); // Resync Changes
                router.navigate(appUrl.forIndexes(db));
            });

            dialog.show(deleteViewModel);
        }
    }

    /* TODO
    refreshIndex() {
        eventsCollector.default.reportEvent("index", "refresh");
        var canContinue = this.canContinueIfNotDirty('Unsaved Data', 'You have unsaved data. Are you sure you want to refresh the index from the server?');
        canContinue.done(() => {
            this.fetchIndexData(this.originalIndexName)
                .done(() => {
                    this.initializeDirtyFlag();
                    this.editedIndex().name.valueHasMutated();
            });
        });
    }

    copyIndex() {
        eventsCollector.default.reportEvent("index", "copy");
        app.showBootstrapDialog(new copyIndexDialog(this.editedIndex().name(), this.activeDatabase(), false));
    }

    createCSharpCode() {
        eventsCollector.default.reportEvent("index", "generate-csharp-code");
        new getCSharpIndexDefinitionCommand(this.editedIndex().name(), this.activeDatabase())
            .execute()
            .done((data: string) => app.showBootstrapDialog(new showDataDialog("C# Index Definition", data, null)));
    }

    formatIndex() {
        eventsCollector.default.reportEvent("index", "format-index");
        var index: indexDefinition = this.editedIndex();
        var mapReduceObservableArray = new Array<KnockoutObservable<string>>();
        mapReduceObservableArray.push(...index.maps());
        if (!!index.reduce()) {
            mapReduceObservableArray.push(index.reduce);
        }

        var mapReduceArray = mapReduceObservableArray.map((observable: KnockoutObservable<string>) => observable());

        new formatIndexCommand(this.activeDatabase(), mapReduceArray)
            .execute()
            .done((formatedMapReduceArray: string[]) => {
                formatedMapReduceArray.forEach((element: string, i: number) => {
                    if (element.indexOf("Could not format:") == -1) {
                        mapReduceObservableArray[i](element);
                    } else {
                        var isReduce = !!index.reduce() && i == formatedMapReduceArray.length - 1;
                        var errorMessage = isReduce ? "Failed to format reduce!" : "Failed to format map '" + i + "'!";
                        messagePublisher.reportError(errorMessage, element);
                    }
                });
        });
    }*/

    /* TODO
    private renameIndex(existingIndexName: string, newIndexName: string): JQueryPromise<any> {
        eventsCollector.default.reportEvent("index", "rename");
        return new renameIndexCommand(existingIndexName, newIndexName, this.activeDatabase())
            .execute()
            .done(() => {
                this.initializeDirtyFlag();
                this.updateUrl(newIndexName, false);
            });
    }

    replaceIndex() {
        eventsCollector.default.reportEvent("index", "replace");
        var indexToReplaceName = this.editedIndex().name();
        var replaceDialog = new replaceIndexDialog(indexToReplaceName, this.activeDatabase());

        replaceDialog.replaceSettingsTask.done((replaceDocument: any) => {
            if (!this.editedIndex().isSideBySideIndex()) {
                this.editedIndex().name(index.SideBySideIndexPrefix + this.editedIndex().name());
            }

            var indexDef = this.editedIndex().toDto();
            var replaceDocumentKey = indexReplaceDocument.replaceDocumentPrefix + this.editedIndex().name();

            if (this.scriptedIndex() !== null) {
                // reset etag as we save different document
                this.scriptedIndex().__metadata.etag(null);
            }

            this.saveIndex(indexDef)
                .fail((response: JQueryXHR) => messagePublisher.reportError("Failed to save replace index.", response.responseText, response.statusText))
                .done(() => {
                    new saveDocumentCommand(replaceDocumentKey, replaceDocument, this.activeDatabase(), false)
                        .execute()
                        .fail((response: JQueryXHR) => messagePublisher.reportError("Failed to save replace index document.", response.responseText, response.statusText))
                        .done(() => messagePublisher.reportSuccess("Successfully saved side-by-side index"));
                })
                .always(() => dialog.close(replaceDialog));
        });

        app.showBootstrapDialog(replaceDialog);
    }*/

    //TODO: below we have functions for remaining features: 

    /* TODO test index
    makePermanent() {
        eventsCollector.default.reportEvent("index", "make-permanent");
        if (this.editedIndex().name() && this.editedIndex().isTestIndex()) {
            this.editedIndex().isTestIndex(false);
            // trim Test prefix
            var indexToDelete = this.editedIndex().name();
            this.editedIndex().name(this.editedIndex().name().substr(index.TestIndexPrefix.length));
            var indexDef = this.editedIndex().toDto();
            if (this.scriptedIndex() !== null) {
                // reset etag as we save different document
                this.scriptedIndex().__metadata.etag(null);
            }

            this.saveIndex(indexDef)
                .done(() => {
                    new deleteIndexCommand(indexToDelete, this.activeDatabase()).execute();
                });
        }
    }

    tryIndex() {
        eventsCollector.default.reportEvent("index", "try-index");
        if (this.editedIndex().name()) {
            if (!this.editedIndex().isTestIndex()) {
                this.editedIndex().isTestIndex(true);
                this.editedIndex().name(index.TestIndexPrefix + this.editedIndex().name());
            }
            var indexDef = this.editedIndex().toDto();

            if (this.scriptedIndex() !== null) {
                // reset etag as we save different document
                this.scriptedIndex().__metadata.etag(null);
            }
            
            this.saveIndex(indexDef);
        }
    }*/

    /* TODO side by side


    cancelSideBySideIndex() {
        eventsCollector.default.reportEvent("index", "cancel-side-by-side");
        var indexName = this.originalIndexName;
        if (indexName) {
            var db = this.activeDatabase();
            var cancelSideBySideIndexViewModel = new cancelSideBySizeConfirm([indexName], db);
            cancelSideBySideIndexViewModel.cancelTask.done(() => {
                //prevent asking for unsaved changes
                this.dirtyFlag().reset(); // Resync Changes
                router.navigate(appUrl.forIndexes(db));
            });

            dialog.show(cancelSideBySideIndexViewModel);
        }
    }

    */

     /*TODO merged indexes
    private deleteMergedIndexes(indexesToDelete: string[]) {
        var db = this.activeDatabase();
        var deleteViewModel = new deleteIndexesConfirm(indexesToDelete, db, "Delete Merged Indexes?");
        dialog.show(deleteViewModel);
    }*/

    /* TODO scripted index
    checkIfScriptedIndexBundleIsActive() {
        var db = this.activeDatabase();
        var activeBundles = db.activeBundles();
        this.isScriptedIndexBundleActive(activeBundles.indexOf("ScriptedIndexResults") != -1);
    }

    fetchOrCreateScriptedIndex() {
        var self = this;
        new getScriptedIndexesCommand(this.activeDatabase(), this.indexName())
            .execute()
            .done((scriptedIndexes: scriptedIndexModel[]) => {
                if (scriptedIndexes.length > 0) {
                    self.scriptedIndex(scriptedIndexes[0]);
                } else {
                    self.scriptedIndex(scriptedIndexModel.emptyForIndex(self.indexName()));
                }

                this.initializeDirtyFlag();
            });
    }

    private scriptedIndexCompleter(editor: any, session: any, pos: AceAjax.Position, prefix: string, callback: (errors: any[], wordlist: { name: string; value: string; score: number; meta: string }[]) => void) {
      var completions = [ 
        { name: "LoadDocument", args: "id" },
        { name: "PutDocument", args: "id, doc" },
        { name: "DeleteDocument", args: "id" }
      ];
        var result = completions
            .filter(entry => autoCompleterSupport.wordMatches(prefix, entry.name))
            .map(entry => { return { name: entry.name, value: entry.name, score: 100, meta: entry.args} });

        callback(null, result);
    }*/
}

export = editIndex;

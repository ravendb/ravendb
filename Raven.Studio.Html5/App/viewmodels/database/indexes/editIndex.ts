import router = require("plugins/router");
import viewModelBase = require("viewmodels/viewModelBase");
import index = require("models/database/index/index");
import indexDefinition = require("models/database/index/indexDefinition");
import luceneField = require("models/database/index/luceneField");
import spatialIndexField = require("models/database/index/spatialIndexField");
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

class editIndex extends viewModelBase { 

    isEditingExistingIndex = ko.observable<boolean>(false);
    mergeSuggestion = ko.observable<indexMergeSuggestion>(null);
    editedIndex = ko.observable<indexDefinition>();
    hasExistingReduce: KnockoutComputed<string>;
    hasMultipleMaps: KnockoutComputed<boolean>;
    termsUrl = ko.observable<string>();
    queryUrl = ko.observable<string>();
    editMaxIndexOutputsPerDocument = ko.observable<boolean>(false);
    indexErrorsList = ko.observableArray<string>();
    appUrls: computedAppUrls;
    indexName: KnockoutComputed<string>;
    currentIndexName: KnockoutComputed<string>;
    isSaveEnabled: KnockoutComputed<boolean>;
    indexAutoCompleter: indexAceAutoCompleteProvider;
    loadedIndexName = ko.observable<string>();
    originalIndexName: string;
    canSaveSideBySideIndex: KnockoutComputed<boolean>;
    // Scripted Index Part
    isScriptedIndexBundleActive = ko.observable<boolean>(false);
    scriptedIndex = ko.observable<scriptedIndexModel>(null);
    indexScript = ko.observable<string>("");
    deleteScript = ko.observable<string>("");
    
    constructor() {
        super();
      
        aceEditorBindingHandler.install();
        autoCompleteBindingHandler.install();

        this.appUrls = appUrl.forCurrentDatabase();

        this.hasExistingReduce = ko.computed(() => this.editedIndex() && this.editedIndex().reduce());
        this.hasMultipleMaps = ko.computed(() => this.editedIndex() && this.editedIndex().maps().length > 1);
        this.indexName = ko.computed(() => (!!this.editedIndex() && this.isEditingExistingIndex()) ? this.editedIndex().name() : "New Index");
        this.currentIndexName = ko.computed(() => this.isEditingExistingIndex() ? this.editedIndex().name() : (this.mergeSuggestion() != null) ? "Merged Index" : "New Index");

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
        });

        this.editedIndex(this.createNewIndexDefinition());
        this.canSaveSideBySideIndex = ko.computed(() => {
            var isEdit = this.isEditingExistingIndex();
            var loadedIndex = this.loadedIndexName();
            var editedName = this.editedIndex().name();
            return isEdit && (loadedIndex === editedName);
        });
    }

    canActivate(indexToEditName: string) {
        super.canActivate(indexToEditName);
        
        var db = this.activeDatabase();
        var mergeSuggestion: indexMergeSuggestion = mergedIndexesStorage.getMergedIndex(db, indexToEditName);
        if (mergeSuggestion != null) {
            this.mergeSuggestion(mergeSuggestion);
            this.editedIndex(mergeSuggestion.mergedIndexDefinition);
        }
        else if (indexToEditName) {
            this.isEditingExistingIndex(true);
            var canActivateResult = $.Deferred();
            this.fetchIndexData(indexToEditName)
                .done(() => canActivateResult.resolve({ can: true }))
                .fail(() => {
                    messagePublisher.reportError("Could not find " + decodeURIComponent(indexToEditName) + " index");
                    canActivateResult.resolve({ redirect: appUrl.forIndexes(db) });
                });
            return canActivateResult;
        }

        return $.Deferred().resolve({ can: true });
    }

    activate(indexToEditName: string) {
        super.activate(indexToEditName);
        
        if (this.isEditingExistingIndex()) {
            this.editExistingIndex(indexToEditName);
        }
        this.updateHelpLink('CQ5AYO');

        this.initializeDirtyFlag();
        this.indexAutoCompleter = new indexAceAutoCompleteProvider(this.activeDatabase(), this.editedIndex);
        this.checkIfScriptedIndexBundleIsActive();
    }

    attached() {
        super.attached();
        this.addMapHelpPopover();
        this.addReduceHelpPopover();
        this.addScriptsLabelPopover();
    }

    private initializeDirtyFlag() {
        var indexDef: indexDefinition = this.editedIndex();
        var checkedFieldsArray = [indexDef.storeAllFields, indexDef.name, indexDef.map, indexDef.maps, indexDef.reduce, indexDef.numOfLuceneFields, indexDef.numOfSpatialFields, indexDef.maxIndexOutputsPerDocument];

        indexDef.luceneFields().forEach((lf: luceneField) => {
            checkedFieldsArray.push(lf.name);
            checkedFieldsArray.push(lf.stores);
            checkedFieldsArray.push(lf.sort);
            checkedFieldsArray.push(lf.termVector);
            checkedFieldsArray.push(lf.indexing);
            checkedFieldsArray.push(lf.analyzer);
            checkedFieldsArray.push(lf.suggestionEnabled);
        });

        indexDef.spatialFields().forEach((sf: spatialIndexField) => {
            checkedFieldsArray.push(sf.name);
            checkedFieldsArray.push(sf.type);
            checkedFieldsArray.push(sf.strategy);
            checkedFieldsArray.push(sf.minX);
            checkedFieldsArray.push(sf.maxX);
            checkedFieldsArray.push(sf.circleRadiusUnits);
            checkedFieldsArray.push(sf.maxTreeLevel);
            checkedFieldsArray.push(sf.minY);
            checkedFieldsArray.push(sf.maxY);
        });


        checkedFieldsArray.push(this.indexScript);
        checkedFieldsArray.push(this.deleteScript);

        this.dirtyFlag = new ko.DirtyFlag(checkedFieldsArray, false, jsonUtil.newLineNormalizingHashFunction);

        this.isSaveEnabled = ko.computed(() => !!this.editedIndex().name() && this.dirtyFlag().isDirty());
    }

    private editExistingIndex(unescapedIndexName: string) {
        var indexName = decodeURIComponent(unescapedIndexName);
        this.loadedIndexName(indexName);
        this.termsUrl(appUrl.forTerms(unescapedIndexName, this.activeDatabase()));
        this.queryUrl(appUrl.forQuery(this.activeDatabase(), indexName));
    }

    addMapHelpPopover() {
        $("#indexMapsLabel").popover({
            html: true,
            trigger: 'hover',
            content: 'Maps project the fields to search on or to group by. It uses LINQ query syntax.<br/><br/>Example:</br><pre><span class="code-keyword">from</span> order <span class="code-keyword">in</span> docs.Orders<br/><span class="code-keyword">where</span> order.IsShipped<br/><span class="code-keyword">select new</span><br/>{</br>   order.Date, <br/>   order.Amount,<br/>   RegionId = order.Region.Id <br />}</pre>Each map function should project the same set of fields.',
        });
    }

    addReduceHelpPopover() {
        $("#indexReduceLabel").popover({
            html: true,
            trigger: 'hover',
            content: 'The Reduce function consolidates documents from the Maps stage into a smaller set of documents. It uses LINQ query syntax.<br/><br/>Example:</br><pre><span class="code-keyword">from</span> result <span class="code-keyword">in</span> results<br/><span class="code-keyword">group</span> result <span class="code-keyword">by new</span> { result.RegionId, result.Date } into g<br/><span class="code-keyword">select new</span><br/>{<br/>  Date = g.Key.Date,<br/>  RegionId = g.Key.RegionId,<br/>  Amount = g.Sum(x => x.Amount)<br/>}</pre>The objects produced by the Reduce function should have the same fields as the inputs.',
        });
    }

    private fetchIndexData(unescapedIndexName: string): JQueryPromise<any> {
        var indexName = decodeURIComponent(unescapedIndexName);
        return $.when(this.fetchIndexToEdit(indexName));
    }

    private fetchIndexToEdit(indexName: string): JQueryPromise<any> {
        var deferred = $.Deferred();

        new getIndexDefinitionCommand(indexName, this.activeDatabase())
            .execute()
            .done((results: indexDefinitionContainerDto) => {
                this.editedIndex(new indexDefinition(results.Index));
                this.originalIndexName = this.editedIndex().name();
                this.editMaxIndexOutputsPerDocument(results.Index.MaxIndexOutputsPerDocument ? results.Index.MaxIndexOutputsPerDocument > 0 ? true : false : false);
                deferred.resolve();
            })
            .fail(() => deferred.reject());

        return deferred;
    }



    createNewIndexDefinition(): indexDefinition {
        return indexDefinition.empty();
    }

    save() {
        var editedIndex = this.editedIndex();         
        //if index name has changed it isn't the same index
        if (this.originalIndexName === this.indexName() && editedIndex.lockMode === "LockedIgnore") {
            messagePublisher.reportWarning("Can not overwrite locked index: " + editedIndex.name() + ". " + 
                                            "Any changes to the index will be ignored.");
            return;
        }

        if (editedIndex.name()) {
            var index = editedIndex.toDto();

            if (this.isEditingExistingIndex() && index.Name !== this.loadedIndexName()) {
                // user changed index name on edit page, ask him what to do: rename or duplicate
                var dialog = new renameOrDuplicateIndexDialog(this.loadedIndexName(), this.editedIndex().name());

                dialog.getSaveAsNewTask()
                    .done(() => this.saveIndex(index));

                dialog.getRenameTask()
                    .done(() => this.renameIndex(this.loadedIndexName(), index.Name));

                app.showDialog(dialog);
            } else {
                this.saveIndex(index);
            }
        }
    }

    private deleteMergedIndexes(indexesToDelete: string[]) {
        var db = this.activeDatabase();
        var deleteViewModel = new deleteIndexesConfirm(indexesToDelete, db, "Delete Merged Indexes?");
        dialog.show(deleteViewModel);
    }

    updateUrl(indexName: string, isSavingMergedIndex: boolean = false) {
        var url = appUrl.forEditIndex(indexName, this.activeDatabase());
        if (this.loadedIndexName() !== indexName) {
            super.navigate(url);
        }
        else if (isSavingMergedIndex) {
            super.updateUrl(url);
        }
    }

    refreshIndex() {
        var canContinue = this.canContinueIfNotDirty('Unsaved Data', 'You have unsaved data. Are you sure you want to refresh the index from the server?');
        canContinue.done(() => {
            this.fetchIndexData(this.loadedIndexName())
                .done(() => {
                    this.initializeDirtyFlag();
                    this.editedIndex().name.valueHasMutated();
            });
        });
    }

    deleteIndex() {
        var indexName = this.loadedIndexName();
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

    cancelSideBySideIndex() {
        var indexName = this.loadedIndexName();
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

    addMap() {
        this.editedIndex().maps.push(ko.observable<string>());
    }

    addReduce() {
        if (!this.hasExistingReduce()) {
            this.editedIndex().reduce(" ");
            this.addReduceHelpPopover();
        }
    }

    addField() {
        var field = new luceneField("");
        field.indexFieldNames = this.editedIndex().fields();
        field.calculateFieldNamesAutocomplete();
        this.editedIndex().luceneFields.push(field);
    }

    removeStoreAllFields() {
        this.editedIndex().setOrRemoveStoreAllFields(false);
    }

    removeMaxIndexOutputs() {
        this.editedIndex().maxIndexOutputsPerDocument(0);
        this.editMaxIndexOutputsPerDocument(false);
    }

    addSpatialField() {
        var field = spatialIndexField.empty();
        this.editedIndex().spatialFields.push(field);
    }
    
    removeMap(mapIndex: number) {
        this.editedIndex().maps.splice(mapIndex, 1);
    }

    removeReduce() {
        this.editedIndex().reduce(null);
    }

    removeLuceneField(fieldIndex: number) {
        var fieldToRemove = this.editedIndex().luceneFields()[fieldIndex];
        this.editedIndex().luceneFields.splice(fieldIndex, 1);
        if (fieldToRemove.name() === "__all_fields") {
            this.editedIndex().setOrRemoveStoreAllFields(false);
        }
    }

    removeSpatialField(fieldIndex: number) {
        this.editedIndex().spatialFields.splice(fieldIndex, 1);
    }

    copyIndex() {
        app.showDialog(new copyIndexDialog(this.editedIndex().name(), this.activeDatabase(), false));
    }

    createCSharpCode() {
        new getCSharpIndexDefinitionCommand(this.editedIndex().name(), this.activeDatabase())
            .execute()
            .done((data: string) => {
              app.showDialog(new showDataDialog("C# Index Definition", data));
            });
    }

    formatIndex() {
        var index: indexDefinition = this.editedIndex();
        var mapReduceObservableArray = new Array<KnockoutObservable<string>>();
        mapReduceObservableArray.pushAll(index.maps());
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
    }

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

    private addScriptsLabelPopover() {
        var indexScriptpopOverSettings = {
            html: true,
            trigger: 'hover',
            content: 'Index Scripts are written in JavaScript.<br/><br/>Example:</br><pre><span class="code-keyword">var</span> company = LoadDocument(<span class="code-keyword">this</span>.Company);<br/><span class="code-keyword">if</span>(company == null) <span class="code-keyword">return</span>;<br/>company.Orders = { Count: <span class="code-keyword">this</span>.Count, Total: <span class="code-keyword">this</span>.Total };<br/>PutDocument(<span class="code-keyword">this</span>.Company, company);</pre>',
            selector: '.index-script-label',
        };
        $('#indexScriptPopover').popover(indexScriptpopOverSettings);
        var deleteScriptPopOverSettings = {
            html: true,
            trigger: 'hover',
            content: 'Index Scripts are written in JavaScript.<br/><br/>Example:</br><pre><span class="code-keyword">var</span> company = LoadDocument(<span class="code-keyword">this</span>.Company);<br/><span class="code-keyword">if</span> (company == null) <span class="code-keyword">return</span>;<br/><span class="code-keyword">delete</span> company.Orders;<br/>PutDocument(<span class="code-keyword">this</span>.Company, company);</pre>',
            selector: '.delete-script-label',
        };
        $('#deleteScriptPopover').popover(deleteScriptPopOverSettings);
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
    }

    makePermanent() {
        if (this.editedIndex().name() && this.editedIndex().isTestIndex()) {
            this.editedIndex().isTestIndex(false);
            // trim Test prefix
            var indexToDelete = this.editedIndex().name();
            this.editedIndex().name(this.editedIndex().name().substr(index.TestIndexPrefix.length));
            var indexDef = this.editedIndex().toDto();
            if (this.scriptedIndex() !== null) {
                // reset etag as we save different document
                delete this.scriptedIndex().__metadata.etag;
            }

            this.saveIndex(indexDef)
                .done(() => {
                    new deleteIndexCommand(indexToDelete, this.activeDatabase()).execute();
                });
        }
    }

    tryIndex() {
        if (this.editedIndex().name()) {
            if (!this.editedIndex().isTestIndex()) {
                this.editedIndex().isTestIndex(true);
                this.editedIndex().name(index.TestIndexPrefix + this.editedIndex().name());
            }
            var indexDef = this.editedIndex().toDto();

            if (this.scriptedIndex() !== null) {
                // reset etag as we save different document
                delete this.scriptedIndex().__metadata.etag;
            }
            
            this.saveIndex(indexDef);
        }
    }

    private renameIndex(existingIndexName: string, newIndexName: string): JQueryPromise<any> {
        return new renameIndexCommand(existingIndexName, newIndexName, this.activeDatabase())
            .execute()
            .done(() => {
                this.initializeDirtyFlag();
                this.updateUrl(newIndexName, false);
            });
    }

    private saveIndex(index: indexDefinitionDto): JQueryPromise<any> {
        var commands = [];

        commands.push(new saveIndexDefinitionCommand(index, this.activeDatabase()).execute());
        if (this.scriptedIndex() !== null) {
            commands.push(new saveScriptedIndexesCommand([this.scriptedIndex()], this.activeDatabase()).execute());
        }

        return $.when.apply($, commands).done(() => {

            if (this.scriptedIndex()) {
                this.fetchOrCreateScriptedIndex(); // reload scripted index to obtain fresh etag and metadata
            }
            
            this.initializeDirtyFlag();
            this.editedIndex().name.valueHasMutated();
            var isSavingMergedIndex = this.mergeSuggestion() != null;

            if (!this.isEditingExistingIndex()) {
                this.isEditingExistingIndex(true);
                this.editExistingIndex(index.Name);
            }
            if (isSavingMergedIndex) {
                var indexesToDelete = this.mergeSuggestion().canMerge.filter((indexName: string) => indexName != this.editedIndex().name());
                this.deleteMergedIndexes(indexesToDelete);
                this.mergeSuggestion(null);
            }

            this.updateUrl(index.Name, isSavingMergedIndex);
        });
    }

    replaceIndex() {
        var indexToReplaceName = this.editedIndex().name();
        var replaceDialog = new replaceIndexDialog(indexToReplaceName, this.activeDatabase());

        replaceDialog.replaceSettingsTask.done((replaceDocument) => {
            if (!this.editedIndex().isSideBySideIndex()) {
                this.editedIndex().name(index.SideBySideIndexPrefix + this.editedIndex().name());
            }

            var indexDef = this.editedIndex().toDto();
            var replaceDocumentKey = indexReplaceDocument.replaceDocumentPrefix + this.editedIndex().name();

            if (this.scriptedIndex() !== null) {
                // reset etag as we save different document
                delete this.scriptedIndex().__metadata.etag;
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

        app.showDialog(replaceDialog);
    }
}

export = editIndex;

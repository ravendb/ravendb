import router = require("plugins/router");
import viewModelBase = require("viewmodels/viewModelBase");
import index = require("models/index");
import indexDefinition = require("models/indexDefinition");
import indexPriority = require("models/indexPriority");
import luceneField = require("models/luceneField");
import spatialIndexField = require("models/spatialIndexField");
import getIndexDefinitionCommand = require("commands/getIndexDefinitionCommand");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import appUrl = require("common/appUrl");
import dialog = require("plugins/dialog");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import alertType = require("common/alertType");
import alertArgs = require("common/alertArgs");
import autoCompleteBindingHandler = require("common/autoCompleteBindingHandler");
import app = require("durandal/app");
import indexAceAutoCompleteProvider = require("models/indexAceAutoCompleteProvider");
import getDatabaseSettingsCommand = require("commands/getDatabaseSettingsCommand");
import getScriptedIndexesCommand = require("commands/getScriptedIndexesCommand");
import scriptedIndexModel = require("models/scriptedIndex");
import autoCompleterSupport = require("common/autoCompleterSupport");

class editIndex extends viewModelBase { 

    isEditingExistingIndex = ko.observable(false);
    priority = ko.observable<indexPriority>().extend({ required: true });
    priorityLabel: KnockoutComputed<string>;
    priorityFriendlyName: KnockoutComputed<string>;
    editedIndex = ko.observable<indexDefinition>();
    hasExistingReduce: KnockoutComputed<string>;
    hasExistingTransform: KnockoutComputed<string>;
    hasMultipleMaps: KnockoutComputed<boolean>;
    termsUrl = ko.observable<string>();
    queryUrl = ko.observable<string>();
    editMaxIndexOutputsPerDocument = ko.observable<boolean>(false);
    indexErrorsList = ko.observableArray<string>();
    appUrls: computedAppUrls;
    indexName: KnockoutComputed<string>;
    isSaveEnabled: KnockoutComputed<boolean>;
    indexAutoCompleter: indexAceAutoCompleteProvider;
    loadedIndexName = ko.observable<string>();

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

        this.priorityFriendlyName = ko.computed(() => this.getPriorityFriendlyName());
        this.priorityLabel = ko.computed(() => this.priorityFriendlyName() ? "Priority: " + this.priorityFriendlyName() : "Priority");
        this.hasExistingReduce = ko.computed(() => this.editedIndex() && this.editedIndex().reduce());
        this.hasExistingTransform = ko.computed(() => this.editedIndex() && this.editedIndex().transformResults());
        this.hasMultipleMaps = ko.computed(() => this.editedIndex() && this.editedIndex().maps().length > 1);
        this.indexName = ko.computed(() => (!!this.editedIndex() && this.isEditingExistingIndex()) ? this.editedIndex().name() : "New Index");
        
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

    }
    
    canActivate(indexToEditName: string) {
        super.canActivate(indexToEditName);

        if (indexToEditName) {
            var canActivateResult = $.Deferred();
            this.fetchIndexData(indexToEditName)
                .done(() => canActivateResult.resolve({ can: true }))
                .fail(() => {
                    ko.postbox.publish("Alert", new alertArgs(alertType.danger, "Could not find " + decodeURIComponent(indexToEditName) + " index", null));
                    canActivateResult.resolve({ redirect: appUrl.forIndexes(this.activeDatabase()) });
                });

            return canActivateResult;
        } else {
            return $.Deferred().resolve({ can: true });
        }
    }

    activate(indexToEditName: string) {
        super.activate(indexToEditName);
        
        this.isEditingExistingIndex(indexToEditName != null);

        if (indexToEditName) {
            this.editExistingIndex(indexToEditName);
        } else {
            this.priority(indexPriority.normal);
            this.editedIndex(this.createNewIndexDefinition());
        }

        this.initializeDirtyFlag();

        this.indexAutoCompleter = new indexAceAutoCompleteProvider(this.activeDatabase(), this.editedIndex);

        this.checkIfScriptedIndexBundleIsActive();

    }

    attached() {
        this.addMapHelpPopover();
        this.addReduceHelpPopover();
        this.addTransformHelpPopover();
        this.addScriptsLabelPopover();
    }

    private initializeDirtyFlag() {
        var indexDef: indexDefinition = this.editedIndex();
        var checkedFieldsArray = [this.priority, indexDef.name, indexDef.map, indexDef.maps, indexDef.reduce, indexDef.transformResults, indexDef.numOfLuceneFields, indexDef.numOfSpatialFields, indexDef.maxIndexOutputsPerDocument];

        indexDef.luceneFields().forEach((lf: luceneField) => {
            checkedFieldsArray.push(lf.name);
            checkedFieldsArray.push(lf.stores);
            checkedFieldsArray.push(lf.sort);
            checkedFieldsArray.push(lf.termVector);
            checkedFieldsArray.push(lf.indexing);
            checkedFieldsArray.push(lf.analyzer);
            checkedFieldsArray.push(lf.suggestionDistance);
            checkedFieldsArray.push(lf.suggestionAccuracy);
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

        this.dirtyFlag = new ko.DirtyFlag(checkedFieldsArray);

        this.isSaveEnabled = ko.computed(() => !!this.editedIndex().name() && this.dirtyFlag().isDirty());
    }

    private editExistingIndex(unescapedIndexName: string) {
        var indexName = decodeURIComponent(unescapedIndexName);
        this.loadedIndexName(indexName);
        this.termsUrl(appUrl.forTerms(indexName, this.activeDatabase()));
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

    addTransformHelpPopover() {
        $("#indexTransformLabel").popover({
            html: true,
            trigger: 'hover',
            content: '<span class="text-danger">Deprecated.</span> Index Transform has been replaced with <strong>Result Transformers</strong>.<br/><br/>The Transform function allows you to change the shape of individual result documents before the server returns them. It uses LINQ query syntax.<br/><br/>Example:<pre><span class="code-keyword">from</span> order <span class="code-keyword">in</span> orders<br/><span class="code-keyword">let</span> region = Database.Load(result.RegionId)<br/><span class="code-keyword">select new</span><br/>{<br/>   result.Date,<br/>   result.Amount,<br/>   Region = region.Name,<br/>   Manager = region.Manager<br/>}</pre>'
        });
    }

    private fetchIndexData(unescapedIndexName: string): JQueryPromise<any> {
        var indexName = decodeURIComponent(unescapedIndexName);
        return $.when(this.fetchIndexToEdit(indexName), this.fetchIndexPriority(indexName));
    }

    private fetchIndexToEdit(indexName: string): JQueryPromise<any> {
        var deferred = $.Deferred();

        new getIndexDefinitionCommand(indexName, this.activeDatabase())
            .execute()
            .done((results: indexDefinitionContainerDto) => {
                this.editedIndex(new indexDefinition(results.Index));
                this.editMaxIndexOutputsPerDocument(results.Index.MaxIndexOutputsPerDocument ? results.Index.MaxIndexOutputsPerDocument > 0 ? true : false : false);
                deferred.resolve();
            })
            .fail(() => deferred.reject());

        return deferred;
    }

    private fetchIndexPriority(indexName: string): JQueryPromise<any> {
        var deferred = $.Deferred();

        new getDatabaseStatsCommand(this.activeDatabase())
            .execute()
            .done((stats: databaseStatisticsDto) => {
                var lowerIndexName = indexName.toLowerCase();
                var matchingIndex = stats.Indexes.first(i => i.PublicName.toLowerCase() === lowerIndexName);
                if (matchingIndex) {
                    var priorityWithoutWhitespace = matchingIndex.Priority.replace(", ", ",");
                    this.priority(index.priorityFromString(priorityWithoutWhitespace));
                }
                deferred.resolve();
            })
            .fail(() => deferred.reject());

        return deferred;
    }

    createNewIndexDefinition(): indexDefinition {
        return indexDefinition.empty();
    }

    save() {
        if (this.editedIndex().name()) {
            var index = this.editedIndex().toDto();

            require(["commands/saveIndexDefinitionCommand", "commands/saveScriptedIndexesCommand"], (saveIndexDefinitionCommand, saveScriptedIndexesCommand) => {
                var commands = [];

                commands.push(new saveIndexDefinitionCommand(index, this.priority(), this.activeDatabase()).execute());
                if (this.scriptedIndex() !== null) {
                    commands.push(new saveScriptedIndexesCommand([this.scriptedIndex()], this.activeDatabase()).execute());
                }

                jQuery.when(commands).then(() => {
                    this.initializeDirtyFlag();
                    this.editedIndex().name.valueHasMutated();

                    if (!this.isEditingExistingIndex()) {
                        this.isEditingExistingIndex(true);
                        this.editExistingIndex(index.Name);
                    }
                    this.updateUrl(index.Name);
                });
            }); 
        }
    }

    updateUrl(indexName: string) {
        if (indexName != null && this.loadedIndexName() !== indexName) {
            router.navigate(appUrl.forEditIndex(indexName, this.activeDatabase()));
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
            require(["viewmodels/deleteIndexesConfirm"], deleteIndexesConfirm => {
                var db = this.activeDatabase();
                var deleteViewModel = new deleteIndexesConfirm([indexName], db);
                deleteViewModel.deleteTask.done(() => {
                    //prevent asking for unsaved changes
                    this.dirtyFlag().reset(); // Resync Changes
                    router.navigate(appUrl.forIndexes(db));
                });

                dialog.show(deleteViewModel);
            });
        }
    }

    idlePriority() {
        this.priority(indexPriority.idleForced);
    }

    disabledPriority() {
        this.priority(indexPriority.disabledForced);
    }

    abandonedPriority() {
        this.priority(indexPriority.abandonedForced);
    }

    normalPriority() {
        this.priority(indexPriority.normal);
    }

    getPriorityFriendlyName(): string {
        // Instead of showing things like "Idle,Forced", just show Idle.
        
        var priority = this.priority();
        if (!priority) {
            return "";
        }
        if (priority === indexPriority.idleForced) {
            return index.priorityToString(indexPriority.idle);
        }
        if (priority === indexPriority.disabledForced) {
            return index.priorityToString(indexPriority.disabled);
        }
        if (priority === indexPriority.abandonedForced) {
            return index.priorityToString(indexPriority.abandoned);
        }

        return index.priorityToString(priority);
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

    addTransform() {
        if (!this.hasExistingTransform()) {
            this.editedIndex().transformResults(" ");
            this.addTransformHelpPopover();
        }
    }

    addField() {
        var field = new luceneField("");
        field.indexFieldNames = this.editedIndex().fields();
        field.calculateFieldNamesAutocomplete();
        this.editedIndex().luceneFields.push(field);
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

    removeTransform() {
        this.editedIndex().transformResults(null);
    }

    removeLuceneField(fieldIndex: number) {
        this.editedIndex().luceneFields.splice(fieldIndex, 1);
    }

    removeSpatialField(fieldIndex: number) {
        this.editedIndex().spatialFields.splice(fieldIndex, 1);
    }

    copyIndex() {
        require(["viewmodels/copyIndexDialog"], copyIndexDialog => {
            app.showDialog(new copyIndexDialog(this.editedIndex().name(), this.activeDatabase(), false));
        });   
    }

    createCSharpCode() {
        require(["commands/getCSharpIndexDefinitionCommand"], getCSharpIndexDefinitionCommand => {
            new getCSharpIndexDefinitionCommand(this.editedIndex().name(), this.activeDatabase())
                .execute()
                .done((data: string) => {
                    require(["viewmodels/showDataDialog"], showDataDialog => {
                        app.showDialog(new showDataDialog("C# Index Definition", data));
                    });
                });
        });
    }

    checkIfScriptedIndexBundleIsActive() {
        new getDatabaseSettingsCommand(this.activeDatabase())
            .execute()
            .done(document => {
                var documentSettings = document.Settings["Raven/ActiveBundles"];
                this.isScriptedIndexBundleActive(documentSettings.indexOf("ScriptedIndex") !== -1);
            });
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
            content: 'Index Scripts are written in JScript.<br/><br/>Example:</br><pre><span class="code-keyword">var</span> company = LoadDocument(<span class="code-keyword">this</span>.Company);<br/><span class="code-keyword">if</span>(company == null) <span class="code-keyword">return</span>;<br/>company.Orders = { Count: <span class="code-keyword">this</span>.Count, Total: <span class="code-keyword">this</span>.Total };<br/>PutDocument(<span class="code-keyword">this</span>.Company, company);</pre>',
            selector: '.index-script-label',
        };
        $('#indexScriptPopover').popover(indexScriptpopOverSettings);
        var deleteScriptPopOverSettings = {
            html: true,
            trigger: 'hover',
            content: 'Index Scripts are written in JScript.<br/><br/>Example:</br><pre><span class="code-keyword">var</span> company = LoadDocument(<span class="code-keyword">this</span>.Company);<br/><span class="code-keyword">if</span> (company == null) <span class="code-keyword">return</span>;<br/><span class="code-keyword">delete</span> company.Orders;<br/>PutDocument(<span class="code-keyword">this</span>.Company, company);</pre>',
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
}

export = editIndex; 

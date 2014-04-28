import router = require("plugins/router");
import viewModelBase = require("viewmodels/viewModelBase");
import index = require("models/index");
import indexDefinition = require("models/indexDefinition");
import indexPriority = require("models/indexPriority");
import luceneField = require("models/luceneField");
import spatialIndexField = require("models/spatialIndexField");
import getIndexDefinitionCommand = require("commands/getIndexDefinitionCommand");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import saveIndexDefinitionCommand = require("commands/saveIndexDefinitionCommand");
import appUrl = require("common/appUrl");
import deleteIndexesConfirm = require("viewmodels/deleteIndexesConfirm");
import dialog = require("plugins/dialog");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import alertType = require("common/alertType");
import alertArgs = require("common/alertArgs");

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
    statsUrl = ko.observable<string>();
    queryUrl = ko.observable<string>();
    editMaxIndexOutputsPerDocument = ko.observable<boolean>(false);
    indexErrorsList = ko.observableArray<string>();

    constructor() {
        super();

        aceEditorBindingHandler.install();

        this.priorityFriendlyName = ko.computed(() => this.getPriorityFriendlyName());
        this.priorityLabel = ko.computed(() => this.priorityFriendlyName() ? "Priority: " + this.priorityFriendlyName() : "Priority");
        this.hasExistingReduce = ko.computed(() => this.editedIndex() && this.editedIndex().reduce());
        this.hasExistingTransform = ko.computed(() => this.editedIndex() && this.editedIndex().transformResults());
        this.hasMultipleMaps = ko.computed(() => this.editedIndex() && this.editedIndex().maps().length > 1);
    }
    
    canActivate(indexToEditName: string) {
        if (indexToEditName) {
            var canActivateResult = $.Deferred();
            this.fetchIndexToEdit(indexToEditName)
                .done(()=> canActivateResult.resolve({ can: true }))
                .fail(() => {
                    ko.postbox.publish("Alert", new alertArgs(alertType.danger, "Could not find " + decodeURIComponent(indexToEditName) + " index", null));
                    canActivateResult.resolve({ redirect: appUrl.forIndexes(this.activeDatabase() )});
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

        var indexDef = this.editedIndex();
        viewModelBase.dirtyFlag = new ko.DirtyFlag([this.priority, indexDef.name, indexDef.map, indexDef.maps, indexDef.reduce, indexDef.fields, indexDef.transformResults, indexDef.spatialFields, indexDef.maxIndexOutputsPerDocument]);
        //need to add more fields like: this.editedIndex().luceneFields()[0].name, this.editedIndex().luceneFields()[0].indexing()
    }

    attached() {
        this.addMapHelpPopover();
        this.addReduceHelpPopover();
        this.addTransformHelpPopover();
    }

    // Called back after the entire composition has finished (parents and children included)
    compositionComplete() {
        super.compositionComplete();
    }

    /*saveInObservable() {
        var docEditor = ace.edit("docEditor");
        var docEditorText = docEditor.getSession().getValue();
        this.editedTransformer().transformResults(docEditorText);
    }*/

    editExistingIndex(unescapedIndexName: string) {
        var indexName = decodeURIComponent(unescapedIndexName);
        this.fetchIndexPriority(indexName);
        this.termsUrl(appUrl.forTerms(indexName, this.activeDatabase()));
        this.statsUrl(appUrl.forStatus(this.activeDatabase()));
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
            content: 'The Reduce function consolidates documents from the Maps stage into a smaller set of documents. It uses LINQ query syntax.<br/><br/>Example:</br><pre><span class="code-keyword">from</span> result <span class="code-keyword">in</span> results<br/><span class="code-keyword">group</span> result <span class="code-keyword">by new</span> { result.RegionId, result.Date }<br/><span class="code-keyword">select new</span><br/>{<br/>  Date = g.Key.Date,<br/>  RegionId = g.Key.RegionId,<br/>  Amount = g.Sum(x => x.Amount)<br/>}</pre>The objects produced by the Reduce function should have the same fields as the inputs.',
        });
    }

    addTransformHelpPopover() {
        $("#indexTransformLabel").popover({
            html: true,
            trigger: 'hover',
            content: '<span class="text-danger">Deprecated.</span> Index Transform has been replaced with <strong>Result Transformers</strong>.<br/><br/>The Transform function allows you to change the shape of individual result documents before the server returns them. It uses LINQ query syntax.<br/><br/>Example:<pre><span class="code-keyword">from</span> order <span class="code-keyword">in</span> orders<br/><span class="code-keyword">let</span> region = Database.Load(result.RegionId)<br/><span class="code-keyword">select new</span><br/>{<br/>   result.Date,<br/>   result.Amount,<br/>   Region = region.Name,<br/>   Manager = region.Manager<br/>}</pre>'
        });
    }

    fetchIndexToEdit(indexName: string) : JQueryPromise<any>{
        return new getIndexDefinitionCommand(indexName, this.activeDatabase())
            .execute()
            .done((results: indexDefinitionContainerDto) => {
                this.editedIndex(new indexDefinition(results.Index));
                this.editMaxIndexOutputsPerDocument(results.Index.MaxIndexOutputsPerDocument ? results.Index.MaxIndexOutputsPerDocument > 0 ? true : false : false);
        });
    }

    fetchIndexPriority(indexName: string) {
        new getDatabaseStatsCommand(this.activeDatabase())
            .execute()
            .done((stats: databaseStatisticsDto) => {
                var lowerIndexName = indexName.toLowerCase();
                var matchingIndex = stats.Indexes.first(i => i.PublicName.toLowerCase() === lowerIndexName);
                if (matchingIndex) {
                    var priorityWithoutWhitespace = matchingIndex.Priority.replace(", ", ",");
                    this.priority(index.priorityFromString(priorityWithoutWhitespace));
                }
            });
    }

    createNewIndexDefinition(): indexDefinition {
        return indexDefinition.empty();
    }

    save() {
        if (this.editedIndex().name()) {
            var index = this.editedIndex().toDto();
            var saveCommand = new saveIndexDefinitionCommand(index, this.priority(), this.activeDatabase());
            saveCommand
                .execute()
                .done(() => {
                    // Resync Changes
                    viewModelBase.dirtyFlag().reset();

                    if (!this.isEditingExistingIndex()) {
                        this.isEditingExistingIndex(true);
                        this.editExistingIndex(index.Name);
                    }
                this.updateUrl(index.Name);
            });
        }
    }

    updateUrl(indexName: string) {
        router.navigate(appUrl.forEditIndex(indexName, this.activeDatabase()));
    }

    refreshIndex() {
        var existingIndex = this.editedIndex();
        if (existingIndex) {
            this.editedIndex(null);
            this.editExistingIndex(existingIndex.name());
        }

        // Resync Changes
        viewModelBase.dirtyFlag().reset();
    }

    deleteIndex() {
        var index = this.editedIndex();
        if (index) {
            var db = this.activeDatabase();
            var deleteViewModel = new deleteIndexesConfirm([index.name()], db);
            deleteViewModel.deleteTask.done(() => router.navigate(appUrl.forIndexes(db)))

            dialog.show(deleteViewModel);
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
        this.editedIndex().luceneFields.push(field);
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
}

export = editIndex; 
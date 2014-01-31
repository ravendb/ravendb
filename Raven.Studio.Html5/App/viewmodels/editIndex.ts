import activeDbViewModelBase = require("viewmodels/activeDbViewModelBase");
import index = require("models/index");
import indexDefinition = require("models/indexDefinition");
import indexPriority = require("models/indexPriority");
import luceneField = require("models/luceneField");
import spatialIndexField = require("models/spatialIndexField");
import getIndexDefinitionCommand = require("commands/getIndexDefinitionCommand");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import saveIndexDefinitionCommand = require("commands/saveIndexDefinitionCommand");
import appUrl = require("common/appUrl");

class editIndex extends activeDbViewModelBase { 

    isEditingExistingIndex = ko.observable(false);
    priority = ko.observable<indexPriority>();
    priorityLabel: KnockoutComputed<string>;
    priorityFriendlyName: KnockoutComputed<string>;
    editedIndex = ko.observable<indexDefinition>();
    hasExistingReduce: KnockoutComputed<string>;
    hasExistingTransform: KnockoutComputed<string>;
    hasMultipleMaps: KnockoutComputed<boolean>;
    termsUrl = ko.observable<string>();
    statsUrl = ko.observable<string>();

    constructor() {
        super();

        this.priorityFriendlyName = ko.computed(() => this.getPriorityFriendlyName());
        this.priorityLabel = ko.computed(() => this.priorityFriendlyName() ? "Priority: " + this.priorityFriendlyName() : "Priority");
        this.hasExistingReduce = ko.computed(() => this.editedIndex() && this.editedIndex().reduce());
        this.hasExistingTransform = ko.computed(() => this.editedIndex() && this.editedIndex().transformResults());
        this.hasMultipleMaps = ko.computed(() => this.editedIndex() && this.editedIndex().maps().length > 1);
    }

    activate(indexToEditName: string) {
        super.activate(indexToEditName);

        this.isEditingExistingIndex(indexToEditName != null);

        if (indexToEditName) {
            this.fetchIndexToEdit(indexToEditName);
            this.fetchIndexPriority(indexToEditName);
            this.termsUrl(appUrl.forTerms(indexToEditName, this.activeDatabase()));
            this.statsUrl(appUrl.forStatus(this.activeDatabase()));
        } else {
            this.priority(indexPriority.normal);
            this.editedIndex(this.createNewIndexDefinition());
        }
    }

    attached() {
        $("#indexMapsLabel").popover({
            html: true,
            trigger: 'hover',
            content: 'Maps project the fields to search on or to group by. It uses LINQ query syntax.<br/><br/>Example:</br><pre><span class="code-keyword">from</span> order <span class="code-keyword">in</span> docs.Orders<br/><span class="code-keyword">where</span> order.IsShipped<br/><span class="code-keyword">select new</span><br/>{</br>   order.Date, <br/>   order.Amount,<br/>   RegionId = order.Region.Id <br />}</pre>Each map function should project the same set of fields.',
        });

        $("#indexReduceLabel").popover({
            html: true,
            trigger: 'hover',
            content: 'The Reduce function consolidates documents from the Maps stage into a smaller set of documents. It uses LINQ query syntax.<br/><br/>Example:</br><pre><span class="code-keyword">from</span> result <span class="code-keyword">in</span> results<br/><span class="code-keyword">group</span> result <span class="code-keyword">by new</span> { result.RegionId, result.Date }<br/><span class="code-keyword">select new</span><br/>{<br/>  Date = g.Key.Date,<br/>  RegionId = g.Key.RegionId,<br/>  Amount = g.Sum(x => x.Amount)<br/>}</pre>The objects produced by the Reduce function should have the same fields as the inputs.',
        });

        $("#indexTransformLabel").popover({
            html: true,
            trigger: 'hover',
            content: '<span class="text-danger">Deprecated.</span> Index Transform has been replaced with <strong>Result Transformers</strong>.<br/><br/>The Transform function allows you to change the shape of individual result documents before the server returns them. It uses LINQ query syntax.<br/><br/>Example:<pre><span class="code-keyword">from</span> order <span class="code-keyword">in</span> orders<br/><span class="code-keyword">let</span> region = Database.Load(result.RegionId)<br/><span class="code-keyword">select new</span><br/>{<br/>   result.Date,<br/>   result.Amount,<br/>   Region = region.Name,<br/>   Manager = region.Manager<br/>}</pre>'
        });
    }

    fetchIndexToEdit(indexName: string) {
        new getIndexDefinitionCommand(indexName, this.activeDatabase())
            .execute()
            .done((results: indexDefinitionContainerDto) => this.editedIndex(new indexDefinition(results.Index)));
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
        return null;
    }

    save() {
        var index = this.editedIndex().toDto();
        var saveCommand = new saveIndexDefinitionCommand(index, this.priority(), this.activeDatabase());
        saveCommand.execute();
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
        }
    }

    addTransform() {
        if (!this.hasExistingTransform()) {
            this.editedIndex().transformResults(" ");
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
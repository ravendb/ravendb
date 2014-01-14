import activeDbViewModelBase = require("viewmodels/activeDbViewModelBase");
import index = require("models/index");
import indexDefinition = require("models/indexDefinition");
import getIndexDefinitionCommand = require("commands/getIndexDefinitionCommand");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");

class editIndex extends activeDbViewModelBase { 

    isCreatingNewIndex = ko.observable(false);
    priority = ko.observable("");
    priorityText: KnockoutComputed<string>;
    editedIndex = ko.observable<indexDefinition>();
    hasExistingReduce: KnockoutComputed<string>;
    hasExistingTransform: KnockoutComputed<string>;

    constructor() {
        super();

        this.priorityText = ko.computed(() => this.priority() ? "Priority: " + this.priority() : "Priority");
        this.hasExistingReduce = ko.computed(() => this.editedIndex() && this.editedIndex().reduce());
        this.hasExistingTransform = ko.computed(() => this.editedIndex() && this.editedIndex().transformResults());
    }

    activate(indexToEditName: string) {
        super.activate(indexToEditName);

        this.isCreatingNewIndex(indexToEditName == null);

        if (indexToEditName) {
            this.fetchIndexToEdit(indexToEditName);
            this.fetchIndexPriority(indexToEditName);
        } else {
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
            content: '<span class="text-danger">Deprecated.</span> Index Transform has been replaced with <span class="text-info">Result Transformers</span>.<br/><br/>The Transform function allows you to change the shape of individual result documents before the server returns them. It uses LINQ query syntax.<br/><br/>Example:<pre><span class="code-keyword">from</span> order <span class="code-keyword">in</span> orders<br/><span class="code-keyword">let</span> region = Database.Load(result.RegionId)<br/><span class="code-keyword">select new</span><br/>{<br/>   result.Date,<br/>   result.Amount,<br/>   Region = region.Name,<br/>   Manager = region.Manager<br/>}</pre>'
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
                var matchingIndex = stats.Indexes.first(i => i.Name.toLowerCase() === lowerIndexName);
                if (matchingIndex) {
                    this.priority(matchingIndex.Priority);
                }
            });
    }

    createNewIndexDefinition(): indexDefinition {
        return null;
    }
}

export = editIndex; 
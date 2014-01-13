import activeDbViewModelBase = require("viewmodels/activeDbViewModelBase");
import index = require("models/index");
import getIndexDefinitionCommand = require("commands/getIndexDefinitionCommand");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");

class editIndex extends activeDbViewModelBase { 

    isCreatingNewIndex = ko.observable(false);
    priority = ko.observable("");
    priorityText: KnockoutComputed<string>;
    editedIndex = ko.observable<indexDefinitionDto>();

    constructor() {
        super();

        this.priorityText = ko.computed(() => this.priority() ? "Priority: " + this.priority() : "Priority");
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

    fetchIndexToEdit(indexName: string) {
        
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

    createNewIndexDefinition(): indexDefinitionDto {
        return null;
    }
}

export = editIndex; 
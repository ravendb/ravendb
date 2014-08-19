import indexDefinition = require("models/indexDefinition");
import idGenerator = require("common/idGenerator");

class indexMergeSuggestion {
    canMerge: string[];
    collection: string;
    mergedIndexDefinition: indexDefinition;
    id = ko.observable<string>();
    mergedIndexUrl: KnockoutComputed<string>;

    constructor(dto: suggestionDto) {
        this.canMerge = dto.CanMerge;
        this.collection = "123";//dto.Collection;
        this.mergedIndexDefinition = new indexDefinition(dto.MergedIndex);
        this.id(idGenerator.generateId());
    }

    toDto(){
        return {
            CanMerge: this.canMerge, 
            Collection: this.collection,
            MergedIndex: this.mergedIndexDefinition.toDto()
        }
    }
}

export = indexMergeSuggestion;
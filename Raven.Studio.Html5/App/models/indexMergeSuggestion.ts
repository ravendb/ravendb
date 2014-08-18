import indexDefinition = require("models/indexDefinition");

class indexMergeSuggestion {
    canMerge: string[];
    collection: string;
    mergedIndexDefinition: indexDefinition;

    constructor(dto: suggestionDto) {
        this.canMerge = dto.CanMerge;
        this.collection = "123";//dto.Collection;
        this.mergedIndexDefinition = new indexDefinition(dto.MergedIndex);
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
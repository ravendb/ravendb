import indexDefinition = require("models/database/index/indexDefinition");
import idGenerator = require("common/idGenerator");

class indexMergeSuggestion {
    canMerge: string[];
    collection: string;
    mergedIndexDefinition: indexDefinition;
    canDelete: string[];
    surpassingIndex: string;
    id = ko.observable<string>();
    mergedIndexUrl: KnockoutComputed<string>;

    constructor(dto: any) { //TODO: use server side type
        this.canMerge = dto.CanMerge;
        this.collection = dto.Collection;
        this.mergedIndexDefinition = dto.MergedIndex ? new indexDefinition(dto.MergedIndex) : null;
        this.id(idGenerator.generateId());
        this.canDelete = dto.CanDelete;
        this.surpassingIndex = dto.SurpassingIndex;
    }

    public isSurpassingIndex():boolean {
        return this.mergedIndexDefinition == null;
    }

    toDto(){
        return {
            CanMerge: this.canMerge, 
            Collection: this.collection,
            MergedIndex: this.mergedIndexDefinition ? this.mergedIndexDefinition.toDto() : null,
            CanDelete: this.canDelete,
            SurpassingIndex: this.surpassingIndex,
        }
    }
}

export = indexMergeSuggestion;

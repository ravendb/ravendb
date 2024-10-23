/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");
import { th } from "cronstrue/dist/i18n/locales/th";

class vectorOptions {
    dimensions = ko.observable<number>();
    indexingStrategy = ko.observable<Raven.Client.Documents.Indexes.Vector.VectorIndexingStrategy>();
    sourceEmbeddingType = ko.observable<Raven.Client.Documents.Indexes.Vector.EmbeddingType>();
    destinationEmbeddingType = ko.observable<Raven.Client.Documents.Indexes.Vector.EmbeddingType>();

    constructor(dto: Raven.Client.Documents.Indexes.Vector.VectorOptions) {
        this.dimensions(dto.Dimensions);
        this.indexingStrategy(dto.IndexingStrategy);
        this.sourceEmbeddingType(dto.SourceEmbeddingType);
        this.destinationEmbeddingType(dto.DestinationEmbeddingType);
    }
    
    toDto() : Raven.Client.Documents.Indexes.Vector.VectorOptions {
        return {
            Dimensions: this.dimensions(),
            IndexingStrategy: this.indexingStrategy(),
            SourceEmbeddingType: this.sourceEmbeddingType(),
            DestinationEmbeddingType: this.destinationEmbeddingType()
        };
    }
    
    static empty() : vectorOptions {
        const dto : Raven.Client.Documents.Indexes.Vector.VectorOptions = {
            Dimensions: undefined,
            SourceEmbeddingType: "Float32",
            DestinationEmbeddingType: "Float32",
            IndexingStrategy: "Exact"
        };
        
        return new vectorOptions(dto);
    }
}
export = vectorOptions; 

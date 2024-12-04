/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");
import { th } from "cronstrue/dist/i18n/locales/th";

class vectorOptions {
    dimensions = ko.observable<number>();
    sourceEmbeddingType = ko.observable<Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType>();
    destinationEmbeddingType = ko.observable<Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType>();
    numberOfCandidatesForIndexing = ko.observable<number>();
    numberOfEdges = ko.observable<number>();
    
    constructor(dto: Raven.Client.Documents.Indexes.Vector.VectorOptions) {
        this.dimensions(dto.Dimensions);
        this.sourceEmbeddingType(dto.SourceEmbeddingType);
        this.destinationEmbeddingType(dto.DestinationEmbeddingType);
        this.numberOfCandidatesForIndexing(dto.NumberOfCandidatesForIndexing);
        this.numberOfEdges(dto.NumberOfEdges);
    }
    
    toDto() : Raven.Client.Documents.Indexes.Vector.VectorOptions {
        return {
            Dimensions: this.dimensions(),
            SourceEmbeddingType: this.sourceEmbeddingType(),
            DestinationEmbeddingType: this.destinationEmbeddingType(),
            NumberOfCandidatesForIndexing: this.numberOfCandidatesForIndexing(),
            NumberOfEdges: this.numberOfEdges()
        };
    }
    
    static empty() : vectorOptions {
        const dto : Raven.Client.Documents.Indexes.Vector.VectorOptions = {
            Dimensions: undefined,
            SourceEmbeddingType: "Single",
            DestinationEmbeddingType: "Single",
            NumberOfCandidatesForIndexing: 16,
            NumberOfEdges: 8
        };
        
        return new vectorOptions(dto);
    }
}
export = vectorOptions; 

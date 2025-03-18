/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");
import common = require("components/utils/common");

type VectorEmbeddingTypeWithoutText = Exclude<Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType, "Text">;

class vectorOptions {
    dimensions = ko.observable<number>();
    sourceEmbeddingType = ko.observable<Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType>();
    destinationEmbeddingType = ko.observable<VectorEmbeddingTypeWithoutText>();
    numberOfCandidatesForIndexing = ko.observable<number>();
    numberOfEdges = ko.observable<number>();

    sourceEmbeddingTypes = common.exhaustiveStringTuple<Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType>()(
        "Single", "Int8", "Text", "Binary"
    )

    destinationEmbeddingTypes = common.exhaustiveStringTuple<VectorEmbeddingTypeWithoutText>()(
        "Single", "Int8", "Binary"
    )

    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Indexes.Vector.VectorOptions) {
        this.dimensions(dto.Dimensions);
        this.sourceEmbeddingType(dto.SourceEmbeddingType);
        this.destinationEmbeddingType(dto.DestinationEmbeddingType as VectorEmbeddingTypeWithoutText);
        this.numberOfCandidatesForIndexing(dto.NumberOfCandidatesForIndexing);
        this.numberOfEdges(dto.NumberOfEdges);

        this.initValidation()
        this.initObservables()

        this.dirtyFlag = new ko.DirtyFlag([
            this.dimensions,
            this.sourceEmbeddingType,
            this.destinationEmbeddingType,
            this.numberOfCandidatesForIndexing,
            this.numberOfEdges,
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    toDto(): Raven.Client.Documents.Indexes.Vector.VectorOptions {
        return {
            Dimensions: this.dimensions(),
            SourceEmbeddingType: this.sourceEmbeddingType(),
            DestinationEmbeddingType: this.destinationEmbeddingType(),
            NumberOfCandidatesForIndexing: this.numberOfCandidatesForIndexing(),
            NumberOfEdges: this.numberOfEdges()
        };
    }

    private static getDefaultDto(): Raven.Client.Documents.Indexes.Vector.VectorOptions {
        return {
            Dimensions: null,
            SourceEmbeddingType: "Single",
            DestinationEmbeddingType: "Single",
            NumberOfCandidatesForIndexing: null,
            NumberOfEdges: null
        }
    }
    
    static empty(): vectorOptions {
        return new vectorOptions(this.getDefaultDto());
    }

    private initObservables() {
        this.sourceEmbeddingType.subscribe((value) => {
            if (value === "Int8" || value === "Binary") {
                this.destinationEmbeddingType(value);
            }
        });
    }

    private initValidation() {
        this.destinationEmbeddingType.extend({
            required: true,
            validation: [
                {
                    validator: (destValue: string) => {
                        const srcValue = this.sourceEmbeddingType();
                        const allowedMappings = {
                            "Text": ["Single", "Int8", "Binary"],
                            "Single": ["Single", "Int8", "Binary"],
                            "Int8": ["Int8"],
                            "Binary": ["Binary"],
                        };
                        if (!srcValue) {
                            return true;
                        }
                        return allowedMappings[srcValue] && allowedMappings[srcValue].indexOf(destValue) !== -1;
                    },
                    message: "Destination embedding type is invalid for the selected source embedding type.",
                },
            ],
        });

        this.sourceEmbeddingType.extend({
            required: true,
            validation: [
                {
                    validator: () => !(this.sourceEmbeddingType() === "Text" && this.dimensions() !== null),
                    message: "Dimensions are set internally by the embedder."
                }
            ]
        });

        this.dimensions.extend({
            min: {
                params: 0,
                message: "Number of vector dimensions has to be positive."
            },
        });

        this.numberOfEdges.extend({
            min: {
                params: 0,
                message: "Number of edges has to be positive."
            },
        });

        this.numberOfCandidatesForIndexing.extend({
            min: {
                params: 0,
                message: "Number of candidate nodes has to be positive."
            }
        });
    }
}
export = vectorOptions; 

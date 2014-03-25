class facet {
    mode: number; // Default = 0, Ranges = 1
    aggregation = ko.observable<number>(); // None = 0, Count = 1, Max = 2, Min = 4, Average = 8, Sum = 16
    aggregationField: string;
    aggregationType: string;
    name: string;
    displayName: string;
    ranges: any[];
    maxResults: number;
    termSortMode: number;
    includeRemainingTerms: boolean;

    aggregationLabel = ko.computed(() => facet.getLabelForAggregation(this.aggregation()));

    constructor(dto: facetDto) {
        this.aggregation(dto.Aggregation);
        this.aggregationField = dto.AggregationField;
        this.aggregationType = dto.AggregationType;
        this.displayName = dto.DisplayName;
        this.includeRemainingTerms = dto.IncludeRemainingTerms;
        this.maxResults = dto.MaxResults;
        this.mode = dto.Mode;
        this.name = dto.Name;
        this.ranges = dto.Ranges;
        this.termSortMode = dto.TermSortMode;
    }

    toDto(): facetDto {
        return {
            Aggregation: this.aggregation(),
            AggregationField: this.aggregationField,
            AggregationType: "System.Int32",
            DisplayName: this.displayName,
            IncludeRemainingTerms: this.includeRemainingTerms,
            MaxResults: this.maxResults,
            Mode: this.mode,
            Name: this.name,
            Ranges: this.ranges,
            TermSortMode: this.termSortMode
        };
    }

    static fromNameAndAggregation(name: string, aggregationField: string): facet {
        var dto: facetDto = {
            Aggregation: 0,
            AggregationField: aggregationField,
            AggregationType: "System.Int32",
            DisplayName: name + "-" + aggregationField,
            IncludeRemainingTerms: false,
            MaxResults: null,
            Mode: 0,
            Name: name,
            Ranges: [],
            TermSortMode: 0
        };

        return new facet(dto);
    }

    static getLabelForAggregation(aggregation: number) {
        // None = 0, Count = 1, Max = 2, Min = 4, Average = 8, Sum = 16
        return aggregation === 1 ? "Count" :
            aggregation === 2 ? "Max" :
            aggregation === 4 ? "Min" :
            aggregation === 8 ? "Average" :
            aggregation === 16 ? "Sum" :
            "None";
    }

    setAggregationToCount() {
        this.aggregation(1);
    }

    setAggregationToMax() {
        this.aggregation(2);
    }

    setAggregationToMin() {
        this.aggregation(4);
    }

    setAggregationToAverage() {
        this.aggregation(8);
    }

    setAggregationToSum() {
        this.aggregation(16);
    }
}

export = facet;
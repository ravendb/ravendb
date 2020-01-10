
class timeSeriesQueryResult {
    constructor(private dto: timeSeriesQueryResultDto) {
    }
    
    getCount() {
        return this.dto.Count;
    }
    
    getDateRange(): [string, string] {
        if (this.dto.Results.length === 0) {
            return [null, null];
        }
        
        switch (this.detectResultType()) {
            case "grouped":
                const groupedResults = this.dto.Results as Array<timeSeriesQueryGroupedItemResultDto>;
                return [groupedResults[0].From, groupedResults[groupedResults.length - 1].To];
            case "raw":
                const rawResults = this.dto.Results as Array<timeSeriesRawItemResultDto>;
                return [rawResults[0].Timestamp, rawResults[rawResults.length - 1].Timestamp];
        }
    }
    
    detectResultType(): timeSeriesResultType {
        const results = this.dto.Results;
        if (results.length === 0) {
            return "raw"; //we guess but list is empty
        }
        
        const firstResult = results[0] as timeSeriesQueryGroupedItemResultDto;
        return firstResult.From && firstResult.To ? "grouped" : "raw";
    }
}


export = timeSeriesQueryResult;

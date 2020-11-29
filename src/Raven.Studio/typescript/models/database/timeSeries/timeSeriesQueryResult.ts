/// <reference path="../../../../typings/tsd.d.ts" />

class timeSeriesQueryResult {
    
    queryHasGroupByTag: boolean;
    
    constructor(private dto: timeSeriesQueryResultDto) {

        if (timeSeriesQueryResult.detectResultType(dto) === "grouped") {
            const groupedResults = dto.Results as Array<timeSeriesQueryGroupedItemResultDto>;
            if (groupedResults.length) {
                this.queryHasGroupByTag = !!groupedResults[0].Key;
            }
        }
    }
    
    getCount() {
        return this.dto.Count;
    }

    getBucketCount() {
        return this.dto.Results.length;
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
        return timeSeriesQueryResult.detectResultType(this.dto);
    }
    
    static detectResultType(dto: timeSeriesQueryResultDto): timeSeriesResultType {
        const results = dto.Results;
        if (results.length === 0) {
            return "raw"; //we guess but list is empty
        }
        
        const firstResult = results[0] as timeSeriesQueryGroupedItemResultDto;
        return firstResult.From && firstResult.To ? "grouped" : "raw";
    }
    
    static detectGroupKeys(groupedValues: Array<timeSeriesQueryGroupedItemResultDto>): string[] {
        const allKeys = Object.keys(groupedValues[0]);
        const keyWithOutRange = _.without(allKeys, "From", "To", "Key");
        // server added Count property every time, so we filter it out, unless only Count is available in result
        if (keyWithOutRange.length === 1 && keyWithOutRange[0] === "Count") {
            return ["Count"];
        }
        return _.without(keyWithOutRange, "Count"); 
    }
    
    static detectValuesCount(dto: timeSeriesQueryResultDto): number {
        switch (timeSeriesQueryResult.detectResultType(dto)) {
            case "grouped":
                const groupedValues = dto.Results as Array<timeSeriesQueryGroupedItemResultDto>;
                const keys = timeSeriesQueryResult.detectGroupKeys(groupedValues);
                if (keys.length) {
                    const firstKey = keys[0];
                    return _.max(groupedValues.map(x => x[firstKey].length));
                } else {
                    return 0;
                }
            case "raw":
                const rawValues = dto.Results as Array<timeSeriesRawItemResultDto>;
                return _.max(rawValues.map(x => x.Values.length));
        }
    }
}


export = timeSeriesQueryResult;

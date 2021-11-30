/// <reference path="../../typings/tsd.d.ts" />

interface rqlTokensIndexInfo {
    update?: RegExpExecArray,
    where?: RegExpExecArray,
    load?: RegExpExecArray,
    orderby?: RegExpExecArray,
    select?: RegExpExecArray,
    include?: RegExpExecArray
}

class queryUtil {

    static readonly AutoPrefix = "auto/";
    static readonly DynamicPrefix = "collection/";
    static readonly AllDocs = "AllDocs";
    static readonly MinDateUTC = "0001-01-01T00:00:00.000Z"; // TODO - replace w/ syntax from RavenDB-17618 when done
    static readonly MaxDateUTC = "9999-01-01T00:00:00.000Z";

    static formatRawTimeSeriesQuery(collectionName: string, documentId: string, timeSeriesName: string, startDate?: string, endDate?: string) {
        const escapedCollectionName = queryUtil.wrapWithSingleQuotes(collectionName || "@all_docs");
        const escapedDocumentId = queryUtil.escapeName(documentId);
        const escapedTimeSeriesName = queryUtil.wrapWithSingleQuotes(timeSeriesName);
        const dates = queryUtil.formatDates(startDate, endDate);
        
        return `from ${escapedCollectionName}\r\nwhere id() == ${escapedDocumentId}\r\nselect timeseries(from ${escapedTimeSeriesName} ${dates})`;
    }

    static formatGroupedTimeSeriesQuery(collectionName: string, documentId: string, timeSeriesName: string, group: string, startDate?: string, endDate?: string) {
        const escapedCollectionName = queryUtil.wrapWithSingleQuotes(collectionName || "@all_docs");
        const escapedDocumentId = queryUtil.escapeName(documentId);
        const escapedTimeSeriesName = queryUtil.wrapWithSingleQuotes(timeSeriesName);
        const dates = queryUtil.formatDates(startDate, endDate);

        return `from ${escapedCollectionName}\r\nwhere id() == ${escapedDocumentId}\r\nselect timeseries(from ${escapedTimeSeriesName} ${dates} group by ${group} select avg())`;
    }
    
    private static formatDates(startDate?: string, endDate?: string): string {
        if (startDate && !endDate) {
            endDate = queryUtil.MaxDateUTC;
        } else if (endDate && !startDate) {
            startDate = queryUtil.MinDateUTC;
        }
        
        return startDate && endDate ? `between "${startDate}" and "${endDate}"` : "";
    }
    
    static formatIndexQuery(indexName: string, fieldName: string, value: string) {
        const escapedFieldName = queryUtil.wrapWithSingleQuotes(fieldName);
        const escapedIndexName = queryUtil.escapeName(indexName);
        return `from index ${escapedIndexName} where ${escapedFieldName} = '${value}' `;
    }
    
    static wrapWithSingleQuotes(input: string) {
        if (input.includes("'")) {
            input = input.replace(/'/g, "''");
        }
        return "'" + input + "'";
    }

    static escapeName(name: string) {
        return queryUtil.wrapWithSingleQuotes(name);
    }

    static escapeIndexName(indexName: string): string {
        indexName = indexName.replace(/"/g, '\\"');
        
        if (indexName.toLocaleLowerCase().startsWith(queryUtil.AutoPrefix) && indexName.includes("'")) {
            return `"${indexName}"`;
        }

        return `'${indexName}'`;
    }

    private static readonly RQL_TOKEN_REGEX = /(?=([^{]*{[^}{]*})*[^}]*$)(?=([^']*'[^']*')*[^']*$)(?=([^"]*"[^"]*")*[^"]*$)(SELECT|WHERE|ORDER BY|LOAD|UPDATE|INCLUDE)(\s+|{)/gi;

    static replaceSelectAndIncludeWithFetchAllStoredFields(query: string) {
        if (!query)
            throw new Error("Query is required.");

        const getStoredFieldsText = " select __all_stored_fields";
        const tokenIndexes = queryUtil.findTokenIndexes(query);

        if (tokenIndexes.select) {
            const selectIdx = tokenIndexes.select.index;
            return query.substring(0, selectIdx) + getStoredFieldsText;
            
        } else if (tokenIndexes.include) {
            // 'select' not found. Check for 'include' which can come after 'select'. 
            const includeIdx = tokenIndexes.include.index;
            return query.substring(0, includeIdx) + getStoredFieldsText;
        } else {
            // Both 'select' & 'include' not found. Append at end of query.
            return query + getStoredFieldsText;
        }
    }
    
    private static findTokenIndexes(query: string) {
        let tokenIndexes: rqlTokensIndexInfo = {};

        let match: RegExpExecArray;
        let keyword;
        try {
            while ((match = queryUtil.RQL_TOKEN_REGEX.exec(query)) !== null) {
                keyword = (match[4] || '').toLowerCase().replace(/\s/, '');
                (tokenIndexes as any)[keyword] = match;
            }
        } finally {
            queryUtil.RQL_TOKEN_REGEX.lastIndex = 0;
        }
        
        return tokenIndexes;
    }

    private static tokenizeQuery(query: string): Array<string> {
        return query
            .toLocaleLowerCase()
            .replace(/(\r\n|\n|\r|')/gm, ' ')
            .replace(/\s+/g, ' ')
            .trim()
            .split(" ");
    }
    
    static getCollectionOrIndexName(query: string): [string, "index" | "collection" | "unknown"] {
        const queryWoDeclare = queryUtil.trimDeclareSections(query);
        const words = queryUtil.tokenizeQuery(queryWoDeclare);

        for (let i = 0; i < words.length; i++) {
            if (words[i] === "from") {
                if (words[i + 1] === "index") {
                    return [words[i + 2], "index"];
                } else {
                    return [words[i + 1], "collection"];
                }
            }
        }
        
        return [undefined, "unknown"];
    }
    
    private static trimDeclareSections(query: string): string {
        if (!query.toLocaleLowerCase().includes("declare")) {
            return query;
        }
        
        const rangesToDelete: Array<[number, number]> = [];
        
        const queryLowered = query.toLocaleLowerCase();
        
        let index = 0;
        
        do {
            const declareIndex = queryLowered.indexOf("declare ", index);
            if (declareIndex === -1) {
                break;
            }
            
            const openBracketPosition = queryLowered.indexOf("{", declareIndex + 1);
            if (openBracketPosition === -1) {
                break;
            }
            
            let openBracketsCounter = 1;
            
            for (index = openBracketPosition + 1; index < query.length; index++) {
                const char = query.charAt(index);
                if (char === '{') {
                    openBracketsCounter++;
                } else if (char === "}") {
                    openBracketsCounter--;
                    if (openBracketsCounter === 0) {
                        rangesToDelete.push([declareIndex, index]);
                        break;
                    }
                }
            }
        } while (true);

        const rangesReversed = rangesToDelete.reverse();
        for (let i = 0; i < rangesReversed.length; i++) {
            const range = rangesReversed[i];
            query = query.substring(0, range[0]) + query.substring(range[1] + 1);
        }
        
        return query;
    }

    static isDynamicQuery(query: string): boolean {
        const words = queryUtil.tokenizeQuery(query);

        for (let i = 0; i < words.length; i++) {
            if (words[i] === "from" && words[i + 1] === "index") {
                return false;
            }
        }
        
        return true;
    }
    
}

export = queryUtil;

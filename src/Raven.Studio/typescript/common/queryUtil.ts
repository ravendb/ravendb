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

    static formatRawTimeSeriesQuery(collectionName: string, documentId: string, timeSeriesName: string) {
        const escapedCollectionName = queryUtil.escapeCollectionOrFieldName(collectionName || "@all_docs");
        const escapedDocumentId = queryUtil.escapeDocumentId(documentId);
        const escapedTimeSeriesName = queryUtil.escapeTimeSeriesName(timeSeriesName);
        
        return `from ${escapedCollectionName}\r\nwhere id() == ${escapedDocumentId}\r\nselect timeseries(from ${escapedTimeSeriesName})`;
    }

    static formatGroupedTimeSeriesQuery(collectionName: string, documentId: string, timeSeriesName: string, group: string) {
        const escapedCollectionName = queryUtil.escapeCollectionOrFieldName(collectionName || "@all_docs");
        const escapedDocumentId = queryUtil.escapeDocumentId(documentId);
        const escapedTimeSeriesName = queryUtil.escapeTimeSeriesName(timeSeriesName);

        return `from ${escapedCollectionName}\r\nwhere id() == ${escapedDocumentId}\r\nselect timeseries(from ${escapedTimeSeriesName} group by ${group} select avg())`;
    }
    
    static formatIndexQuery(indexName: string, fieldName: string, value: string) {
        const escapedFieldName = queryUtil.escapeCollectionOrFieldName(fieldName);
        return `from index ${indexName} where ${escapedFieldName} = '${value}' `;
    }
    
    private static wrapWithSingleQuotes(input: string) {
        if (input.includes("'")) {
            input = input.replace("'", "''");
        }
        return "'" + input + "'";
    }

    static escapeDocumentId(name: string) {
        return queryUtil.wrapWithSingleQuotes(name);
    }
    
    static escapeCollectionOrFieldName(name: string) : string {
        // wrap collection name in 'collection name' if it has spaces.
        // @ is allowed (ex. @all_docs) - but only in front
        if (/^@?[0-9a-zA-Z_]+$/.test(name)) {
            return name;
        }

        return queryUtil.wrapWithSingleQuotes(name);
    }
    
    static escapeTimeSeriesName(name: string): string {
        // wrap collection name in 'collection name' if it has spaces.
        if (/^[0-9a-zA-Z_]+$/.test(name)) {
            return name;
        }

        return queryUtil.wrapWithSingleQuotes(name);
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

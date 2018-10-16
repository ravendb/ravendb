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

    static formatIndexQuery(indexName: string, ...predicates: { name?: string, value?: string }[]) {
        let query = `from index '${indexName}'`;
        if (predicates && predicates.length) {
            query = predicates.reduce((result, field) => {
                return `${result} where ${field.name} = '${field.value}'`;
            }, query);
        }

        return query;
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
    
    static getCollectionOrIndexName(query: string): string {
        const words = queryUtil.tokenizeQuery(query);

        for (let i = 0; i < words.length; i++) {
            if (words[i] === "from") {
                if (words[i + 1] === "index") {
                    return words[i + 2];
                } else {
                    return words[i + 1];
                }
            }
        }
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

/// <reference path="../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import getIndexEntriesFieldsCommand = require("commands/database/index/getIndexEntriesFieldsCommand");
import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");


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

    /**
     * Escapes lucene single term
     * 
     * Note: Do not use this method for escaping entire query unless you want to end up with: query\:value\ AND\ a\:b
     * @param term term to escape
     */
    static escapeTerm(term: string) {
        let output = "";

        for (let i = 0; i < term.length; i++) {
            const c = term.charAt(i);
            if (c === '\\' || c === '+' || c === '-' || c === '!' || c === '(' || c === ')'
                || c === ':' || c === '^' || c === '[' || c === ']' || c === '\"'
                || c === '{' || c === '}' || c === '~' || c === '*' || c === '?'
                || c === '|' || c === '&' || c === ' ') {
                output += "\\";
            }
            output += c;
        }

        return output;
    }

    static fetchIndexFields(db: database, indexName: string, outputFields: KnockoutObservableArray<string>): void {
        outputFields([]);

        // Fetch the index definition so that we get an updated list of fields to be used as sort by options.
        // Fields don't show for All Documents.
        const isAllDocumentsDynamicQuery = indexName === this.AllDocs;
        if (!isAllDocumentsDynamicQuery) {

            //if index is not dynamic, get columns using index definition, else get it using first index result
            if (indexName.startsWith(queryUtil.DynamicPrefix)) {
                new collection(indexName.substr(queryUtil.DynamicPrefix.length), db)
                    .fetchDocuments(0, 1)
                    .done(result => {
                        if (result && result.items.length > 0) {
                            const propertyNames = new document(result.items[0]).getDocumentPropertyNames();
                            outputFields(propertyNames);
                        }
                    });
            } else {
                new getIndexEntriesFieldsCommand(indexName, db)
                    .execute()
                    .done((fields) => {
                        outputFields(fields.Results);
                    });
            }
        }
    }

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
    
    static getCollectionOrIndexName(query: string): [string, "index" | "collection"] {
        const words = queryUtil.tokenizeQuery(query);

        for (let i = 0; i < words.length; i++) {
            if (words[i] === "from") {
                if (words[i + 1] === "index") {
                    return [words[i + 2], "index"];
                } else {
                    return [words[i + 1], "collection"];
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

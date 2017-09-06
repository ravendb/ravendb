/// <reference path="../../typings/tsd.d.ts" />

import getIndexTermsCommand = require("commands/database/index/getIndexTermsCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import database = require("models/resources/database");
import getIndexEntriesFieldsCommand = require("commands/database/index/getIndexEntriesFieldsCommand");
import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");


interface rqlTokensIndexInfo {
    update?: RegExpExecArray,
    where?: RegExpExecArray,
    load?: RegExpExecArray,
    orderby?: RegExpExecArray,
    select?: RegExpExecArray
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
                        //TODO: self.isTestIndex(result.IsTestIndex);
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

    private static readonly RQL_TOKEN_REGEX = /(?=([^{]*{[^}{]*})*[^}]*$)(?=([^']*'[^']*')*[^']*$)(?=([^"]*"[^"]*")*[^"]*$)(SELECT|WHERE|ORDER BY|LOAD|UPDATE)(\s+|{)/gi;

    private static readonly RQL_TOKEN_ORDER = [
        'where', 'load', 'orderby', 'update'
    ];

    static replaceSelectWithFetchAllStoredFields(query: string) {
        if (!query)
            throw new Error("Query is required.");

        const tokenIndexes = queryUtil.findTokenIndexes(query);
        if (tokenIndexes.select) {
            const selectIdx = tokenIndexes.select.index;
            
            return query.substring(0, selectIdx) + " select __all_stored_fields";
        } else {
            // select statement wasn't found append at the end of query
            return query + " select __all_stored_fields";
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
    
    static replaceWhereWithDocumentIdPredicate(query: string, documentId: string) {
        if (!query)
            throw new Error("Query is required.");

        if (!documentId)
            throw new Error("Document ID is required.");

        const tokenIndexes = queryUtil.findTokenIndexes(query);

        const { where, update, load, orderby } = tokenIndexes;

        let startToken;
        if (where) {
            startToken = where;

            let endToken = queryUtil.RQL_TOKEN_ORDER
                .filter(x => x !== 'where')
                .filter(token => (tokenIndexes as any)[token])
                .map(x => (tokenIndexes as any)[x])[0] as RegExpExecArray;

            let whereStartIndex = where.index;
            let whereEndIndex = endToken ? endToken.index : query.length; 
            const qstart = query.substring(0, whereStartIndex).trim();
            const qend = query.substring(whereEndIndex, query.length).trim();
            return `${qstart} where id() = '${documentId}' ${qend}`.trim();
        }

        startToken = queryUtil.RQL_TOKEN_ORDER
            .filter(token => (tokenIndexes as any)[token])
            .map(x => (tokenIndexes as any)[x])[0] as RegExpExecArray;
        if (!startToken) {
            return `${query} where id() = '${documentId}'`;
        }

        const qstart = query.substring(0, startToken.index).trim() ;
        const qend = query.substring(startToken.index, query.length).trim();
        return `${qstart} where id() = '${documentId}' ${qend}`;
    }
}

export = queryUtil;

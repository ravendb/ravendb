/// <reference path="../../typings/tsd.d.ts" />

import getIndexTermsCommand = require("commands/database/index/getIndexTermsCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import database = require("models/resources/database");
import getIndexEntriesFieldsCommand = require("commands/database/index/getIndexEntriesFieldsCommand");
import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");

class queryUtil {

    static readonly AutoPrefix = "auto/";
    static readonly DynamicPrefix = "collection/";
    static readonly AllDocs = "AllDocs";

    /**
     * Escapes lucene single term
     * 
     * Note: Do not use this method for escaping entire query unless you want to end up with: query\:value\ AND\ a\:b
     * @param query query to escape
     */
    static escapeTerm(term: string) {
        var output = "";

        for (var i = 0; i < term.length; i++) {
            var c = term.charAt(i);
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
}

export = queryUtil;

/// <reference path="../../typings/tsd.d.ts" />

import getIndexTermsCommand = require("commands/database/index/getIndexTermsCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import database = require("models/resources/database");
import getIndexEntriesFieldsCommand = require("commands/database/index/getIndexEntriesFieldsCommand");
import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");

class queryUtil {

    static readonly DynamicPrefix = "dynamic/";

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
        const isAllDocumentsDynamicQuery = indexName === "All Documents" || indexName === "dynamic";
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

    static queryCompleter(indexFields: KnockoutObservableArray<string>, selectedIndex: KnockoutObservable<string>, activeDatabase: KnockoutObservable<database>, editor: any, session: any, pos: AceAjax.Position, prefix: string, callback: (errors: any[], worldlist: { name: string; value: string; score: number; meta: string }[]) => void) {
        const currentToken: AceAjax.TokenInfo = session.getTokenAt(pos.row, pos.column);
        if (!currentToken || typeof currentToken.type === "string") {
            // if in beginning of text or in free text token
            if (!currentToken || currentToken.type === "text") {
                callback(null, indexFields().map(curColumn => {
                    return { name: curColumn, value: curColumn, score: 10, meta: "field" };
                }));
            } else if (currentToken.type === "keyword" || currentToken.type === "value") {
                // if right after, or a whitespace after keyword token ([column name]:)

                // first, calculate and validate the column name
                let currentColumnName: string = null;
                let currentValue: string = "";

                if (currentToken.type == "keyword") {
                    currentColumnName = currentToken.value.substring(0, currentToken.value.length - 1);
                } else {
                    currentValue = currentToken.value.trim();
                    const rowTokens: any[] = session.getTokens(pos.row);
                    if (!!rowTokens && rowTokens.length > 1) {
                        currentColumnName = rowTokens[rowTokens.length - 2].value.trim();
                        currentColumnName = currentColumnName.substring(0, currentColumnName.length - 1);
                    }
                }

                // for non dynamic indexes query index terms, for dynamic indexes, try perform general auto complete
                if (currentColumnName && indexFields().find(x => x === currentColumnName)) {

                    if (!selectedIndex().startsWith(queryUtil.DynamicPrefix)) {
                        new getIndexTermsCommand(selectedIndex(), currentColumnName, activeDatabase(), 20)
                            .execute()
                            .done(terms => {
                                if (terms && terms.Terms.length > 0) {
                                    callback(null, terms.Terms.map(curVal => {
                                        return { name: curVal, value: curVal, score: 10, meta: "value" };
                                    }));
                                }
                            });
                    } else {
                        if (currentValue.length > 0) {
                            new getDocumentsMetadataByIDPrefixCommand(currentValue, 10, activeDatabase())
                                .execute()
                                .done((results: metadataAwareDto[]) => {
                                    if (results && results.length > 0) {
                                        callback(null, results.map(curVal => {
                                            return { name: curVal["@metadata"]["@id"], value: curVal["@metadata"]["@id"], score: 10, meta: "value" };
                                        }));
                                    }
                                });
                        } else {
                            callback([{ error: "notext" }], null);
                        }
                    }
                }
            }
        }
    }
    
}

export = queryUtil;

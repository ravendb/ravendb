/// <reference path="../../typings/tsd.d.ts" />

import collectionsTracker = require("common/helpers/database/collectionsTracker");
import getIndexTermsCommand = require("commands/database/index/getIndexTermsCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import database = require("models/resources/database");
import getIndexEntriesFieldsCommand = require("commands/database/index/getIndexEntriesFieldsCommand");
import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");

class queryCompleter {
    private collectionsTracker: collectionsTracker;
    private indexFieldsCache = new Map<string, string[]>();
    
    constructor(private activeDatabase: KnockoutObservable<database>,
                private indexes: KnockoutObservableArray<Raven.Client.Documents.Operations.IndexInformation>) {
        this.collectionsTracker = collectionsTracker.default;
    }
    
    private getIndexName(session: AceAjax.IEditSession): [string, boolean] {
        let keyword: string;
        
        for (let row = 0; row < session.getLength(); row++) {
            let lineTokens: AceAjax.TokenInfo[] = session.getTokens(row);

            for (let i = 0; i < lineTokens.length; i++) {
                const token = lineTokens[i];
                switch (token.type) {
                    case "keyword": {
                        keyword = token.value.toLowerCase();
                        break;
                    }
                    case "string": {
                        const indexName = token.value.substr(1, token.value.length - 2);
                        if (keyword === "from")
                            return [indexName, false];
                        if (keyword === "index")
                            return [indexName, true];
                        break;
                    }
                    case "identifier": {
                        const indexName = token.value;
                        if (keyword === "from")
                            return [token.value, false];
                        if (keyword === "index")
                            return [indexName, true];
                        break;
                    }
                }
            }
        }
    }

    private getIndexFields(session: AceAjax.IEditSession): JQueryPromise<string[]> {
        
        const [indexName, isStaticIndex] = this.getIndexName(session);
        if (!indexName) {
            return $.when<string[]>([]);
        }

        const cache = this.indexFieldsCache.get(indexName);
        if (cache) {
            return $.when<string[]>(cache);
        }

        if (isStaticIndex) {
            return new getIndexEntriesFieldsCommand(indexName, this.activeDatabase())
                .execute()
                .then((fields) => {
                    this.indexFieldsCache.set(indexName, fields.Results);
                    return $.when(fields.Results);
                });
        } else {
            return new collection(indexName, this.activeDatabase())
                .fetchDocuments(0, 1)
                .then(result => {
                    // TODO: Modify the command to return also nested pathes, like Address.City
                    if (result && result.items.length > 0) {
                        const propertyNames = new document(result.items[0]).getDocumentPropertyNames();
                        this.indexFieldsCache.set(indexName, propertyNames);
                        return $.when(propertyNames);
                    }
                    return $.when<string[]>([]);
                });
        }
    }

    private getLastKeyword(session: AceAjax.IEditSession, pos: AceAjax.Position): [string, string, string, string] {

        // let tokensAfterKeyword: AceAjax.TokenInfo[] = []; // TODO: Can be deleted
        let identifier: string;
        let text: string;
        let operator: string;
        for (let row = 0; row <= pos.row; row++) {
            let lineTokens: AceAjax.TokenInfo[] = session.getTokens(pos.row - row);

            for (let i = lineTokens.length - 1; i >= 0; i--) {
                const token = lineTokens[i];
                switch (token.type) {
                    case "keyword":
                        let keyword = token.value.toLowerCase();
                        if (keyword === "desc" ||
                            keyword === "asc" ||
                            keyword === "and" ||
                            keyword === "or")
                            continue;

                        return [keyword, operator, identifier, text];

                    case "keyword.operator":
                        operator = token.value;
                        break;
                    case "identifier":
                        identifier = token.value;
                        break;
                    case "string":
                        const indexName = token.value.substr(1, token.value.length - 2);
                        identifier = indexName;
                        break;
                    case "text":
                        if  (!identifier) {
                            text = token.value;
                        }
                        break;
                    default:
                        // tokensAfterKeyword.push(token);
                        break;
                }
            }
        }
        
        return [null, null, null, null];
    }

    complete(editor: AceAjax.Editor,
             session: AceAjax.IEditSession,
             pos: AceAjax.Position,
             prefix: string,
             callback: (errors: any[], worldlist: autoCompleteWordList[]) => void) {

        let currentToken: AceAjax.TokenInfo = session.getTokenAt(pos.row, pos.column);
        // If token is space, use the previous token
        if (currentToken && currentToken.start > 0 && /^\s+$/g.test(currentToken.value)) {
            currentToken = session.getTokenAt(pos.row, currentToken.start - 1);
        }

        const [lastKeyword, operator, identifier, text] = this.getLastKeyword(session, pos);
        if (!lastKeyword)
            return;

        switch (lastKeyword) {
            case "from": {
                 if (identifier && text) { // collection name already specified
                     return;
                 }

                if (!prefix ||
                    prefix.length === 0 ||
                    prefix.startsWith("@")) {
                    callback(null, [{name: "@all_docs", value: "@all_docs", score: 100, meta: "collection"}]);
                    callback(null, [{name: "@system", value: "@system", score: 9, meta: "collection"}]);
                }
                callback(null, this.collectionsTracker.getCollectionNames().map(collection => {
                    return {
                        name: collection,
                        value: collection,
                        score: 10,
                        meta: "collection"
                    };
                }));
                break;
            }
            case "index": {
                if (identifier && text) { // index name already specified
                    return;
                }
                
                callback(null,
                    this.indexes().map(index => {
                        const name = `'${index.Name}'`;
                        return {name: name, value: name, score: 10, meta: "index"};
                    }));
                break;
            }
            case "select":
            case "by": // group by, order by
            case "where": {
                if (operator === "=") {
                    // first, calculate and validate the column name
                    let currentField = identifier;
                    if (!currentField) {
                        return;
                    }

                    this.getIndexFields(session)
                        .done((indexFields) => {
                            if (!indexFields.find(x => x === currentField)) {
                                return;
                            }

                            let currentValue: string = "";

                            /*currentValue = currentToken.value.trim();
                             const rowTokens: any[] = session.getTokens(pos.row);
                             if (!!rowTokens && rowTokens.length > 1) {
                             currentColumnName = rowTokens[rowTokens.length - 2].value.trim();
                             currentColumnName = currentColumnName.substring(0, currentColumnName.length - 1);
                             }*/


                            // for non dynamic indexes query index terms, for dynamic indexes, try perform general auto complete
                            const [indexName, isStaticIndex] = this.getIndexName(session);
                            if (!indexName)
                                return; // todo: try to callback with error

                            if (isStaticIndex) {
                                new getIndexTermsCommand(indexName, currentField, this.activeDatabase(), 20)
                                    .execute()
                                    .done(terms => {
                                        if (terms && terms.Terms.length > 0) {
                                            callback(null,
                                                terms.Terms.map(term => {
                                                    term = "'" + term + "'";
                                                    return {name: term, value: term, score: 10, meta: "value"};
                                                }));
                                        }
                                    });
                            } else {
                                if (currentValue.length > 0) {
                                    // TODO: Not sure what we want to show here?
                                    new getDocumentsMetadataByIDPrefixCommand(currentValue, 10, this.activeDatabase())
                                        .execute()
                                        .done((results: metadataAwareDto[]) => {
                                            if (results && results.length > 0) {
                                                callback(null,
                                                    results.map(curVal => {
                                                        const id = "'" + curVal["@metadata"]["@id"] + "'";
                                                        return {
                                                            name: id,
                                                            value: id,
                                                            score: 10,
                                                            meta: "value"
                                                        };
                                                    }));
                                            }
                                        });
                                } else {
                                    callback([{error: "notext"}], null);
                                }
                            }
                        });
                    return;
                }
                
                this.getIndexFields(session)
                    .done((indexFields) => callback(null, indexFields.map(field => {
                        return {name: field, value: field, score: 10, meta: "field"};
                    })));
                break;
            }
        }
    }
}

export = queryCompleter;

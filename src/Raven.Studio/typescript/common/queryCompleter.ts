/// <reference path="../../typings/tsd.d.ts" />

import collectionsTracker = require("common/helpers/database/collectionsTracker");
import getIndexTermsCommand = require("commands/database/index/getIndexTermsCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import database = require("models/resources/database");
import getIndexEntriesFieldsCommand = require("commands/database/index/getIndexEntriesFieldsCommand");
import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");

interface AutoCompleteLastKeyword {
    keyword: string,
    keywordModifier: string,
    operator: string,
    identifier: string,
    text: string,
    paren: number,
}

class queryCompleter {
    private tokenIterator: new(session : AceAjax.IEditSession, initialRow: number, initialColumn: number) => AceAjax.TokenIterator = ace.require("ace/token_iterator").TokenIterator;
    private collectionsTracker: collectionsTracker;
    private indexFieldsCache = new Map<string, string[]>();
    private defaultScore = 10000;
    
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
        
        return [null, null];
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

    private getLastKeyword(session: AceAjax.IEditSession, pos: AceAjax.Position): AutoCompleteLastKeyword {
        let keyword: string;
        let keywordModifier: string;
        let identifier: string;
        let text: string;
        let operator: string;
        let paren = 0;

        const iterator: AceAjax.TokenIterator = new this.tokenIterator(session, pos.row, pos.column);
        do {
            if ((<any>iterator).$tokenIndex < 0) {
                text = "__new_line";
                continue;
            }
            const token = iterator.getCurrentToken();
            if (!token) {
                break;
            }

            switch (token.type) {
                case "keyword":
                    if (keyword === "by") {
                        keyword = token.value.toLowerCase() + " by";
                    }
                    else {
                        keyword = token.value.toLowerCase();
                    }

                    if (keyword === "desc" ||
                        keyword === "asc" ||
                        keyword === "and" ||
                        keyword === "or" ||
                        keyword === "as") {
                        
                        if (!identifier || !keywordModifier) {
                            keywordModifier = keyword;
                        }
                        
                        continue;
                    }

                    if (keyword === "by") {
                        continue;
                    }

                    return {
                        keyword: keyword,
                        keywordModifier: keywordModifier,
                        operator: operator,
                        identifier: identifier,
                        text: text,
                        paren: paren,
                    };
                case "support.function":
                    return {
                        keyword: "__support.function",
                        keywordModifier: keywordModifier,
                        operator: operator,
                        identifier: identifier,
                        text: text,
                        paren: paren,
                    };
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
                case "paren.lparen":
                    paren++;
                    break;
                case "paren.rparen":
                    paren--;
                    break;
                case "text":
                    if (!identifier && text !== ",") {
                        if (token.value.trim() === ",") {
                            text = ",";
                        }
                        else {
                            text = token.value;
                        }
                    }
                    break;
            }
        } while (iterator.stepBackward());

        return null;
    }

    private completeFields(session: AceAjax.IEditSession, callback: (errors: any[], worldlist: autoCompleteWordList[]) => void): void {
        this.getIndexFields(session)
            .done((indexFields) => callback(null, indexFields.map(field => {
                field += " ";
                return {name: field, value: field, score: this.defaultScore, meta: "field"};
            })));
    }
    
    private completeKeywords(keywords: [string, number][], callback: (errors: any[], worldlist: autoCompleteWordList[]) => void): void {
        callback(null, keywords.map(([keyword, score]) => {
            return {name: keyword, value: keyword, score: score, meta: "keyword"};
        }));
    }

    complete(editor: AceAjax.Editor,
             session: AceAjax.IEditSession,
             pos: AceAjax.Position,
             prefix: string,
             callback: (errors: any[], worldlist: autoCompleteWordList[]) => void) {

        const lastKeyword = this.getLastKeyword(session, pos);
        if (!lastKeyword || !lastKeyword.keyword){
            this.completeKeywords([
                ["from ", 1],
                ["from index ", 1],
                ["select ", 0]
            ], callback);
            
            return;
        }
        
        switch (lastKeyword.keyword) {
            case "from": {
                if (lastKeyword.identifier && lastKeyword.text) {
                    if (lastKeyword.paren > 0) {
                        // from (Collection, {show fields here})
                        this.completeFields(session, callback);
                        return;
                    }

                    const keywords: [string, number][] = [
                        ["order by ", 1],
                        ["where ", 0]
                    ];
                    const [indexName, isStaticIndex] = this.getIndexName(session);
                    if(!isStaticIndex){
                        keywords.push(["group by ", 2])
                    }
                    if(!lastKeyword.keywordModifier){
                        keywords.push(["as ", 3])
                    }
                    this.completeKeywords(keywords, callback);
                    return;
                }

                if (!prefix ||
                    prefix.startsWith("@")) {
                    callback(null, [{name: "@all_docs", value: "@all_docs ", score: this.defaultScore * 10, meta: "collection"}]);
                    callback(null, [{name: "@system", value: "@system ", score: this.defaultScore - 1, meta: "collection"}]);
                }
                callback(null, this.collectionsTracker.getCollectionNames().map(collection => {
                    collection += " ";
                    return {
                        name: collection,
                        value: collection,
                        score: this.defaultScore,
                        meta: "collection"
                    };
                }));
                break;
            }
            case "index": {
                if (lastKeyword.identifier && lastKeyword.text) { // index name already specified
                    return;
                }

                callback(null,
                    this.indexes().map(index => {
                        const name = `'${index.Name}' `;
                        return {name: name, value: name, score: this.defaultScore, meta: "index"};
                    }));
                break;
            }
            case "__support.function":
                if (lastKeyword.identifier && lastKeyword.text) { // field already specified
                    return;
                }
                
                this.completeFields(session, callback);
                break;
            case "select":
                this.completeFields(session, callback);
                break;
            case "group by":
                if (lastKeyword.identifier && lastKeyword.text) { // field already specified
                    return;
                }
                this.completeFields(session, callback);
                break;
            case "order by":
                if (lastKeyword.identifier && lastKeyword.text !== ",") { // field already specified but there is not comma separator for next field
                    if (!lastKeyword.keywordModifier){
                        this.completeKeywords([
                            ["asc ", 0],
                            ["desc ", 1]
                        ], callback);
                    }
                    
                    return;
                }
                
                this.completeFields(session, callback);
                break;
                
            case "where": {
                if (lastKeyword.operator === "=") {
                    // first, calculate and validate the column name
                    let currentField = lastKeyword.identifier;
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
                                                    term = "'" + term + "' ";
                                                    return {name: term, value: term, score: this.defaultScore, meta: "value"};
                                                }));
                                        }
                                    });
                            } else {
                                if (currentValue.length > 0) {
                                    // TODO: Not sure what we want to show here?
                                    new getDocumentsMetadataByIDPrefixCommand(currentValue, this.defaultScore, this.activeDatabase())
                                        .execute()
                                        .done((results: metadataAwareDto[]) => {
                                            if (results && results.length > 0) {
                                                callback(null,
                                                    results.map(curVal => {
                                                        const id = "'" + curVal["@metadata"]["@id"] + "' ";
                                                        return {
                                                            name: id,
                                                            value: id,
                                                            score: this.defaultScore,
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
                
                this.completeFields(session, callback);
                break;
            }
            case "group":
            case "order":
                this.completeKeywords([
                    ["by ", 0]
                ], callback);
                break;
            default: 
                debugger
                break;
        }
    }
}

export = queryCompleter;

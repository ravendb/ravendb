/// <reference path="../../typings/tsd.d.ts" />

import collectionsTracker = require("common/helpers/database/collectionsTracker");
import getIndexTermsCommand = require("commands/database/index/getIndexTermsCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import database = require("models/resources/database");
import getIndexEntriesFieldsCommand = require("commands/database/index/getIndexEntriesFieldsCommand");
import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");

interface autoCompleteLastKeyword {
    keyword: string,
    keywordModifier: string,
    operator: string,
    identifiers: string[],
    text: string,
    paren: number,
}

interface queryCompleterProviders {
    terms: (indexName: string, field: string, pageSize: number, callback: (terms: Array<string>) => void) => void;
    indexFields: (indexName: string, callback: (fields: Array<string>) => void) => void;
    collectionFields: (collectionName: string, callback: (fields: Array<string>) => void) => void;
    collections: (callback: (collectionNames: Array<string>) => void) => void;
    indexNames: (callback: (indexNames: Array<string>) => void) => void;
}

class queryCompleter {
    private tokenIterator: new(session : AceAjax.IEditSession, initialRow: number, initialColumn: number) => AceAjax.TokenIterator = ace.require("ace/token_iterator").TokenIterator;
    private indexOrCollectionFieldsCache = new Map<string, string[]>();
    
    constructor(private providers: queryCompleterProviders) {
        _.bindAll(this, "complete");
    }

    /**
     * Extracts collection or index used in current query 
     */
    private static getQuerySubject(session: AceAjax.IEditSession): { type: "index" | "collection", subject: string } {
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
                        if (keyword === "from") {
                            return {
                                type: "collection",
                                subject: indexName
                            }
                        }
                        if (keyword === "index") {
                            return {
                                type: "index",
                                subject: indexName
                            };
                        }
                        break;
                    }
                    case "identifier": {
                        const indexName = token.value;
                        if (keyword === "from") {
                            return {
                                type: "collection",
                                subject: indexName
                            }
                        }
                        if (keyword === "index") {
                            return {
                                type: "index",
                                subject: indexName
                            }
                        }
                        break;
                    }
                }
            }
        }
        return null;
    }

    private getIndexFields(session: AceAjax.IEditSession): JQueryPromise<string[]> {
        const querySubject = queryCompleter.getQuerySubject(session);
        if (!querySubject) {
            return $.when<string[]>([]); 
        }

        const cachedFields = this.indexOrCollectionFieldsCache.get(querySubject.subject);
        if (cachedFields) {
            return $.when<string[]>(cachedFields);
        }

        const fieldsTasks = $.Deferred<string[]>();
        
        if (querySubject.type === "index") {
            this.providers.indexFields(querySubject.subject, fields => {
                this.indexOrCollectionFieldsCache.set(querySubject.subject, fields);
                fieldsTasks.resolve(fields);
            });
        } else {
            this.providers.collectionFields(querySubject.subject, fields => {
                if (fields && fields.length) {
                    this.indexOrCollectionFieldsCache.set(querySubject.subject, fields);
                    fieldsTasks.resolve(fields);
                }
            });
        }
        
        return fieldsTasks.promise();
    }

    private getLastKeyword(session: AceAjax.IEditSession, pos: AceAjax.Position): autoCompleteLastKeyword {
        let keyword: string;
        let keywordModifier: string;
        let identifiers: string[] = [];
        let text: string;
        let operator: string;
        let paren = 0;
        let liveAutoCompleteSkippedTriggerToken = false;

        const iterator: AceAjax.TokenIterator = new this.tokenIterator(session, pos.row, pos.column);
        do {
            if ((<any>iterator).$tokenIndex < 0) {
                text = "__new_line";
                continue;
            }
            const token = iterator.getCurrentToken();
            if (!token) {
                break;
            } else if (!liveAutoCompleteSkippedTriggerToken){
                const firstToken = token.value.trim();
                liveAutoCompleteSkippedTriggerToken = true;
                if (firstToken !== "" && firstToken !==","){
                    continue;
                }
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
                        
                        if (identifiers.length > 0 || !keywordModifier) {
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
                        identifiers: identifiers,
                        text: text,
                        paren: paren,
                    };
                case "support.function":
                    return {
                        keyword: "__support.function",
                        keywordModifier: keywordModifier,
                        operator: operator,
                        identifiers: identifiers,
                        text: text,
                        paren: paren,
                    };
                case "keyword.operator":
                    operator = token.value;
                    break;
                case "identifier":
                    identifiers.push(token.value);
                    break;
                case "string":
                    const indexName = token.value.substr(1, token.value.length - 2);
                    identifiers.push(indexName);
                    break;
                case "paren.lparen":
                    paren++;
                    break;
                case "paren.rparen":
                    paren--;
                    break;
                case "text":
                    if (identifiers.length > 0 && text !== ",") {
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

    private completeFields(session: AceAjax.IEditSession, callback: (errors: any[], wordList: autoCompleteWordList[]) => void): void {
        this.getIndexFields(session)
            .done((indexFields) => this.completeWords(callback, indexFields.map(field => {
                return {value: field, score: 1, meta: "field"};
            })));
    }

    complete(editor: AceAjax.Editor,
             session: AceAjax.IEditSession,
             pos: AceAjax.Position,
             prefix: string,
             callback: (errors: any[], wordList: autoCompleteWordList[]) => void) {

        const lastKeyword = this.getLastKeyword(session, pos);
        if (!lastKeyword || !lastKeyword.keyword) {

            const keywords = [
                {value: "from", score: 1, meta: "keyword"},
                {value: "select", score: 0, meta: "keyword"}
            ];
            this.completeWords(callback, keywords);

            return;
        }
        
        switch (lastKeyword.keyword) {
            case "from": {
                if (lastKeyword.identifiers.length > 0 && lastKeyword.text) {
                    if (lastKeyword.paren > 0) {
                        // from (Collection, {show fields here})
                        this.completeFields(session, callback);
                        return;
                    }

                    this.completeFromAfter(callback, false, lastKeyword);
                    return;
                }

                this.completeFrom(callback);
                break;
            }
            case "index": {
                if (lastKeyword.identifiers.length > 0 && lastKeyword.text) { // index name already specified
                    this.completeFromAfter(callback, true, lastKeyword);
                    return;
                }

                this.providers.indexNames(names => {
                    this.completeWords(callback, names.map(name => ({
                        value: `'${name}'`,
                        score: 1, 
                        meta: "index"
                    })));
                });
                break;
            }
            case "__support.function":
                if (lastKeyword.identifiers.length > 0 && lastKeyword.text) { // field already specified
                    return;
                }
                
                this.completeFields(session, callback);
                break;
            case "select":
                if (lastKeyword.identifiers.length > 0 && lastKeyword.text && !lastKeyword.text.trim()) {
                    if (!lastKeyword.keywordModifier) {
                        this.completeWords(callback, [{value: "as", score: 3, meta: "keyword"}]);
                    }
                    
                    return;
                }
                
                this.completeFields(session, callback);
                break;
            case "group by":
                if (lastKeyword.identifiers.length > 0 && lastKeyword.text) { // field already specified
                    return;
                }
                this.completeFields(session, callback);
                break;
            case "order by":
                if (lastKeyword.identifiers.length > 0 && lastKeyword.text !== ",") { // field already specified but there is not comma separator for next field
                    if (!lastKeyword.keywordModifier){
                        const keywords = [
                            {value: ",", score: 2, meta: "separator"},
                            {value: "asc", score: 0, meta: "keyword"},
                            {value: "desc", score: 1, meta: "keyword"}
                        ];
                        this.completeWords(callback, keywords);
                    }
                    
                    return;
                }
                
                this.completeFields(session, callback);
                break;
                
            case "where": {
                if (lastKeyword.operator === "=") {
                    // first, calculate and validate the column name
                    let currentField = _.last(lastKeyword.identifiers);
                    if (!currentField) {
                        return;
                    }

                    this.getIndexFields(session)
                        .done((indexFields) => {
                            if (!indexFields.find(x => x === currentField)) {
                                return;
                            }

                            let currentValue: string = "";

                            /* TODO: currentValue = currentToken.value.trim();
                             const rowTokens: any[] = session.getTokens(pos.row);
                             if (!!rowTokens && rowTokens.length > 1) {
                             currentColumnName = rowTokens[rowTokens.length - 2].value.trim();
                             currentColumnName = currentColumnName.substring(0, currentColumnName.length - 1);
                             }*/


                            // for non dynamic indexes query index terms, for dynamic indexes, try perform general auto complete
                            const querySubject = queryCompleter.getQuerySubject(session);
                            if (!querySubject) {
                                return; // todo: try to callback with error
                            }
                            
                            if (querySubject.type === "index") {
                                this.providers.terms(querySubject.subject, currentField, 20, terms => {
                                    if (terms && terms.length) {
                                        this.completeWords(callback,
                                            terms.map(term => ({value: `'${term}'`, score: 1, meta: "value"})));
                                    }
                                })
                            } else {
                                /* TODO finish me!
                                if (currentValue.length > 0) {
                                    // TODO: Not sure what we want to show here?
                                    new getDocumentsMetadataByIDPrefixCommand(currentValue, 1, this.activeDatabase())
                                        .execute()
                                        .done((results: metadataAwareDto[]) => {
                                            if (results && results.length > 0) {
                                                this.completeWords(callback, results.map(curVal => {
                                                    return {
                                                        value: "'" + curVal["@metadata"]["@id"] + "'",
                                                        score: 1,
                                                        meta: "value"
                                                    };
                                                }));
                                            }
                                        });
                                } else {
                                    callback([{error: "notext"}], null);
                                }*/
                            }
                        });
                    return;
                }
                
                this.completeFields(session, callback);
                break;
            }
            case "group":
            case "order":
                this.completeWords(callback, [{value: "by", score: 0, meta: "keyword"}]);
                break;
            default: 
                debugger;
                break;
        }
    }

    private completeWords(callback: (errors: any[], wordList: autoCompleteWordList[]) => void, keywords: ({value: string; score: number; meta: string})[]) {
        callback(null,  keywords.map(keyword  => {
            const word = <autoCompleteWordList>keyword;
            word.caption = _.trim(keyword.value, "'");
            return word;
        }))
    }

    private completeFrom(callback: (errors: any[], wordList: autoCompleteWordList[]) => void) {
        this.providers.collections(collections => {
           const wordList = collections.map(name => ({
               value: name + " ",
               score: 2,
               meta: "collection"
           })); 
           
           wordList.push(
               {value: "index", score: 4, meta: "keyword"},
               {value: "@all_docs", score: 3, meta: "collection"},
               {value: "@system", score: 1, meta: "collection"}
           );
           
           this.completeWords(callback,  wordList);
        });
    }

    private completeFromAfter(callback: (errors: any[], wordList: autoCompleteWordList[]) => void, isStaticIndex: boolean, lastKeyword: autoCompleteLastKeyword) {
        if (lastKeyword.keywordModifier && lastKeyword.identifiers.length < 2) {
            return;
        }

        const keywords = [
            {value: "order", score: 1, meta: "keyword"},
            {value: "where", score: 0, meta: "keyword"}
        ];
        if (!isStaticIndex) {
            keywords.push({value: "group", score: 2, meta: "keyword"})
        }
        if (!lastKeyword.keywordModifier) {
            keywords.push({value: "as", score: 3, meta: "keyword"})
        }

        this.completeWords(callback, keywords);
    }
    
    
    static remoteCompleter(activeDatabase: KnockoutObservable<database>, indexes: KnockoutObservableArray<Raven.Client.Documents.Operations.IndexInformation>) {
        return new queryCompleter({
            terms: (indexName, field, pageSize, callback) => {
                new getIndexTermsCommand(indexName, field, activeDatabase(), pageSize)
                    .execute()
                    .done(terms => {
                        callback(terms.Terms);
                    });
            },
            collections: (callback) => {
                callback(collectionsTracker.default.getCollectionNames());
            },
            indexFields: (indexName, callback) => {
                new getIndexEntriesFieldsCommand(indexName, activeDatabase())
                    .execute()
                    .done(result => {
                        callback(result.Results);
                    })
            },
            collectionFields: (collectionName, callback) => {
                const matchedCollection = collectionsTracker.default.collections().find(x => x.name === collectionName);
                if (matchedCollection) {
                    matchedCollection.fetchDocuments(0, 1)
                        .done(result => {
                            if (result && result.items.length) {
                                const propertyNames = new document(result.items[0]).getDocumentPropertyNames();
                                callback(propertyNames);
                            }
                        });
                }
            },
            indexNames: callback => {
                callback(indexes().map(x => x.Name));
            }
        });
    }
}

export = queryCompleter;

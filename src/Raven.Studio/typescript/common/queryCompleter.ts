/// <reference path="../../typings/tsd.d.ts" />

import collectionsTracker = require("common/helpers/database/collectionsTracker");
import getIndexTermsCommand = require("commands/database/index/getIndexTermsCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import database = require("models/resources/database");
import getIndexEntriesFieldsCommand = require("commands/database/index/getIndexEntriesFieldsCommand");
import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");

interface autoCompleteLastKeyword {
    keywordsBefore: string[],
    keyword: string,
    keywordModifier: string,
    operator: string,
    fieldPrefix: string[],
    readonly getFieldPrefix: string,
    identifiers: string[],
    text: string,
    paren: number
}

interface queryCompleterProviders {
    terms: (indexName: string, field: string, pageSize: number, callback: (terms: Array<string>) => void) => void;
    indexFields: (indexName: string, callback: (fields: Array<string>) => void) => void;
    collectionFields: (collectionName: string, prefix: string, callback: (fields: object) => void) => void;
    collections: (callback: (collectionNames: Array<string>) => void) => void;
    indexNames: (callback: (indexNames: Array<string>) => void) => void;
}

class queryCompleter {
    private tokenIterator: new(session : AceAjax.IEditSession, initialRow: number, initialColumn: number) => AceAjax.TokenIterator = ace.require("ace/token_iterator").TokenIterator;
    private indexOrCollectionFieldsCache = new Map<string, autoCompleteWordList[]>();
    
    constructor(private providers: queryCompleterProviders) {
        _.bindAll(this, "complete");
    }

    /**
     * Extracts collection or index used in current query 
     */
    private static extractIndexOrCollectionName(session: AceAjax.IEditSession): { type: "index" | "collection", name: string } {
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
                                name: indexName
                            }
                        }
                        if (keyword === "index") {
                            return {
                                type: "index",
                                name: indexName
                            };
                        }
                        break;
                    }
                    case "identifier": {
                        const indexName = token.value;
                        if (keyword === "from") {
                            return {
                                type: "collection",
                                name: indexName
                            }
                        }
                        if (keyword === "index") {
                            return {
                                type: "index",
                                name: indexName
                            }
                        }
                        break;
                    }
                }
            }
        }
        return null;
    }

    private getIndexFields(queryIndexName: string, queryIndexType: string, prefix: string): JQueryPromise<autoCompleteWordList[]> {
        const wordList: autoCompleteWordList[] = [];

        let key = queryIndexName;
        if (prefix) {
            key += prefix;
        }
        
        const cachedFields = this.indexOrCollectionFieldsCache.get(key);
        if (cachedFields) {
            return $.when<autoCompleteWordList[]>(cachedFields);
        }

        const fieldsTasks = $.Deferred<autoCompleteWordList[]>();
        
        if (queryIndexType === "index") {
            this.providers.indexFields(queryIndexName, fields => {
                fields.map(field => {
                    wordList.push({caption: field, value: field, score: 1, meta: "field"});
                });
                
                this.indexOrCollectionFieldsCache.set(key, wordList);
                fieldsTasks.resolve(wordList);
            });
        } else {
            this.providers.collectionFields(queryIndexName, prefix, fields => {
                _.forOwn(fields, (value, key) => {
                    let formattedFieldType = value.toLowerCase().split(", ").map((fieldType: string) => {
                        if (fieldType.length > 5 && fieldType.startsWith("array")){
                            fieldType = fieldType.substr(5) + "[]";
                        }
                        return fieldType;
                    }).join(" | ");

                    wordList.push({caption: key, value: key, score: 1, meta: formattedFieldType + " field"});
                });
                
                this.indexOrCollectionFieldsCache.set(key, wordList);
                fieldsTasks.resolve(wordList);
            });
        }

        return fieldsTasks.promise();
    }

    private getKeywordsBefore(iterator: AceAjax.TokenIterator): string[] {
        const keywords = [];
        
        while (iterator.stepBackward()){
            const token = iterator.getCurrentToken();
            if (token.type ==="keyword"){
                const keyword = token.value.toLowerCase();
                keywords.push(keyword);
            }
        }
        
        return keywords;
    }
    
    private getLastKeyword(session: AceAjax.IEditSession, pos: AceAjax.Position): autoCompleteLastKeyword {
        const result: autoCompleteLastKeyword = {
            keywordsBefore: undefined,
            keyword: undefined,
            keywordModifier: undefined,
            operator: undefined,
            fieldPrefix: undefined,
            get getFieldPrefix():string {
                return this.fieldPrefix ? this.fieldPrefix.join(".") : undefined;
            },
            identifiers: [],
            text: undefined,
            paren: 0
        };
            
        let liveAutoCompleteSkippedTriggerToken = false;
        let isFieldPrefixMode = 0;

        const iterator: AceAjax.TokenIterator = new this.tokenIterator(session, pos.row, pos.column);
        do {
            if ((<any>iterator).$tokenIndex < 0) {
                result.text = "__new_line";
                continue;
            }
            const token = iterator.getCurrentToken();
            if (!token) {
                break;
            } else if (!liveAutoCompleteSkippedTriggerToken){
                liveAutoCompleteSkippedTriggerToken = true;
                if (token.type === "identifier") {
                    continue;
                }
                else if (token.type === "text") {
                    const firstToken = token.value.trim();
                    if (firstToken !== "" && firstToken !== "," && firstToken !== "." && firstToken !== "[].") {
                        continue;
                    }
                }
            }

            switch (token.type) {
                case "keyword":
                    if (result.keyword === "by") {
                        result.keyword = token.value.toLowerCase() + " by";
                    }
                    else {
                        result.keyword = token.value.toLowerCase();
                    }

                    if (result.keyword === "desc" ||
                        result.keyword === "asc" ||
                        result.keyword === "and" ||
                        result.keyword === "or" ||
                        result.keyword === "as") {

                        if (result.identifiers.length > 0 || !result.keywordModifier) {
                            result.keywordModifier = result.keyword;
                        }

                        continue;
                    }

                    if (result.keyword === "by") {
                        continue;
                    }

                    result.keywordsBefore = this.getKeywordsBefore(iterator);
                    return result;
                case "support.function":
                    result.keywordsBefore = this.getKeywordsBefore(iterator);
                    result.keyword = "__support.function";
                    return result;
                case "keyword.operator":
                    result.operator = token.value;
                    break;
                case "identifier":
                    if (isFieldPrefixMode === 1) {
                        result.fieldPrefix.push(token.value);
                    } else {
                        result.identifiers.push(token.value);
                    }
                    break;
                case "string":
                    const indexName = token.value.substr(1, token.value.length - 2);
                    result.identifiers.push(indexName);
                    break;
                case "paren.lparen":
                    result.paren++;
                    break;
                case "paren.rparen":
                    result.paren--;
                    break;
                case "text":
                    const text = token.value.trim();
                    if (isFieldPrefixMode === 0 && (text === "." || text === "[].")) {
                        isFieldPrefixMode = 1;
                        result.fieldPrefix = [];
                    }
                    else if (isFieldPrefixMode === 1 && !token.value.trim()) {
                        isFieldPrefixMode = 2;
                    }
                    
                    if (result.identifiers.length > 0 && result.text !== ",") {
                        if (token.value.trim() === ",") {
                            result.text = ",";
                        }
                        else {
                            result.text = token.value;
                        }
                    }
                    break;
            }
        } while (iterator.stepBackward());

        return null;
    }

    private completeFields(session: AceAjax.IEditSession, prefix: string, callback: (errors: any[], wordList: autoCompleteWordList[]) => void): void {
        const queryIndexName = queryCompleter.extractIndexOrCollectionName(session);
        if (!queryIndexName) {
            return;
        }
        
        this.getIndexFields(queryIndexName.name, queryIndexName.type, prefix)
            .done((wordList) => {
                callback(null, wordList);
            });
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
                        this.completeFields(session, lastKeyword.getFieldPrefix, callback);
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
                
                this.completeFields(session, lastKeyword.getFieldPrefix, callback);
                break;
            case "select":
                if (lastKeyword.identifiers.length > 0 && lastKeyword.text && !lastKeyword.text.trim()) {
                    if (!lastKeyword.keywordModifier) {
                        this.completeWords(callback, [{value: "as", score: 3, meta: "keyword"}]);
                    }
                    
                    return;
                }
                
                this.completeFields(session, lastKeyword.getFieldPrefix, callback);
                break;
            case "group by":
                if (lastKeyword.identifiers.length > 0 && lastKeyword.text) { // field already specified
                    return;
                }
                this.completeFields(session, lastKeyword.getFieldPrefix, callback);
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
                
                this.completeFields(session, lastKeyword.getFieldPrefix, callback);
                break;
                
            case "where": {
                if (lastKeyword.operator === "=") {
                    // first, calculate and validate the column name
                    let currentField = _.last(lastKeyword.identifiers);
                    if (!currentField) {
                        return;
                    }

                    // TODO: remove extractIndexOrCollectionName and extract in getLastKeyword
                    const queryIndexName = queryCompleter.extractIndexOrCollectionName(session);
                    if (!queryIndexName) {
                        return;
                    }

                    this.getIndexFields(queryIndexName.name, queryIndexName.type, prefix)
                        .done((wordList) => {
                            if (!wordList.find(x => x.value === currentField)) {
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
                            const queryIndexName = queryCompleter.extractIndexOrCollectionName(session);
                            if (!queryIndexName) {
                                return; // todo: try to callback with error
                            }
                            
                            if (queryIndexName.type === "index") {
                                this.providers.terms(queryIndexName.name, currentField, 20, terms => {
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
                
                this.completeFields(session, lastKeyword.getFieldPrefix, callback);
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
            keywords.push({value: "as", score: 4, meta: "keyword"})
        }
        if (!lastKeyword.keywordsBefore.find(keyword => keyword === "select")) {
            keywords.push({value: "select", score: 3, meta: "keyword"})
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
            collectionFields: (collectionName, prefix, callback) => {
                if (collectionName === "@all_docs"){
                    collectionName = "All Documents";
                }
                const matchedCollection = collectionsTracker.default.collections().find(x => x.name === collectionName);
                if (matchedCollection) {
                    matchedCollection.fetchFields(prefix)
                        .done(result => {
                            if (result) {
                                callback(result);
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

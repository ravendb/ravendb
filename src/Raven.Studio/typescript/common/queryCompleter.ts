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
    spaceCount: number,
    parentheses: number
}

interface queryCompleterProviders {
    terms: (indexName: string, field: string, pageSize: number, callback: (terms: string[]) => void) => void;
    indexFields: (indexName: string, callback: (fields: string[]) => void) => void;
    collectionFields: (collectionName: string, prefix: string, callback: (fields: dictionary<string>) => void) => void;
    collections: (callback: (collectionNames: string[]) => void) => void;
    indexNames: (callback: (indexNames: string[]) => void) => void;
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
                    case "keyword.clause": {
                        keyword = token.value.toLowerCase();
                        break;
                    }
                    case "keyword.clause.clauseAppend": {
                        keyword += " " + token.value.toLowerCase();
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
                        if (keyword === "from index") {
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
                        if (keyword === "from index") {
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
            if (token.type ==="keyword.clause"){
                const keyword = token.value.toLowerCase();
                keywords.push(keyword);
            }
        }
        
        return keywords;
    }
    
    private getLastKeyword(session: AceAjax.IEditSession, pos: AceAjax.Position): autoCompleteLastKeyword {
        const mode = session.getMode();
        const rules = <AceAjax.RqlHighlightRules>mode.$highlightRules;
        
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
            spaceCount: 0,
            parentheses: 0
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
                case "keyword.clause":
                    const keyword = token.value.toLowerCase();
                    if (_.includes(rules.clauseAppendKeywords, result.keyword)) {
                        result.keyword = keyword + " " + result.keyword;
                    } else {
                        result.keyword = keyword;
                    }
                    
                    result.keywordsBefore = this.getKeywordsBefore(iterator);
                    return result;
                case "keyword.clause.clauseAppend":
                    result.keyword = token.value.toLowerCase();
                    break;
                case "keyword.insideClause":
                    if (result.identifiers.length > 0 || !result.keywordModifier) {
                        result.keywordModifier = token.value.toLowerCase();
                    }
                    break;
                case "function":
                    result.keywordsBefore = this.getKeywordsBefore(iterator);
                    result.keyword = "__function";
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
                    const lastChar = token.value[token.value.length - 1];
                    if (lastChar === "'" || 
                        lastChar === '"') {
                        const indexName = token.value.substr(1, token.value.length - 2);
                        result.identifiers.push(indexName);
                    } else {
                        // const partialIndexName = token.value.substr(1);
                        // do nothing with it as of now
                    }
                    break;
                case "paren.lparen":
                    result.parentheses++;
                    break;
                case "paren.rparen":
                    result.parentheses--;
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
                        if (text === ",") {
                            result.text = ",";
                        }
                        else {
                            result.text = token.value;
                        }
                    }
                    
                    if (!text) {
                        result.spaceCount++;
                    }
                    
                    break;
            }
        } while (iterator.stepBackward());

        return null;
    }

    private completeFields(session: AceAjax.IEditSession, prefix: string, callback: (errors: any[], wordList: autoCompleteWordList[]) => void, functions: autoCompleteWordList[] = null): void {
        const queryIndexName = queryCompleter.extractIndexOrCollectionName(session);
        if (!queryIndexName) {
            return;
        }
        
        this.getIndexFields(queryIndexName.name, queryIndexName.type, prefix)
            .done((wordList) => {
                if (functions) {
                    wordList = wordList.concat(functions);
                }
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
            this.completeEmpty(callback);
            return;
        }
        
        switch (lastKeyword.keyword) {
            case "from": {
                if (lastKeyword.identifiers.length > 0 && lastKeyword.spaceCount >= 2) {
                    if (lastKeyword.parentheses > 0) {
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
            case "from index": {
                if (lastKeyword.identifiers.length > 0 && lastKeyword.spaceCount >= 3) { // index name already specified
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
            case "__function":
                if (lastKeyword.identifiers.length > 0 && lastKeyword.text) { // field already specified
                    return;
                }
                
                this.completeFields(session, lastKeyword.getFieldPrefix, callback);
                break;
            case "declare":
                this.completeWords(callback, [
                    {value: "function", score: 0, meta: "keyword"}
                ]);
                break;
            case "declare function":
                if (lastKeyword.parentheses === 0 && lastKeyword.identifiers.length > 0 && lastKeyword.text && !lastKeyword.text.trim()) {
                    this.completeEmpty(callback);
                }
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
                            {value: ",", score: 5, meta: "separator"},
                            {value: "asc", score: 4, meta: "keyword"},
                            {value: "desc", score: 3, meta: "keyword"}
                        ];
                        this.completeWords(callback, keywords);
                    }
                    
                    return;
                }
                
                this.completeFields(session, lastKeyword.getFieldPrefix, callback, [
                    {caption: "random", value: "random(", score: 0, meta: "function"},
                    {caption: "score", value: "score(", score: 0, meta: "function"}
                ]);
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
                break;
        }
    }

    private completeWords(callback: (errors: any[], wordList: autoCompleteWordList[]) => void, keywords: ({value: string; score: number; meta: string})[]) {
        callback(null,  keywords.map(keyword  => {
            const word = <autoCompleteWordList>keyword;
            word.caption = _.trim(keyword.value, "'");
            if (keyword.meta === "function"){
                keyword.value += "(";
            } else {
                keyword.value += " "; // insert space after each completed keyword or other value.
            }
            return word;
        }))
    }

    private isLetterOrDigit(str: string) {
        // TODO: Add support for more letters in other lanuagse.
        return /^[0-9a-zA-Z_@]+$/.test(str)
    }

    private completeEmpty(callback: (errors: any[], wordList: autoCompleteWordList[]) => void) {
        const keywords = [
            {value: "from", score: 2, meta: "keyword"},
            {value: "declare", score: 1, meta: "keyword"},
            {value: "select", score: 0, meta: "keyword"}
        ];
        this.completeWords(callback, keywords);
    }

    private completeFrom(callback: (errors: any[], wordList: autoCompleteWordList[]) => void) {
        this.providers.collections(collections => {
            const wordList = collections.map(name => {
                if (!this.isLetterOrDigit(name)) {
                    name = "'" + name + "'";     // wrap collection name in 'collection name' if it has spaces.
                }
                return {
                    value: name,
                    score: 2,
                    meta: "collection"
                };
            });

            wordList.push(
                {value: "index", score: 4, meta: "keyword"},
                {value: "@all_docs", score: 3, meta: "collection"},
                {value: "@system", score: 1, meta: "collection"}
            );

            this.completeWords(callback, wordList);
        });
    }

    private completeFromAfter(callback: (errors: any[], wordList: autoCompleteWordList[]) => void, isStaticIndex: boolean, lastKeyword: autoCompleteLastKeyword) {
        if (lastKeyword.keywordModifier && lastKeyword.identifiers.length < 2) {
            return;
        }

        const keywords = [
            {value: "where", score: 6, meta: "keyword"},
            {value: "load", score: 4, meta: "keyword"},
            {value: "order", score: 2, meta: "keyword"},
            {value: "include", score: 1, meta: "keyword"},
        ];
        if (!isStaticIndex) {
            keywords.push({value: "group", score: 5, meta: "keyword"})
        }
        if (!lastKeyword.keywordModifier) {
            keywords.push({value: "as", score: 7, meta: "keyword"})
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

/// <reference path="../../typings/tsd.d.ts" />

import collectionsTracker = require("common/helpers/database/collectionsTracker");
import getIndexTermsCommand = require("commands/database/index/getIndexTermsCommand");
import database = require("models/resources/database");
import getIndexEntriesFieldsCommand = require("commands/database/index/getIndexEntriesFieldsCommand");
import queryUtil = require("common/queryUtil");

class queryCompleter {
    private rules: AceAjax.RqlHighlightRules;
    private session: AceAjax.IEditSession;
    private callback: (errors: any[], wordList: autoCompleteWordList[]) => void;
    public lastKeyword: autoCompleteLastKeyword;

    private tokenIterator: new(session : AceAjax.IEditSession, initialRow: number, initialColumn: number) => AceAjax.TokenIterator = ace.require("ace/token_iterator").TokenIterator;
    private indexOrCollectionFieldsCache = new Map<string, autoCompleteWordList[]>();
    
    public static whereOperators: autoCompleteWordList[] = [
        {caption: "in", value: "in ", score: 20, meta: "any in array", snippet: "in (${1:value1, value2}) "},
        {caption: "all in", value: "all in ", score: 19, meta: "all in array", snippet: "all in (${1:value1, value2}) "},
        {caption: "between",value: "between ", score: 18, meta: "two numbers", snippet: "between ${1:number1} and ${2:number2} "},
        {caption: "=", value: "= ", score: 17, meta: "operator"},
        {caption: "!=", value: "!= ", score: 16, meta: "operator"}, 
        {caption: ">", value: "> ", score: 15, meta: "operator"},
        {caption: "<", value: "< ", score: 14, meta: "operator"},
        {caption: ">=", value: ">= ", score: 13, meta: "operator"},
        {caption: "<=", value: "<= ", score: 12, meta: "operator"}
    ];

    public static functionsList: autoCompleteWordList[] = [
        {caption: "ID", value: "ID() ", score: 11, meta: "document ID"}
    ];

    public static whereFunctionsOnly: autoCompleteWordList[] = [
        {caption: "search", value: "search ", snippet: "search(${1:alias.Field.Name}, ${2:'*term1* term2*'}, ${3:or}) ", score: 21, meta: "function"}
    ];

    public static whereFunctionsList: autoCompleteWordList[] = queryCompleter.whereFunctionsOnly.concat(queryCompleter.functionsList);

    public static notList: autoCompleteWordList[] = [
        {caption: "not", value: "not ", score: Number.MAX_SAFE_INTEGER, meta: "keyword"}
    ];
    
    public static notAfterAndOrList: autoCompleteWordList[] = queryCompleter.notList.concat(queryCompleter.whereFunctionsList);

    public static asList: autoCompleteWordList[] = [{caption: "as", value: "as ", score: 21, meta: "keyword"}];
    
    public static separatorList: autoCompleteWordList[] = [
        {caption: ",", value: ", ", score: 23, meta: "separator"}
    ];
    
    public static orderBySortList: autoCompleteWordList[] = [
        {caption: "desc", value: "desc ", score: 22, meta: "descending sort"},
        {caption: "asc", value: "asc ", score: 21, meta: "ascending sort"}
    ];
    
    public static binaryOperationsList: autoCompleteWordList[] = [
        {caption: "or", value: "or ", score: 22, meta: "any term"},
        {caption: "and", value: "and ", score: 21, meta: "all terms"}
    ];
    
    constructor(private providers: queryCompleterProviders, private queryType: rqlQueryType) {
        _.bindAll(this, "complete");
    }

    /**
     * Extracts collection or index used in current query 
     */
    private extractQueryInfo(pos: AceAjax.Position): rqlQueryInfo {
        const result: rqlQueryInfo = {
            collection: undefined,
            index: undefined,
            alias: undefined,
            aliases: undefined
        };
        
        let keyword: string;
        let identifier: string;
        let nestedFieldName: string;
        let asSpecified = false;
        let handleAfterNextToken = false;

        const iterator: AceAjax.TokenIterator = new this.tokenIterator(this.session, pos.row, pos.column);
        do {
            const token = iterator.getCurrentToken();
            if (!token) {
                continue;
            }
            if (token.type === "keyword.clause" && token.value.toLowerCase() === "from") {
                break;
            }
        } while (iterator.stepBackward());

        do {
            const token = iterator.getCurrentToken();
            if (!token) {
                break;
            }

            if (handleAfterNextToken){
                handleAfterNextToken = false;
                
                if (asSpecified) {
                    if (keyword === "load") {
                        result.aliases[identifier] = nestedFieldName;
                    } else {
                        result.alias = identifier;
                        result.aliases = {};
                        result.aliases[identifier] = result.collection;
                    }
                } else if (keyword === "load") {
                    nestedFieldName = identifier;
                } else if (keyword === "from") {
                    result.collection = identifier;
                    asSpecified = true;
                } else if (keyword === "index") {
                    result.index = identifier;
                    asSpecified = true;
                }
            }
            
            switch (token.type) {
                case "keyword.clause":
                case "keyword.clause.clauseAppend": {
                    keyword = token.value.toLowerCase();
                    if (keyword !== "from" && 
                        keyword !== "index" && 
                        keyword !== "load") {
                        return result;
                    }
                    asSpecified = false;
                    break;
                }
                case "field":
                    identifier = token.value.split('.').pop();
                    handleAfterNextToken = true;
                    break;
                case "string":
                case "identifier":
                case "text": {
                    identifier = token.value;

                    if (token.type === "string") {
                        const lastChar = identifier[identifier.length - 1];
                        if (lastChar === "'" || lastChar === '"'){ // index name or collection with space
                            identifier = identifier.substr(1, identifier.length - 2);
                        }
                    }
                    handleAfterNextToken = true;
                    break;
                }
                case "keyword.asKeyword":
                    asSpecified = true;
                    break;
                case "comma":
                    asSpecified = false;
                    break;
            }
        } while (iterator.stepForward());
        
        return result;
    }

    private getIndexFields(additions: autoCompleteWordList[] = null): JQueryPromise<autoCompleteWordList[]> {
        const wordList: autoCompleteWordList[] = [];

        const lastKeyword = this.lastKeyword;
        const collection = lastKeyword.info.collection;
        const index = lastKeyword.info.index;
        let key = collection ? collection : index;
        let prefixSpecified = false;
        let prefixToSend: string;
        if (lastKeyword.fieldPrefix) {
            prefixToSend = lastKeyword.fieldPrefix.join(".");
            key += prefixToSend;
            prefixSpecified = true;

            if (lastKeyword.info.aliases) {
                const last = lastKeyword.fieldPrefix.length - 1;
                const alias = lastKeyword.fieldPrefix[last];
                if (lastKeyword.info.aliases[alias]) {
                    lastKeyword.fieldPrefix.splice(last, 1);
                }
            }
        }
        
        const cachedFields = this.indexOrCollectionFieldsCache.get(key);
        if (cachedFields) {
            return $.when<autoCompleteWordList[]>(cachedFields);
        }

        const fieldsTasks = $.Deferred<autoCompleteWordList[]>();
        
        const taskResult = () => {
            this.indexOrCollectionFieldsCache.set(key, wordList);

            if (additions && !prefixSpecified) {
                const copy = wordList.concat(additions); // do not modify the original collection which is cached.
                return fieldsTasks.resolve(copy);
            }
            fieldsTasks.resolve(wordList);
        };
        
        if (index) {
            this.providers.indexFields(index, fields => {
                fields.map(field => {
                    wordList.push({caption: field, value: queryUtil.escapeCollectionOrFieldName(field) + " ", score: 101, meta: "field"});
                });

                return taskResult();
            });
        } else {
            this.providers.collectionFields(collection, prefixToSend, fields => {
                _.forOwn(fields, (value, key) => {
                    let formattedFieldType = value.toLowerCase().split(", ").map((fieldType: string) => {
                        if (fieldType.length > 5 && fieldType.startsWith("array")) {
                            fieldType = fieldType.substr(5) + "[]";
                        }
                        return fieldType;
                    }).join(" | ");

                    wordList.push({
                        caption: key,
                        value: queryUtil.escapeCollectionOrFieldName(key) + " ",
                        score: 101,
                        meta: formattedFieldType + " field"
                    });
                });
                let i = 1;
                _.sortBy(wordList, word => {
                    // @metadata fields should be at the bottom
                    const code = word.caption.charCodeAt(0);
                    if (code && code <= 64)
                        return "~" + word.caption;
                    return word.caption;
                }).reverse().map(keyword => keyword.score = 100 + i++);

                return taskResult();
            });
        }

        return fieldsTasks.promise();
    }
    
    private getLastKeyword(pos: AceAjax.Position): autoCompleteLastKeyword {
        const mode = this.session.getMode();
        this.rules = <AceAjax.RqlHighlightRules>mode.$highlightRules;
        
        const result: autoCompleteLastKeyword = {
            info: this.extractQueryInfo(pos),
            keyword: undefined,
            asSpecified: false,
            notSpecified: false,
            binaryOperation: undefined,
            whereFunction: undefined,
            whereFunctionParameters: 0,
            fieldPrefix: undefined,
            fieldName: undefined,
            dividersCount: 0,
            parentheses: 0
        };
    
        let liveAutoCompleteSkippedTriggerToken = false;
        let isBeforeComma = false;
        let isBeforeBinaryOperation = false;
        let afterCommaDividersCount = 0;

        let lastRow: number;
        let lastToken: AceAjax.TokenInfo;
        const iterator: AceAjax.TokenIterator = new this.tokenIterator(this.session, pos.row, pos.column);
        do {
            const row = iterator.getCurrentTokenRow();
            if (!isBeforeComma && !isBeforeBinaryOperation && lastRow && lastToken && lastToken.type !== "space" && row - lastRow < 0) {
                result.dividersCount++;
                lastToken.type = "space";
            }
            lastRow = row;
            
            if (iterator.$tokenIndex < 0) { // TODO: Refactor, this is not needed anymore
                result.dividersCount++;
                lastToken = {type: "space", start: null, index: null, value: null};
                continue;
            }
            const token = iterator.getCurrentToken();
            if (!token) {
                break;
            } else if (!liveAutoCompleteSkippedTriggerToken){
                liveAutoCompleteSkippedTriggerToken = true;
                if (token.type === "identifier") {
                    lastToken = token;
                    continue;
                }
            }
            
            switch (token.type) {
                case "keyword.clause":
                    const keyword = token.value.toLowerCase();
                    if (_.includes(this.rules.clauseAppendKeywords, result.keyword)) {
                        result.keyword = keyword + " " + result.keyword;
                    } else {
                        result.keyword = keyword;
                    }
                    return result;
                case "keyword.clause.clauseAppend":
                    if (result.keyword) {
                        result.dividersCount++;
                    }
                    result.keyword = token.value.toLowerCase();
                    break;
                case "keyword.asKeyword":
                    if (!isBeforeComma && !isBeforeBinaryOperation) {
                        result.asSpecified = true;
                    }
                    break;
                case "keyword.notKeyword":
                    if (!isBeforeComma && !isBeforeBinaryOperation) {
                        result.notSpecified = true;
                        result.dividersCount--;
                    }
                    break;
                case "operations.type.binary":
                    if (!isBeforeComma && !isBeforeBinaryOperation && !result.binaryOperation) {
                        result.binaryOperation = token.value.toLowerCase();
                        isBeforeBinaryOperation = true;
                    }
                    break;
                case "function.where":
                case "keyword.whereOperators":
                    if (!isBeforeComma && !isBeforeBinaryOperation) {
                        result.whereFunction = token.value.toLowerCase();
                    }
                    break;
                case "identifier":
                case "identifier.whereFunction":
                    if (!isBeforeComma && !isBeforeBinaryOperation) {
                        if (!result.fieldName) {
                            result.fieldName = token.value;
                        }
                    }
                    break;
                case "field":
                    if (!isBeforeComma && !isBeforeBinaryOperation) {
                        const text = token.value;
                        let fieldPrefix = text
                            .split(".")
                            .map(field => field.endsWith("[]") ? field.substring(0, field.length - 2) : field);

                        result.fieldName = fieldPrefix.pop();
                        if (fieldPrefix.length > 0){
                            result.fieldPrefix = fieldPrefix;
                        }
                    }
                    break;
                case "string":
                    if (!isBeforeComma && !isBeforeBinaryOperation && !result.fieldName) {
                        const lastChar = token.value[token.value.length - 1];
                        if (lastChar === "'" ||
                            lastChar === '"') {
                            const indexName = token.value.substr(1, token.value.length - 2);
                            result.fieldName = indexName;
                        } else {
                            // const partialIndexName = token.value.substr(1);
                            // do nothing with it as of now
                        }
                    }
                    break;
                case "paren.lparen":
                case "paren.lparen.whereFunction":
                    if (!isBeforeBinaryOperation) {
                        result.parentheses++;
                        
                        if (token.type === "paren.lparen.whereFunction") {
                            result.whereFunctionParameters++;
                        }
                        
                        if (isBeforeComma){
                            isBeforeComma = false;
                            result.dividersCount = 0;
                        } 
                    }
                    break;
                case "paren.rparen":
                case "paren.rparen.whereFunction":
                    if (!isBeforeComma && !isBeforeBinaryOperation) {
                        result.parentheses--;

                        if (!lastToken || lastToken.type !== "space") {
                            result.dividersCount++;
                            lastToken = {type: "space", start: null, index: null, value: null};
                            continue;
                        }
                    }
                    break;
                case "space":
                    if (isBeforeComma) {
                        afterCommaDividersCount++;
                    }
                    else if (!isBeforeBinaryOperation && !result.keyword) {
                        if (!lastToken || lastToken.type !== "space") {
                            result.dividersCount++;
                        }
                    }
                    break;
                case "comma":
                    if (!isBeforeComma && !isBeforeBinaryOperation) {
                        isBeforeComma = true;

                        if (!lastToken || lastToken.type !== "space") {
                            result.dividersCount++;
                        }
                    }
                    break;
                case "comma.whereFunction":
                    if (!result.whereFunction) {
                        result.whereFunctionParameters++;
                    }
                    break;
                case "operator.where":
                    if (!isBeforeComma && !isBeforeBinaryOperation && result.fieldName) {
                        result.fieldName = null;
                    }
                    break;
            }
            
            lastToken = token;
        } while (iterator.stepBackward());

        return null;
    }

    private completeFields(additions: autoCompleteWordList[] = null): void {
        const queryInfo = this.lastKeyword.info;
        if (!queryInfo.collection && !queryInfo.index) {
            return this.completeError("no collection or index were specified - cannot show fields");
        }

        if (queryInfo.aliases && !this.lastKeyword.fieldPrefix){
            const collections: autoCompleteWordList[] = [];
            let i = 1;
            _.forOwn(queryInfo.aliases, (value, key) => {
                collections.push({
                    caption: key,
                    value: key + ".",
                    score: 1000 - i++,
                    meta: value
                });
            });
            if (additions) {
                collections.push(...additions);
            }
            return this.callback(null, collections);
        }
        
        this.getIndexFields(additions)
            .done((wordList) => {
                this.callback(null, wordList);
            });
    }

    private completeWhereFunctionParameters() {
        if (this.lastKeyword.whereFunctionParameters === 1) {
            return this.completeFields();
        }

        switch (this.lastKeyword.whereFunction) {
            case "search":
                switch (this.lastKeyword.whereFunctionParameters) {
                    case 2:
                        return this.completeError("todo: show terms here?"); // TODO
                    case 3:
                        return this.completeWords(queryCompleter.binaryOperationsList);
                }
        }

        return this.completeError("empty completion");
    }

    private trimValue(value: string) {
        return _.trim(value, "'\"")
    }

    complete(editor: AceAjax.Editor,
             session: AceAjax.IEditSession,
             pos: AceAjax.Position,
             prefix: string,
             callback: (errors: any[], wordList: autoCompleteWordList[]) => void) {

        this.session = session;
        this.callback = callback;
        const lastKeyword = this.lastKeyword = this.getLastKeyword(pos);
        if (!lastKeyword) {
            return this.completeEmpty();
        }
        if (!lastKeyword.keyword) {
            if (lastKeyword.parentheses === 0) {
                return this.completeEmpty();
            }
            return this.completeError("empty completion");
        }
        
        switch (lastKeyword.keyword) {
            case "from":
            case "from index": {
                if (lastKeyword.dividersCount === 1) {
                    if (lastKeyword.keyword === "from") {
                        return this.completeFrom();
                    }

                    return this.providers.indexNames(names => {
                        return this.completeWords(names.map(name => ({
                            caption: name,
                            value: queryUtil.escapeCollectionOrFieldName(name) + " ",
                            score: 101,
                            meta: "index"
                        })));
                    });
                }
                if (lastKeyword.dividersCount === 0) {
                    return this.completeEmpty();
                }
                if (lastKeyword.dividersCount === 3 && lastKeyword.asSpecified) {
                    return this.completeError("empty completion");
                }

                return this.completeFromEnd();
            }
            case "declare":
                return this.completeWords([
                    {caption: "function", value: "function ", score: 0, meta: "keyword"}
                ]);
            case "declare function":
                if (lastKeyword.parentheses === 0 && lastKeyword.dividersCount >= 1) {
                    return this.completeEmpty();
                }
                return this.completeError("empty completion");
            case "select": {
                if (lastKeyword.dividersCount === 1) {
                    return this.completeFields(queryCompleter.functionsList);
                }
                
                if (lastKeyword.dividersCount === 2 ||
                    (lastKeyword.dividersCount === 4 && lastKeyword.asSpecified)) {
                    
                    let additions = queryCompleter.separatorList;
                    if (!lastKeyword.asSpecified) {
                        additions = additions.concat(queryCompleter.asList);
                    }
                    return this.completeKeywordEnd(additions);
                }

                return this.completeError("empty completion");
            }
            case "group by": {
                if (lastKeyword.dividersCount === 0) {
                    return this.completeByKeyword();
                }
                if (lastKeyword.dividersCount === 1) {
                    return this.completeFields();
                }

                return this.completeKeywordEnd(queryCompleter.separatorList);
            }
            case "order by": {
                if (lastKeyword.dividersCount === 0) {
                    return this.completeByKeyword();
                }
                if (lastKeyword.dividersCount === 1) {
                    const additions: autoCompleteWordList[] = lastKeyword.fieldPrefix ? null : [
                        {caption: "score", value: "score() ", snippet: "score() ", score: 22, meta: "function"},// todo: snippet
                        {caption: "random", value: "random() ", snippet: "random() ", score: 21, meta: "function"} // todo: snippet
                    ];
                    return this.completeFields(additions);
                }

                let additions = queryCompleter.separatorList;
                if (lastKeyword.dividersCount === 2) {
                    additions = additions.concat(queryCompleter.orderBySortList);
                }
                return this.completeKeywordEnd(additions);
            }
            case "where": {
                if (lastKeyword.dividersCount === 4 ||
                    (lastKeyword.dividersCount === 0 && lastKeyword.binaryOperation) ||
                    (lastKeyword.dividersCount === 2 && lastKeyword.whereFunction)) {
                    const binaryOperations = this.rules.binaryOperations.map((binaryOperation, i) => {
                        return {caption: binaryOperation, value: binaryOperation + " ", score: 22 - i, meta: "binary operation"};
                    });
                    return this.completeKeywordEnd(binaryOperations);
                }
                if (lastKeyword.dividersCount === 0) {
                    return this.completeKeywordEnd();
                }
                if (lastKeyword.dividersCount > 4) {
                    return this.completeError("empty completion");
                }
                
                if (lastKeyword.dividersCount === 1) {
                    if (lastKeyword.whereFunction && lastKeyword.whereFunctionParameters > 0) {
                        return this.completeWhereFunctionParameters();
                    }
                    
                    if (lastKeyword.binaryOperation && !lastKeyword.notSpecified) {
                        return this.completeFields(queryCompleter.notAfterAndOrList);
                    }

                    return this.completeFields(queryCompleter.whereFunctionsList);
                }
                if (lastKeyword.dividersCount === 2) {
                    return callback(null, queryCompleter.whereOperators);
                }

                if (lastKeyword.dividersCount === 3) {
                    if (!lastKeyword.fieldName) {
                        return this.completeError("No field was specified");
                    }

                    if (!lastKeyword.info.collection && !lastKeyword.info.index) {
                        return this.completeError("no collection or index specified");
                    }
                    
                    if (lastKeyword.whereFunction) {
                        if (this.lastKeyword.parentheses == 0){
                            return this.completeError("in | should not complete anything");
                        }

                        return this.completeTerms();
                    } 

                    return this.completeTerms();
                }

                return this.completeError("Failed to complete");
            }
            case "load": {
                if (lastKeyword.dividersCount === 0) {
                    return this.completeKeywordEnd();
                }
                if (lastKeyword.dividersCount === 1) {
                    return this.completeFields();
                }
                if (lastKeyword.dividersCount === 2) {
                    return this.completeWords(queryCompleter.asList);
                }
                if (lastKeyword.dividersCount === 3) {
                    return this.completeError("empty completion");
                }

                let alias = lastKeyword.info.alias ? lastKeyword.info.alias + "." : "";
                const separator = {caption:",", value: ", ", score: 21, meta: "separator", snippet: ", " + alias + "${1:field} as ${2:alias} "};
                return this.completeKeywordEnd([separator]);
            }
            case "include": {
                if (lastKeyword.dividersCount === 0) {
                    return this.completeKeywordEnd();
                }
                if (lastKeyword.dividersCount === 1) {
                    return this.completeFields();
                }

                return this.completeError("empty completion");
            }
            case "group":
            case "order": {
                if (lastKeyword.dividersCount === 0) {
                    return this.completeKeywordEnd();
                }

                return this.completeByKeyword();
            }
        }
    }

    private completeError(error: string): void {
        this.callback([error], null);
    }

    private completeWords(keywords: autoCompleteWordList[]) {
        this.callback(null, keywords);
    }

    private completeEmpty() {
        const keywords: autoCompleteWordList[] = [
            {caption: "from", value: "from ", score: 3, meta: "clause", snippet: "from ${1:Collection} as ${2:alias}\r\n"},
            {caption: "from index", value: "from index ", score: 2, meta: "clause", snippet: "from index ${1:Index} as ${2:alias}\r\n"},
            {caption: "declare", value: "declare ", score: 1, meta: "JS function", snippet: `declare function \${1:Name}() {
    \${0}
}

`}
        ];
        this.completeWords(keywords);
    }

    private completeByKeyword() {
        const keywords = [{caption: "by", value: "by ", score: 21, meta: "keyword"}];
        this.completeWords(keywords);
    }

    private completeFrom() {
        this.providers.collections(collections => {
            const wordList: autoCompleteWordList[] = collections.map(name => {
                return {
                    caption: name, 
                    value: queryUtil.escapeCollectionOrFieldName(name) + " ",
                    score: 2,
                    meta: "collection"
                };
            });

            wordList.push(
                {caption: "index", value: "index ", score: 4, meta: "keyword"},
                {caption: "@all_docs", value: "@all_docs ", score: 3, meta: "collection"}
            );

            this.completeWords(wordList);
        });
    }

    private completeFromEnd() {
        if (this.lastKeyword.dividersCount === 2) {
            return this.completeKeywordEnd(queryCompleter.asList);
        }
        return this.completeKeywordEnd();
    }

    private completeKeywordEnd(additions: autoCompleteWordList[] = null) {
        const lastKeyword = this.lastKeyword;
        let keywordEncountered = false;
        const lastInitialKeyword = this.getInitialKeyword(lastKeyword);
        let position = 0;
        let projectionSelectPosition: number;

        const keywords: autoCompleteWordList[] = this.rules.clausesKeywords.filter(keyword => {
            if (keywordEncountered) {
                if (keyword === "group") { // group cluase is not shown when querying an index
                    return lastKeyword.keyword === "from";
                }
            } else if (lastInitialKeyword === keyword) {
                keywordEncountered = true;
                return lastKeyword.dividersCount === 0 && !lastKeyword.binaryOperation;
            }
            return keywordEncountered;
        }).filter(keyword => {
            if (keyword === "select" || keyword === "include") {
                return this.queryType === "Select";
            }
            if (keyword === "update") {
                return this.queryType === "Update";
            }
            return true;
        }).map(keyword => {
            const currentPosition = position++;
            if (keyword === "select") {
                projectionSelectPosition = position++;
            } else if (keyword === "load") {
                let alias = lastKeyword.info.alias ? lastKeyword.info.alias + "." : "";
                return {caption: keyword, value: keyword + " ", score: 20 - currentPosition, meta: "clause", snippet: "load " + alias + "${1:field} as ${2:alias} "};
            }
            return {caption: keyword, value: keyword + " ", score: 20 - currentPosition, meta: "keyword"};
        });

        if (projectionSelectPosition) {
            keywords.push({caption: "select {",value: "select { ", score: 20 - projectionSelectPosition, meta: "JS projection", snippet: `select {
    \${1:Name}: \${2:Value}
}
`});
        }
        
        if (additions) {
            keywords.push(...additions);
        }

        this.callback(null, keywords);
    }

    private completeTerms() {
        const lastKeyword = this.lastKeyword;
        this.getIndexFields()
            .done((wordList) => {
                let fieldName = this.trimValue(lastKeyword.fieldName);
                if (!wordList.find(x => x.caption === fieldName)) {
                    return this.completeError("Field not in the words list");
                }

                this.providers.terms(lastKeyword.info.index, lastKeyword.info.collection, fieldName, 20, terms => {
                    if (terms && terms.length) {
                        return this.completeWords(terms.map(term => ({
                            caption: term,
                            value: queryUtil.escapeCollectionOrFieldName(term) + " ",
                            score: 1,
                            meta: "term"
                        })));
                    }

                    return this.completeError("No terms");
                });
            });
    }
    
    static remoteCompleter(activeDatabase: KnockoutObservable<database>, indexes: KnockoutObservableArray<Raven.Client.Documents.Operations.IndexInformation>, queryType: rqlQueryType) {
        const providers: queryCompleterProviders = {
            terms: (indexName, collection, field, pageSize, callback) => {
                new getIndexTermsCommand(indexName, collection, field, activeDatabase(), pageSize)
                    .execute()
                    .done(terms => {
                        callback(terms.Terms);
                    });
            },
            collections: (callback) => {
                callback(collectionsTracker.default.getCollectionNames());
            },
            indexFields: (indexName, callback) => {
                new getIndexEntriesFieldsCommand(indexName, activeDatabase(), false)
                    .execute()
                    .done(result => {
                        callback(result.Static);
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
        };
        return new queryCompleter(providers, queryType);
    }

    private getInitialKeyword(lastKeyword: autoCompleteLastKeyword) {
        switch (lastKeyword.keyword){
            case "from index":
                return "from";
            case "group by":
                return "group";
            case "order by":
                return "order";
            default:
                return lastKeyword.keyword;
        }
    }
}

export = queryCompleter;

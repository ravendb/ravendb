/// <reference path="../../typings/tsd.d.ts" />

import collectionsTracker = require("common/helpers/database/collectionsTracker");
import getIndexTermsCommand = require("commands/database/index/getIndexTermsCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import database = require("models/resources/database");
import getIndexEntriesFieldsCommand = require("commands/database/index/getIndexEntriesFieldsCommand");
import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");

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

        const iterator: AceAjax.TokenIterator = new this.tokenIterator(this.session, 0, 0);
        do {
            const token = iterator.getCurrentToken();
            if (!token) {
                break
            }

            if (handleAfterNextToken){
                handleAfterNextToken = false;
                
                if (asSpecified) {
                    if (keyword === "load") {
                        result.aliases[identifier] = nestedFieldName;
                    }
                    else {
                        result.alias = identifier;
                        result.aliases = {};
                        result.aliases[identifier] = result.collection;
                    }
                }
                else if (keyword === "load") {
                    nestedFieldName = identifier;
                }
                else if (keyword === "from") {
                    result.collection = identifier;
                }
                else if (keyword === "index") {
                    result.index = identifier;
                }
                asSpecified = true;
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
                case "string":
                case "identifier": {
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
            key += lastKeyword.fieldPrefix.join(".");
            prefixSpecified = true;

            if (lastKeyword.info.aliases) {
                const last = lastKeyword.fieldPrefix.length - 1;
                const alias = lastKeyword.fieldPrefix[last];
                if (lastKeyword.info.aliases[alias]) {
                    lastKeyword.fieldPrefix.splice(last, 1);
                }
            }

            if (lastKeyword.fieldPrefix.length > 0) {
                prefixToSend = lastKeyword.fieldPrefix.reverse().join(".");
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
                    wordList.push(this.normalizeWord({caption: field, value: queryCompleter.escapeCollectionOrFieldName(field), score: 101, meta: "field"}));
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

                    wordList.push(this.normalizeWord({
                        caption: key,
                        value: queryCompleter.escapeCollectionOrFieldName(key),
                        score: 101,
                        meta: formattedFieldType + " field"
                    }));
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
    
    private getLastKeyword(pos: AceAjax.Position): autoCompleteLastKeyword {
        const mode = this.session.getMode();
        this.rules = <AceAjax.RqlHighlightRules>mode.$highlightRules;
        
        const result: autoCompleteLastKeyword = {
            info: this.extractQueryInfo(pos),
            keywordsBefore: undefined,
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
        let isFieldPrefixMode = 0;
        let isBeforeCommaOrBinaryOperation = false;

        let lastRow: number;
        let lastToken: AceAjax.TokenInfo;
        const iterator: AceAjax.TokenIterator = new this.tokenIterator(this.session, pos.row, pos.column);
        do {
            const row = iterator.getCurrentTokenRow();
            if (!isBeforeCommaOrBinaryOperation && lastRow && lastToken && lastToken.type !== "space" && row - lastRow < 0) {
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
                else if (token.type === "text") {
                    const firstToken = token.value.trim();
                    if (firstToken !== "" && firstToken !== "," && firstToken !== "." && firstToken !== "[].") {
                        lastToken = token;
                        continue;
                    }
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
                    
                    result.keywordsBefore = this.getKeywordsBefore(iterator);
                    return result;
                case "keyword.clause.clauseAppend":
                    result.keyword = token.value.toLowerCase();
                    break;
                case "keyword.asKeyword":
                    if (!isBeforeCommaOrBinaryOperation) {
                        result.asSpecified = true;
                    }
                    break;
                case "keyword.notKeyword":
                    if (!isBeforeCommaOrBinaryOperation) {
                        result.notSpecified = true;
                        result.dividersCount--;
                    }
                    break;
                case "operations.type.binary":
                    if (!isBeforeCommaOrBinaryOperation && !result.binaryOperation) {
                        result.binaryOperation = token.value.toLowerCase();
                        isBeforeCommaOrBinaryOperation = true;
                    }
                    break;
                case "function.where":
                    if (!isBeforeCommaOrBinaryOperation) {
                        result.whereFunction = token.value.toLowerCase();
                    }
                    break;
                case "identifier":
                case "identifier.whereFunction":
                    if (!isBeforeCommaOrBinaryOperation) {
                        if (isFieldPrefixMode === 1) {
                            result.fieldPrefix.push(token.value);
                        } else if(!result.fieldName) {
                            result.fieldName = token.value;
                        }
                    }
                    break;
                case "string":
                    if (!isBeforeCommaOrBinaryOperation && !result.fieldName) {
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
                    if (!isBeforeCommaOrBinaryOperation) {
                        result.parentheses++;
                        
                        if (token.type === "paren.lparen.whereFunction") {
                            result.whereFunctionParameters++;
                        }
                    }
                    break;
                case "paren.rparen":
                case "paren.rparen.whereFunction":
                    if (!isBeforeCommaOrBinaryOperation) {
                        if (token.type === "paren.rparen" && token.value === "}" && result.parentheses === 0) {
                            result.keywordsBefore = this.getKeywordsBefore(iterator); // todo: do we need this?
                            return result;
                        }

                        result.parentheses--;

                        if (!lastToken || lastToken.type !== "space") {
                            result.dividersCount++;
                            lastToken = {type: "space", start: null, index: null, value: null};
                            continue;
                        }
                    }
                    break;
                case "space":
                    if (!isBeforeCommaOrBinaryOperation && !result.keyword) {
                        if (!lastToken || lastToken.type !== "space") {
                            result.dividersCount++;
                        }

                        if (isFieldPrefixMode === 1) {
                            isFieldPrefixMode = 2;
                        }
                    }
                    break;
                case "text":
                    if (!isBeforeCommaOrBinaryOperation && !result.whereFunction) {
                        const text = token.value;
                        if (isFieldPrefixMode === 0 && (text === "." || text === "[].")) { // TODO: Intorudce regex rule for fieldPrefix /(?:.|[].)/
                            isFieldPrefixMode = 1;
                            result.fieldPrefix = [];
                        }
                    }
                    break;
                case "comma":
                    if (!isBeforeCommaOrBinaryOperation) {
                        isBeforeCommaOrBinaryOperation = true;

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
                        return this.completeWords([
                            {value: "or", score: 22, meta: "any term"},
                            {value: "and", score: 21, meta: "all terms"}
                        ]);
                }
        }

        return this.completeError("empty completion");
    }

    complete(editor: AceAjax.Editor,
             session: AceAjax.IEditSession,
             pos: AceAjax.Position,
             prefix: string,
             callback: (errors: any[], wordList: autoCompleteWordList[]) => void) {

        this.session = session;
        this.callback = callback;
        const lastKeyword = this.lastKeyword = this.getLastKeyword(pos);
        if (!lastKeyword || !lastKeyword.keyword) {
            return this.completeEmpty();
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
                            value: queryCompleter.escapeCollectionOrFieldName(name),
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
                    {value: "function", score: 0, meta: "keyword"}
                ]);
            case "declare function":
                if (lastKeyword.parentheses === 0 && lastKeyword.dividersCount >= 1) {
                    return this.completeEmpty();
                }
                return this.completeError("empty completion");
            case "select": {
                if (lastKeyword.dividersCount >= 2) {
                    if (!lastKeyword.asSpecified) {
                        return this.completeWords([{value: "as", score: 3, meta: "keyword"}]);
                    }

                    return this.completeError("empty completion");
                }

                return this.completeFields(queryCompleter.functionsList);
            }
            case "group by": {
                if (lastKeyword.dividersCount === 0) {
                    return this.completeByKeyword();
                }
                if (lastKeyword.dividersCount === 1) {
                    return this.completeFields();
                }

                const keywords = [
                    {value: ",", score: 23, meta: "separator"}
                ];
                return this.completeKeywordEnd(keywords);
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

                const keywords = [
                    {value: ",", score: 23, meta: "separator"}
                ];
                if (lastKeyword.dividersCount === 2) {
                    keywords.push(
                        {value: "desc", score: 22, meta: "descending sort"},
                        {value: "asc", score: 21, meta: "ascending sort"}
                    );
                }
                return this.completeKeywordEnd(keywords);
            }
            case "where": {
                if (lastKeyword.dividersCount === 4 ||
                    (lastKeyword.dividersCount === 0 && lastKeyword.binaryOperation) ||
                    (lastKeyword.dividersCount === 2 && lastKeyword.whereFunction)) {
                    const binaryOperations = this.rules.binaryOperations.map((binaryOperation, i) => {
                        return {value: binaryOperation, score: 22 - i, meta: "binary operation"};
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
                
                if (true) { // TODO: refactor this
                    if (!lastKeyword.fieldName) {
                        return this.completeError("No field was specified");
                    }

                    const collection = this.lastKeyword.info.collection;
                    const index = this.lastKeyword.info.index;
                    if (!collection && !index) {
                        return this.completeError("no collection or index specified");
                    }

                    this.getIndexFields()
                        .done((wordList) => {
                            if (!wordList.find(x => x.value === lastKeyword.fieldName)) {
                                return this.completeError("Field not in the words list");
                            }

                            let currentValue: string = "";

                            /* TODO: currentValue = currentToken.value.trim();
                             const rowTokens: any[] = session.getTokens(pos.row);
                             if (!!rowTokens && rowTokens.length > 1) {
                             currentColumnName = rowTokens[rowTokens.length - 2].value.trim();
                             currentColumnName = currentColumnName.substring(0, currentColumnName.length - 1);
                             }*/


                            // for non dynamic indexes query index terms, for dynamic indexes, try perform general auto complete
                            if (index) {
                                this.providers.terms(index, lastKeyword.fieldName, 20, terms => {
                                    if (terms && terms.length) {
                                        return this.completeWords(terms.map(term => ({
                                                caption: term,
                                                value: queryCompleter.escapeCollectionOrFieldName(term),
                                                score: 1,
                                                meta: "value"
                                            })));
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
                                    return this.completeError("empty completion");
                                }*/
                            }
                        });
                }

                return;
            }
            case "load": {
                if (lastKeyword.dividersCount === 0) {
                    return this.completeKeywordEnd();
                }

                return this.completeFields();
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
        this.callback(null, keywords.map(keyword => {
            return this.normalizeWord(keyword);
        }));
    }

    private normalizeWord(keyword: autoCompleteWordList) {
        if (!keyword.caption) {
            keyword.caption = _.trim(keyword.value, "'");
        }
        if (keyword.meta === "function") {
            keyword.value += "(";
        } else {
            keyword.value += " "; // insert space after each completed keyword or other value.
        }
        return keyword;
    }

    private static escapeCollectionOrFieldName(name: string) : string {
        // wrap collection name in 'collection name' if it has spaces.
        if (/^[0-9a-zA-Z_@]+$/.test(name)){
            return name;
        }

        // escape ' char
        if (name.includes("'")){
            name = name.replace("'", "''")
        }
        return "'" + name + "'";
    }

    private completeEmpty() {
        const keywords: autoCompleteWordList[] = [
            {value: "from", score: 3, meta: "clause", snippet: "from ${1:Collection} as ${2:alias}\r\n"},
            {value: "from index", score: 2, meta: "clause", snippet: "from index ${1:Index} as ${2:alias}\r\n"},
            {value: "declare", score: 1, meta: "JS function", snippet: `declare function \${1:Name}() {
    \${0}
}

`}
        ];
        this.completeWords(keywords);
    }

    private completeByKeyword() {
        const keywords = [{value: "by", score: 21, meta: "keyword"}];
        this.completeWords(keywords);
    }

    private completeFrom() {
        this.providers.collections(collections => {
            const wordList: autoCompleteWordList[] = collections.map(name => {
                return {
                    caption: name, 
                    value: queryCompleter.escapeCollectionOrFieldName(name),
                    score: 2,
                    meta: "collection"
                };
            });

            wordList.push(
                {value: "index", score: 4, meta: "keyword"},
                {value: "@all_docs", score: 3, meta: "collection"}
            );

            this.completeWords(wordList);
        });
    }

    private completeFromEnd() {
        const keywords: autoCompleteWordList[] = [];
        if (this.lastKeyword.dividersCount === 2) {
            keywords.push({value: "as", score: 21, meta: "keyword"});
        }
        this.completeKeywordEnd(keywords);
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
                let alias = lastKeyword.info.alias;
                if (!alias) {
                    alias = "alias";
                }
                return {value: keyword, score: 20 - currentPosition, meta: "clause", snippet: "load " + alias + ".${1:field} as ${2:alias} "};
            }
            return {value: keyword, score: 20 - currentPosition, meta: "keyword"};
        });

        if (projectionSelectPosition) {
            keywords.push({value: "select {", score: 20 - projectionSelectPosition, meta: "JS projection", snippet: `select {
    \${1:Name}: \${2:Value}
}
`});
        }
        
        if (additions) {
            keywords.push(...additions);
        }

        this.completeWords(keywords);
    }
    
    static remoteCompleter(activeDatabase: KnockoutObservable<database>, indexes: KnockoutObservableArray<Raven.Client.Documents.Operations.IndexInformation>, queryType: rqlQueryType) {
        const providers: queryCompleterProviders = {
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

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
    private tokenIterator: new(session : AceAjax.IEditSession, initialRow: number, initialColumn: number) => AceAjax.TokenIterator = ace.require("ace/token_iterator").TokenIterator;
    private indexOrCollectionFieldsCache = new Map<string, autoCompleteWordList[]>();
    
    constructor(private providers: queryCompleterProviders, private queryType: rqlQueryType) {
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
        
        const taskResult = () => {
            if (!prefix) {
                wordList.push(
                    {caption: "ID", value: "ID() ", score: 11, meta: "document ID"}
                );
            }

            this.indexOrCollectionFieldsCache.set(key, wordList);
            fieldsTasks.resolve(wordList);
        };
        
        if (queryIndexType === "index") {
            this.providers.indexFields(queryIndexName, fields => {
                fields.map(field => {
                    wordList.push(this.normalizeWord({caption: field, value: queryCompleter.escapeCollectionOrFieldName(field), score: 101, meta: "field"}));
                });

                return taskResult();
            });
        } else {
            this.providers.collectionFields(queryIndexName, prefix, fields => {
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
    
    getLastKeyword(session: AceAjax.IEditSession, pos: AceAjax.Position): autoCompleteLastKeyword {
        const mode = session.getMode();
        this.rules = <AceAjax.RqlHighlightRules>mode.$highlightRules;
        
        const result: autoCompleteLastKeyword = {
            keywordsBefore: undefined,
            keyword: undefined,
            keywordModifier: undefined,
            binaryOperation: undefined,
            whereFunction: undefined,
            whereFunctionParameters: 0,
            fieldPrefix: undefined,
            get getFieldPrefix():string {
                return this.fieldPrefix ? this.fieldPrefix.join(".") : undefined;
            },
            identifiers: [],
            text: undefined, // TODO: Not needed anymore
            dividersCount: 0,
            parentheses: 0
        };
            
        let whereOperator = "";
        let liveAutoCompleteSkippedTriggerToken = false;
        let isFieldPrefixMode = 0;
        let isBeforeCommaOrBinaryOperation = false;

        let lastRow: number;
        let lastToken: AceAjax.TokenInfo;
        const iterator: AceAjax.TokenIterator = new this.tokenIterator(session, pos.row, pos.column);
        do {
            const row = iterator.getCurrentTokenRow();
            if (lastRow && lastToken && lastToken.type !== "space" && row - lastRow < 0) {
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
                case "keyword.insideClause":
                    if (result.identifiers.length > 0 || !result.keywordModifier) {
                        result.keywordModifier = token.value.toLowerCase();
                    }
                    break;
                case "operations.type.binary":
                    if (!isBeforeCommaOrBinaryOperation && !result.binaryOperation) {
                        result.binaryOperation = token.value.toLowerCase();
                        isBeforeCommaOrBinaryOperation = true;
                    }
                    break;
                case "function":
                    result.keywordsBefore = this.getKeywordsBefore(iterator);
                    result.keyword = "__function";
                    return result;
                case "operator.where":
                    if (!isBeforeCommaOrBinaryOperation) {
                        whereOperator = token.value;
                    }
                    break;
                case "function.where":
                    if (!isBeforeCommaOrBinaryOperation) {
                        result.whereFunction = token.value.toLowerCase();
                    }
                    break;
                case "identifier":
                    if (!isBeforeCommaOrBinaryOperation) {
                        if (isFieldPrefixMode === 1) {
                            result.fieldPrefix.push(token.value);
                        } else {
                            result.identifiers.push(token.value);
                        }
                    }
                    break;
                case "string":
                    if (!isBeforeCommaOrBinaryOperation) {
                        const lastChar = token.value[token.value.length - 1];
                        if (lastChar === "'" ||
                            lastChar === '"') {
                            const indexName = token.value.substr(1, token.value.length - 2);
                            result.identifiers.push(indexName);
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

                        if (!lastToken || lastToken.type !== "space") {
                            result.dividersCount++;
                        }

                        result.parentheses--;
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

                        if (result.identifiers.length > 0) {
                            result.text = token.value;
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

    private completeFields(session: AceAjax.IEditSession, prefix: string, callback: (errors: any[], wordList: autoCompleteWordList[]) => void,
                           additions: autoCompleteWordList[] = null): void {
        const queryIndexName = queryCompleter.extractIndexOrCollectionName(session);
        if (!queryIndexName) {
            return;
        }

        this.getIndexFields(queryIndexName.name, queryIndexName.type, prefix)
            .done((wordList) => {
                if (additions) {
                    wordList = wordList.concat(additions); // do not modify the original collection which is cached.
                }
                callback(null, wordList);
            });
    }

    private completeWhereFunctionParameters(lastKeyword: autoCompleteLastKeyword ,session: AceAjax.IEditSession,
                                            callback: (errors: any[], wordList: autoCompleteWordList[]) => void) {
        if (lastKeyword.whereFunctionParameters === 1) {
            this.completeFields(session, lastKeyword.getFieldPrefix, callback);
            return;
        }

        switch (lastKeyword.whereFunction) {
            case "search":
                switch (lastKeyword.whereFunctionParameters) {
                    case 2:
                        callback(["todo: show terms here?"], null); // TODO
                        return;
                    case 3:
                        this.completeWords(callback, [
                            {value: "or", score: 22, meta: "any term"},
                            {value: "and", score: 21, meta: "all terms"}
                        ]);
                        return;
                }
        }

        callback(["empty completion"], null);
        return;
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
            case "from":
            case "from index": {
                if (lastKeyword.dividersCount === 1) {
                    if (lastKeyword.keyword === "from") {
                        this.completeFrom(callback);
                    }
                    else {
                        this.providers.indexNames(names => {
                            this.completeWords(callback, names.map(name => ({
                                caption: name,
                                value: queryCompleter.escapeCollectionOrFieldName(name),
                                score: 101,
                                meta: "index"
                            })));
                        });
                    }
                    return;
                }
                if (lastKeyword.dividersCount === 0) {
                    this.completeEmpty(callback);
                    return;
                }
                if (lastKeyword.dividersCount === 3 && lastKeyword.keywordModifier === "as") {
                    callback(["empty completion"], null);
                    return;
                }

                this.completeFromEnd(callback, lastKeyword);
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
                if (lastKeyword.parentheses === 0 && lastKeyword.dividersCount >= 1) {
                    this.completeEmpty(callback);
                }
                break;
            case "select": {
                if (lastKeyword.identifiers.length > 0 && lastKeyword.dividersCount >= 2) {
                    if (!lastKeyword.keywordModifier) {
                        this.completeWords(callback, [{value: "as", score: 3, meta: "keyword"}]);
                    }

                    return;
                }

                this.completeFields(session, lastKeyword.getFieldPrefix, callback);
                break;
            }
            case "group by": {
                if (lastKeyword.dividersCount === 0) {
                    this.completeByKeyword(callback);
                    return;
                }
                if (lastKeyword.dividersCount === 1) {
                    this.completeFields(session, lastKeyword.getFieldPrefix, callback);
                    return;
                }

                const keywords = [
                    {value: ",", score: 23, meta: "separator"}
                ];
                this.completeKeywordEnd(callback, lastKeyword, keywords);
                return;
            }
            case "order by": {
                if (lastKeyword.dividersCount === 0) {
                    this.completeByKeyword(callback);
                    return;
                }
                if (lastKeyword.dividersCount === 1) {
                    const additions: autoCompleteWordList[] = lastKeyword.fieldPrefix ? null : [
                        {caption: "score", value: "score() ", snippet: "score() ", score: 22, meta: "function"},// todo: snippet
                        {caption: "random", value: "random() ", snippet: "random() ", score: 21, meta: "function"} // todo: snippet
                    ];
                    this.completeFields(session, lastKeyword.getFieldPrefix, callback, additions);
                    return;
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
                this.completeKeywordEnd(callback, lastKeyword, keywords);
                return;
            }
            case "where": {
                if (lastKeyword.dividersCount === 4 ||
                    (lastKeyword.dividersCount === 0 && lastKeyword.binaryOperation) ||
                    (lastKeyword.dividersCount === 2 && lastKeyword.whereFunction)) {
                    const binaryOperations = this.rules.binaryOperations.map((binaryOperation, i) => {
                        return {value: binaryOperation, score: 22 - i, meta: "binary operation"};
                    });
                    this.completeKeywordEnd(callback, lastKeyword, binaryOperations);
                    return;
                }
                if (lastKeyword.dividersCount === 0) {
                    this.completeKeywordEnd(callback, lastKeyword);
                    return;
                }
                if (lastKeyword.dividersCount > 4) {
                    callback(["empty completion"], null);
                    return;
                }
                
                if (lastKeyword.dividersCount === 1) {
                    if (lastKeyword.whereFunction && lastKeyword.whereFunctionParameters > 0) {
                        this.completeWhereFunctionParameters(lastKeyword, session, callback);
                        return;
                    }
                    
                    const additions: autoCompleteWordList[] = [
                        {caption: "search", value: "search ", snippet: "search(${1:alias.Field.Name}, ${2:'*term1* term2*'}, ${3:or}) ", score: 21, meta: "function"}
                    ];
                    this.completeFields(session, lastKeyword.getFieldPrefix, callback, additions);
                    return;
                }
                if (lastKeyword.dividersCount === 2) {
                    const whereOperators = this.rules.whereOperators.map((operator, i) => {
                        return {value: operator, score: 40 - i, meta: "operator"};
                    });
                    this.completeWords(callback, whereOperators);
                    return;
                }
                
                if (true) { // TODO: refactor this
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
                                            terms.map(term => ({
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
                                    callback([{error: "notext"}], null);
                                }*/
                            }
                        });
                }

                return;
            }
            case "load": {
                if (lastKeyword.dividersCount === 0) {
                    this.completeKeywordEnd(callback, lastKeyword);
                    return;
                }

                callback(["empty completion"], null);
                return;
            }
            case "include": {
                if (lastKeyword.dividersCount === 0) {
                    this.completeKeywordEnd(callback, lastKeyword);
                    return;
                }
                if (lastKeyword.dividersCount === 1) {
                    this.completeFields(session, lastKeyword.getFieldPrefix, callback);
                    return;
                }

                callback(["empty completion"], null);
                return;
            }
            case "group":
            case "order": {
                if (lastKeyword.dividersCount === 0) {
                    this.completeKeywordEnd(callback, lastKeyword);
                    return;
                }

                this.completeByKeyword(callback);
                return;
            }
        }
    }

    private completeWords(callback: (errors: any[], wordList: autoCompleteWordList[]) => void, keywords: autoCompleteWordList[]) {
        callback(null, keywords.map(keyword => {
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

    private completeEmpty(callback: (errors: any[], wordList: autoCompleteWordList[]) => void) {
        const keywords: autoCompleteWordList[] = [
            {value: "from", score: 3, meta: "clause", snippet: "from ${1:Collection} as ${2:alias}\r\n"},
            {value: "from index", score: 2, meta: "clause", snippet: "from index ${1:Index} as ${2:alias}\r\n"},
            {value: "declare", score: 1, meta: "custom function", snippet: `declare function \${1:Name}() {
    \${0}
}

`}
        ];
        this.completeWords(callback, keywords);
    }

    private completeByKeyword(callback: (errors: any[], wordList: autoCompleteWordList[]) => void) {
        const keywords = [{value: "by", score: 21, meta: "keyword"}];
        this.completeWords(callback, keywords);
    }

    private completeFrom(callback: (errors: any[], wordList: autoCompleteWordList[]) => void) {
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

            this.completeWords(callback, wordList);
        });
    }

    private completeFromEnd(callback: (errors: any[], wordList: autoCompleteWordList[]) => void, lastKeyword: autoCompleteLastKeyword) {
        const keywords: autoCompleteWordList[] = [];
        if (lastKeyword.dividersCount === 2) {
            keywords.push({value: "as", score: 21, meta: "keyword"});
        }
        this.completeKeywordEnd(callback, lastKeyword, keywords);
    }

    private completeKeywordEnd(callback: (errors: any[], wordList: autoCompleteWordList[]) => void, lastKeyword: autoCompleteLastKeyword, additions: autoCompleteWordList[] = null) {
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

        this.completeWords(callback, keywords);
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

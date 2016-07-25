import database = require("models/resources/database");
import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");
import indexDefinition = require("models/database/index/indexDefinition");
import getCollectionsCommand = require("commands/database/documents/getCollectionsCommand");
import getDocumentsByEntityNameCommand = require("commands/database/documents/getDocumentsByEntityNameCommand");
import pagedResultSet = require("common/pagedResultSet");

class indexAceAutoCompleteProvider {
    constructor(private activeDatabase: database, private editedIndex: KnockoutObservable<indexDefinition>) {
        
    }
    getCollectionAliasesInsideIndexText(session: any): { aliasKey: string; aliasValuePrefix: string; aliasValueSuffix: string }[] {
        var aliases: { aliasKey: string; aliasValuePrefix: string; aliasValueSuffix: string }[] = [];

        var curAliasKey: string = null;
        var curAliasValuePrefix: string = null;
        var curAliasValueSuffix: string = null;

        for (var curRow = 0; curRow < session.getLength(); curRow++) {
            var curRowTokens = session.getTokens(curRow);

            for (var curTokenInRow = 0; curTokenInRow < curRowTokens.length; curTokenInRow++) {
                if (curRowTokens[curTokenInRow].type == "from.alias") {
                    curAliasKey = curRowTokens[curTokenInRow].value.trim();
                }
                else if (!!curAliasKey) {
                    if (curRowTokens[curTokenInRow].type == "docs" || curRowTokens[curTokenInRow].type == "collections") {
                        curAliasValuePrefix = curRowTokens[curTokenInRow].value;
                    }
                    else if (curRowTokens[curTokenInRow].type == "collectionName") {
                        curAliasValueSuffix = curRowTokens[curTokenInRow].value;
                        aliases.push({ aliasKey: curAliasKey, aliasValuePrefix: curAliasValuePrefix.replace('.', '').trim(), aliasValueSuffix: curAliasValueSuffix.replace('.', '').trim() });

                        curAliasKey = null;
                        curAliasValuePrefix = null;
                        curAliasValueSuffix = null;
                    }
                }
            }
        }
        return aliases;
    }

    getIndexMapCollectionFieldsForAutocomplete(session: any,
        currentToken: AceAjax.TokenInfo): JQueryPromise<any> {
        var deferred = $.Deferred();

        var collectionAliases: { aliasKey: string; aliasValuePrefix: string; aliasValueSuffix: string }[]
            = this.getCollectionAliasesInsideIndexText(session);
        // find the matching alias and get list of fields
        if (collectionAliases.length > 0) {
            var matchingAliasKeyValue = collectionAliases.first(x => x.aliasKey.replace('.', '').trim() === currentToken.value.replace('.', '').trim());
            if (!!matchingAliasKeyValue) {
                // get list of fields according to it's collection's first row
                if (matchingAliasKeyValue.aliasValuePrefix.toLowerCase() === "docs") {
                    new getDocumentsByEntityNameCommand(new collection(matchingAliasKeyValue.aliasValueSuffix, this.activeDatabase), 0, 1)
                        .execute()
                        .done((result: pagedResultSet<any>) => {
                            if (!!result && result.totalResultCount > 0) {
                                var documentPattern: document = new document(result.items[0]);
                                deferred.resolve(documentPattern.getDocumentPropertyNames());
                            } else {
                                deferred.reject();
                            }
                        }).fail(() => deferred.reject());
                }
                // for now, we do not treat cases of nested types inside document
                else {
                    deferred.reject();
                }
            }
        } else {
            deferred.reject();
        }

        return deferred;
    }

    getIndexMapCompleterValues(editor: any, session: any, pos: AceAjax.Position): JQueryPromise<any> {
        /*
        var currentToken: AceAjax.TokenInfo = session.getTokenAt(pos.row, pos.column);
        var completedToken: AceAjax.TokenInfo;
        var TokenIterator = require("ace/token_iterator").TokenIterator;
        var curPosIterator = new TokenIterator(editor.getSession(), pos.row, pos.column);
        var prevToken = curPosIterator.stepBackward();

        var returnedDeferred = $.Deferred();
        var suggestionsArray = [];
        // validation: if is null or it's type is represented by a string
        if (!currentToken || typeof currentToken.type == "string") {
            // if in beginning of text or in free text token
            if (!currentToken || currentToken.type == "text") {
                suggestionsArray.push("from");
                suggestionsArray.push("docs");
            }
            // if it's a docs predicate, return all collections in the db
            else if (!!currentToken.value && (currentToken.type == "docs" || (!!prevToken && prevToken.type == "docs"))) {
                new getCollectionsCommand(this.activeDatabase)
                    .execute()
                    .done((collections: collection[]) => {
                        collections.forEach((curCollection: collection) =>
                            suggestionsArray.push(curCollection.name));
                        returnedDeferred.resolve(suggestionsArray);
                    })
                    .fail(() => returnedDeferred.reject());
            }
            // if it's a general "collection" predicate, return all fields from first document in the collection
            else if (currentToken.type == "collections" || currentToken.type == "collectionName") {
                if (currentToken.type == "collections") {
                    completedToken = currentToken;
                } else {
                    completedToken = prevToken;
                }

                this.getIndexMapCollectionFieldsForAutocomplete(session, completedToken)
                    .done(x=> returnedDeferred.resolve(x))
                    .fail(() => returnedDeferred.reject());
            }
            else if (currentToken.type == "data.prefix" || currentToken.type == "data.suffix") {

                if (currentToken.type == "data.prefix") {
                    completedToken = currentToken;
                } else {
                    completedToken = prevToken;
                }

                var firstToken = session.getTokenAt(0, 0);
                // treat a "from [foo] in [bar] type of index syntax
                if (firstToken.value == "from") {
                    var aliases: { aliasKey: string; aliasValuePrefix: string; aliasValueSuffix: string }[] = this.getCollectionAliasesInsideIndexText(session);

                    this.getIndexMapCollectionFieldsForAutocomplete(session, completedToken)
                        .done(x=> returnedDeferred.resolve(x))
                        .fail(() => returnedDeferred.reject());
                }
                else {
                    returnedDeferred.resolve(["Methodical Syntax Not Supported"]);
                }
            } else {
                returnedDeferred.reject();
            }


        } else {
            returnedDeferred.reject();
        }

        return returnedDeferred;*/
        return null;
    }

    indexMapCompleter(editor: any, session: any, pos: AceAjax.Position, prefix: string, callback: (errors: any[], worldlist: { name: string; value: string; score: number; meta: string }[]) => void) {
        this.getIndexMapCompleterValues(editor, session, pos)
            .done((x: string[]) => {
                callback(null, x.map((val: string) => {
                    return { name: val, value: val, score: 100, meta: "suggestion" };
                }));
            })
            .fail(() => {
                callback([{ error: "notext" }], null);
            });
    }


    getIndexReduceCompleterValues(): string[] {
        var firstMapSrting = this.editedIndex().maps()[0]();

        var dotPrefixes = firstMapSrting.match(/[.]\w*/g);
        var equalPrefixes = firstMapSrting.match(/\w*\s*=\s*/g);

        var autoCompletes: string[] = [];

        if (!!dotPrefixes) {
            dotPrefixes.forEach(curPrefix => {
                autoCompletes.push(curPrefix.replace(".", "").trim());
            });
        }
        if (!!equalPrefixes) {
            equalPrefixes.forEach(curPrefix => {
                autoCompletes.push(curPrefix.replace("=", "").trim());
            });
        }

        return autoCompletes;
    }

    indexReduceCompleter(editor: any, session: any, pos: AceAjax.Position, prefix: string, callback: (errors: any[], worldlist: { name: string; value: string; score: number; meta: string }[]) => void) {

        var autoCompletes = this.getIndexReduceCompleterValues();
        callback(null, autoCompletes.map(curField => {
            return { name: curField, value: curField, score: 100, meta: "suggestion" };
        }));
    }
}

export = indexAceAutoCompleteProvider;

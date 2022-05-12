import database = require("models/resources/database");
import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");
import indexDefinition = require("models/database/index/indexDefinition");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");
import collectionsStats = require("models/database/documents/collectionsStats");

class indexAceAutoCompleteProvider {
    constructor(private activeDatabase: database, private editedIndex: KnockoutObservable<indexDefinition>) {
        _.bindAll(this, "indexMapCompleter", "indexReduceCompleter")
    
    }

    indexMapCompleter(editor: AceAjax.Editor, session: AceAjax.IEditSession, pos: AceAjax.Position, prefix: string, callback: (errors: any[], worldlist: { name: string; value: string; score: number; meta: string }[]) => void) {
        this.getIndexMapCompleterValues(editor, session, pos)
            .done((x: string[]) => {
                callback(null, x.map((val: string) => ({ name: val, value: val, score: 100, meta: "suggestion" })));
            })
            .fail(() => {
                callback([{ error: "notext" }], null);
            });
    }

    indexReduceCompleter(editor: AceAjax.Editor, session: AceAjax.IEditSession, pos: AceAjax.Position, prefix: string, callback: (errors: any[], worldlist: { name: string; value: string; score: number; meta: string }[]) => void) {
        const autoCompletes = this.getIndexReduceCompleterValues();
        callback(null, autoCompletes.map(curField => ({ name: curField, value: curField, score: 100, meta: "suggestion" })));
    }

    private getIndexMapCompleterValues(editor: AceAjax.Editor, session: AceAjax.IEditSession, pos: AceAjax.Position): JQueryPromise<string[]> {
        const currentToken: any = session.getTokenAt(pos.row, pos.column);
        let completedToken: AceAjax.TokenInfo;
        const tokenIterator = ace.require("ace/token_iterator").TokenIterator;
        const curPosIterator = new tokenIterator(editor.getSession(), pos.row, pos.column) as AceAjax.TokenIterator;
        const prevToken = curPosIterator.stepBackward() as any;

        const returnedDeferred = $.Deferred<string[]>();
        const suggestionsArray: Array<string> = [];
        // validation: if is null or it's type is represented by a string
        if (!currentToken || typeof currentToken.type == "string") {
            // if in beginning of text or in free text token
            if (!currentToken || currentToken.type === "text") {
                suggestionsArray.push("from");
                suggestionsArray.push("docs");
                returnedDeferred.resolve(suggestionsArray);
            }
            // if it's a docs predicate, return all collections in the db
            else if (!!currentToken.value && (currentToken.type === "docs" || (prevToken && prevToken.type === "docs"))) {
                new getCollectionsStatsCommand(this.activeDatabase)
                    .execute()
                    .done((collectionsStats: collectionsStats) => {
                        const collections = collectionsStats.collections;
                        collections.forEach((curCollection: collection) =>
                            suggestionsArray.push(curCollection.name));
                        returnedDeferred.resolve(suggestionsArray);
                    })
                    .fail(() => returnedDeferred.reject());
            }
            // if it's a general "collection" predicate, return all fields from first document in the collection
            else if (currentToken.type === "collections" || currentToken.type === "collectionName") {
                if (currentToken.type === "collections") {
                    completedToken = currentToken;
                } else {
                    completedToken = prevToken;
                }

                this.getIndexMapCollectionFieldsForAutocomplete(session, completedToken)
                    .done(x => returnedDeferred.resolve(x))
                    .fail(() => returnedDeferred.reject());
            } else if (currentToken.type === "punctuation.operator") {
                completedToken = prevToken;

                const firstToken = session.getTokenAt(0, 0);
                // treat a "from [foo] in [bar] type of index syntax
                if (firstToken.value === "from") {
                    this.getIndexMapCollectionFieldsForAutocomplete(session, completedToken)
                        .done(x => returnedDeferred.resolve(x))
                        .fail(() => returnedDeferred.reject());
                } else {
                    returnedDeferred.resolve(["Methodical Syntax Not Supported"]);
                }
            } else {
                returnedDeferred.reject();
            }
        } else {
            returnedDeferred.reject();
        }

        return returnedDeferred;
    }


    private getIndexMapCollectionFieldsForAutocomplete(session: AceAjax.IEditSession, currentToken: AceAjax.TokenInfo): JQueryPromise<string[]> {
        const deferred = $.Deferred<string[]>();

        const collectionAliases: { aliasKey: string; aliasValuePrefix: string; aliasValueSuffix: string }[] = this.getCollectionAliasesInsideIndexText(session);
        // find the matching alias and get list of fields
        if (collectionAliases.length > 0) {
            const matchingAliasKeyValue = collectionAliases.find(x => x.aliasKey.replace('.', '').trim() === currentToken.value.replace('.', '').trim());
            if (!!matchingAliasKeyValue) {
                // get list of fields according to it's collection's first row
                if (matchingAliasKeyValue.aliasValuePrefix.toLowerCase() === "docs") {
                    new collection(matchingAliasKeyValue.aliasValueSuffix, this.activeDatabase)
                        .fetchDocuments(0, 1)
                        .done((result: pagedResult<any>) => {
                            if (!!result && result.items.length > 0) {
                                const documentPattern: document = new document(result.items[0]);
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

    private getCollectionAliasesInsideIndexText(session: AceAjax.IEditSession): { aliasKey: string; aliasValuePrefix: string; aliasValueSuffix: string }[] {
        const aliases: { aliasKey: string; aliasValuePrefix: string; aliasValueSuffix: string }[] = [];

        let curAliasKey: string = null;
        let curAliasValuePrefix: string = null;
        let curAliasValueSuffix: string = null;

        for (let curRow = 0; curRow < session.getLength(); curRow++) {
            const curRowTokens = session.getTokens(curRow);

            for (let curTokenInRow = 0; curTokenInRow < curRowTokens.length; curTokenInRow++) {
                const currentToken = curRowTokens[curTokenInRow];
                if (currentToken.type == "from.alias") {
                    curAliasKey = curRowTokens[curTokenInRow].value.trim();
                } else if (!!curAliasKey) {
                    if (curRowTokens[curTokenInRow].type == "docs" || currentToken.type == "collections") {
                        curAliasValuePrefix = currentToken.value;
                    } else if (curRowTokens[curTokenInRow].type == "collectionName") {
                        curAliasValueSuffix = currentToken.value;
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

    private getIndexReduceCompleterValues(): string[] {
        const firstMapString = this.editedIndex().maps()[0].map();

        const dotPrefixes = firstMapString.match(/[.]\w*/g);
        const equalPrefixes = firstMapString.match(/\w*\s*=\s*/g);

        const autoCompletes: string[] = [];

        if (dotPrefixes) {
            dotPrefixes.forEach(curPrefix => {
                autoCompletes.push(curPrefix.replace(".", "").trim());
            });
        }
        if (equalPrefixes) {
            equalPrefixes.forEach(curPrefix => {
                autoCompletes.push(curPrefix.replace("=", "").trim());
            });
        }

        return autoCompletes;
    }
    
}

export = indexAceAutoCompleteProvider;

/// <reference path="../../typings/tsd.d.ts" />

import getIndexTermsCommand = require("commands/database/index/getIndexTermsCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import database = require("models/resources/database");

class queryUtil {

    public static queryCompleter(indexFields: KnockoutObservableArray<string>, selectedIndex: KnockoutObservable<string>, dynamicPrefix: string, activeDatabase: KnockoutObservable<database>, editor: any, session: any, pos: AceAjax.Position, prefix: string, callback: (errors: any[], worldlist: { name: string; value: string; score: number; meta: string }[]) => void) {
        var currentToken: AceAjax.TokenInfo = session.getTokenAt(pos.row, pos.column);

        /* TODO
        if (!currentToken || typeof currentToken.type === "string") {
            // if in beginning of text or in free text token
            if (!currentToken || currentToken.type === "text") {
                callback(null, indexFields().map(curColumn => {
                    return { name: curColumn, value: curColumn, score: 10, meta: "field" };
                }));
            } else if (currentToken.type === "keyword" || currentToken.type === "value") {
                // if right after, or a whitespace after keyword token ([column name]:)

                // first, calculate and validate the column name
                var currentColumnName: string = null;
                var currentValue: string = "";

                if (currentToken.type == "keyword") {
                    currentColumnName = currentToken.value.substring(0, currentToken.value.length - 1);
                } else {
                    currentValue = currentToken.value.trim();
                    var rowTokens: any[] = session.getTokens(pos.row);
                    if (!!rowTokens && rowTokens.length > 1) {
                        currentColumnName = rowTokens[rowTokens.length - 2].value.trim();
                        currentColumnName = currentColumnName.substring(0, currentColumnName.length - 1);
                    }
                }

                // for non dynamic indexes query index terms, for dynamic indexes, try perform general auto complete

                if (!!currentColumnName && !!indexFields.first(x=> x === currentColumnName)) {

                    if (selectedIndex().indexOf(dynamicPrefix) !== 0) {
                        new getIndexTermsCommand(selectedIndex(), currentColumnName, activeDatabase())
                            .execute()
                            .done(terms => {
                            if (!!terms && terms.length > 0) {
                                callback(null, terms.map(curVal => {
                                    return { name: curVal, value: curVal, score: 10, meta: "value" };
                                }));
                            }
                        });
                    } else {

                        if (currentValue.length > 0) {
                            new getDocumentsMetadataByIDPrefixCommand(currentValue, 10, activeDatabase())
                                .execute()
                                .done((results: string[]) => {
                                if (!!results && results.length > 0) {
                                    callback(null, results.map(curVal => {
                                        return { name: curVal["@metadata"]["@id"], value: curVal["@metadata"]["@id"], score: 10, meta: "value" };
                                    }));
                                }
                            });
                        } else {
                            callback([{ error: "notext" }], null);
                        }
                    }
                }
            }
        }*/
    }
    
}

export = queryUtil;

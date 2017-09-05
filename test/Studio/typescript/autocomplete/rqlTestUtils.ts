import queryCompleter = require("src/Raven.Studio/typescript/common/queryCompleter");
import aceEditorBindingHandler = require("src/Raven.Studio/typescript/common/bindingHelpers/aceEditorBindingHandler");

class rqlTestUtils {
    static autoComplete(query: string, queryCompleterProvider: () => queryCompleter, callback: (errors: any[], worldlist: autoCompleteWordList[], prefix: string) => void): void {
        const queryWoPosition = query.replace("|", "");
        const lines = query.split("\r\n");

        const lineWithCursor = lines.findIndex(x => x.includes("|"));
        if (lineWithCursor === -1) {
            throw new Error("Unable to find | in input query");
        }
        const rowWithCursor = lines[lineWithCursor].indexOf("|");
        if (rowWithCursor < 0) {
            throw "RQL test must include the position"
        }

        const element = $("<div></div>").html(queryWoPosition)[0];
        const aceEditor: AceAjax.Editor = ace.edit(element);

        const langTools = ace.require("ace/ext/language_tools");
        const util = ace.require("ace/autocomplete/util");
        aceEditorBindingHandler.customizeCompletionPrefix(util);

        aceEditor.setOption("enableBasicAutocompletion", true);
        aceEditor.setOption("enableLiveAutocompletion", true);
        aceEditor.setOption("newLineMode", "windows");
        aceEditor.getSession().setUseWorker(true);

        aceEditor.getSession().on("tokenizerUpdate", () => {
            const completer = queryCompleterProvider();

            const position = { row: lineWithCursor, column: rowWithCursor} as AceAjax.Position;
            aceEditor.selection.lead.column = position.column;
            aceEditor.selection.lead.row = position.row;

            const prefix = util.getCompletionPrefix(aceEditor);
            
            completer.complete(aceEditor, aceEditor.getSession(), aceEditor.getCursorPosition(), prefix, (errors: any[], wordlist: autoCompleteWordList[]) =>  {
                callback(errors, wordlist, prefix);
                aceEditor.destroy();
            });
        });

        setTimeout(() => {
            aceEditor.getSession().setMode("ace/mode/rql");
        }, 200);
    }
    
    static emptyProvider() {
        const completer = new queryCompleter({
            terms: (indexName, field, pageSize, callback) => callback([]),
            collections: callback => callback([]),
            indexFields: (indexName, callback) => callback([]),
            collectionFields: (collectionName, prefix, callback) => callback({}),
            indexNames: callback => callback([])
        });
        
        return () => completer;
    }

    static northwindProvider() {
        const completer = new queryCompleter({
            terms: (indexName, field, pageSize, callback) => callback([]),
            collections: callback => {
                callback([
                    "Regions", 
                    "Suppliers", 
                    "Employees", 
                    "Categories", 
                    "Products", 
                    "Shippers", 
                    "Companies", 
                    "Orders", 
                    "Collection With Space",
                    "Collection!"
                    //TODO: "Collection With ' And \" in name"
                ]);
            },
            indexFields: (indexName, callback) => {
                switch (indexName){
                    case "Orders/Totals":
                        callback([
                            "Employee",
                            "Company",
                            "Total"
                        ]);
                        break;
                    default:
                        callback([]);
                        break;
                }
            },
            collectionFields: (collectionName, prefix, callback) => {
                switch (collectionName){
                    case "Orders":
                        callback({
                            "Company": "String",
                            "Employee": "String",
                            "OrderedAt": "String",
                            "RequireAt": "String",
                            "ShippedAt": "String",
                            "ShipTo": "Object",
                            "ShipVia": "String",
                            "Freight": "Number",
                            "Lines": "ArrayObject",
                            "@metadata": "Object"
                        });
                        break;
                    default:
                        callback({});
                        break;
                }
                
            },
            indexNames: callback => callback([
                "Orders/ByCompany", 
                "Product/Sales", 
                "Orders/Totals", 
                // TODO: "Index With ' And \" in name"
                ])
        });

        return () => completer;
    }
}

export = rqlTestUtils;

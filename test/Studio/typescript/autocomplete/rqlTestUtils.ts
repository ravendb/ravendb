import queryCompleter = require("src/Raven.Studio/typescript/common/queryCompleter");

class rqlTestUtils {
    static autoComplete(query: string, queryCompleterProvider: () => queryCompleter, callback: (errors: any[], worldlist: autoCompleteWordList[]) => void): void {
        const queryWoPosition = query.replace("|", "");
        const lines = query.split("\r\n");

        const lineWithCursor = lines.findIndex(x => x.includes("|"));
        if (lineWithCursor === -1) {
            throw new Error("Unable to find | in input query");
        }
        const rowWithCursor = lines[lineWithCursor].indexOf("|");

        const element = $("<div></div>").html(queryWoPosition)[0];
        const aceEditor: AceAjax.Editor = ace.edit(element);

        const langTools = ace.require("ace/ext/language_tools");

        aceEditor.setOption("enableBasicAutocompletion", true);
        aceEditor.setOption("enableLiveAutocompletion", true);
        aceEditor.setOption("newLineMode", "windows");
        aceEditor.getSession().setUseWorker(true);

        aceEditor.getSession().on("tokenizerUpdate", () => {
            const completer = queryCompleterProvider();

            const position = { row: lineWithCursor, column: rowWithCursor} as AceAjax.Position;

            completer.complete(aceEditor, aceEditor.getSession(), position, "", (errors: any[], wordlist: autoCompleteWordList[]) =>  {
                callback(errors, wordlist);
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
            indexFields: (indexName, callback) => callback([]),
            collectionFields: (collectionName, prefix, callback) => callback({}),
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

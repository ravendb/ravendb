/// <reference path="../../typings/tsd.d.ts" />

class defaultAceCompleter {
    
    static completer(): autoCompleteCompleter {
        const langTools = ace.require("ace/ext/language_tools");

        const defaultCompleters = [langTools.snippetCompleter, langTools.textCompleter, langTools.keyWordCompleter];
        
        return (editor: AceAjax.Editor, session: AceAjax.IEditSession, pos: AceAjax.Position, prefix: string, callback: (errors: any[], wordlist: autoCompleteWordList[]) => void) => {
            let remaingCount = defaultCompleters.length;
            let matches = [] as Array<autoCompleteWordList>;
            defaultCompleters.forEach(completer => {
                completer.getCompletions(editor, session, pos, prefix, (localErrors: any[], localResults: autoCompleteWordList[]) => {
                    if (!localErrors && localResults)
                        matches = matches.concat(localResults);

                    remaingCount--;

                    if (!remaingCount) {
                        callback(null, matches);
                    }
                });
            });
        };
        
       
    }
}

export = defaultAceCompleter;

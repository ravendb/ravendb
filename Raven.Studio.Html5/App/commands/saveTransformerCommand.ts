/// <reference path="../models/dto.ts" />

import commandBase = require("commands/commandBase");
import getSingleTransformerCommand = require("commands/getSingleTransformerCommand");
import database = require("models/database");
import transformer = require("models/transformer");


class saveTransformerCommand extends commandBase {
    constructor(private trans: transformer, private db: database) {
        super();
    }

    execute(): JQueryPromise<transformer> {
        var doneTask = $.Deferred();
        
        var saveTransformerUrl = "/transformers/" + this.trans.name();
        var saveTransformerPutArgs = JSON.stringify(this.trans.toSaveDto());
        

        this.put(saveTransformerUrl, saveTransformerPutArgs, this.db)
            .fail((result:JQueryXHR)=> {
                
                this.reportError("Unable to save transformer", result.responseText, result.statusText);
                doneTask.reject(result);
            })
            .done((result: getTransformerResultDto) => {
                doneTask.resolve();
                new getSingleTransformerCommand(result.Transformer, this.db).execute()
                    .fail(xhr=> doneTask.reject(xhr))
                    .done((savedTransformer: savedTransformerDto) => {
                        this.reportInfo("Saved " + this.trans.name());
                        doneTask.resolve(new transformer().initFromSave(savedTransformer));
                });
        });
    
                

        return doneTask;

    }
    
}

export = saveTransformerCommand;
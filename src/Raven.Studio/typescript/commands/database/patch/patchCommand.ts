import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

interface patchCommandOptions {
    test: boolean;
}

class patchCommand extends commandBase {

    constructor(private queryStr: string, private db: database, private options: patchCommandOptions = null) {
        super();
        this.options = this.options || {
            test: false
        };
    }

    execute(): JQueryPromise<operationIdDto> {
        const args = {
            allowStale: true
        };

        const payload = {
            Query: {
                Query: this.queryStr
            }
        };
        
        let url = this.options.test ?
            endpoints.databases.queries.queriesTest :
            endpoints.databases.queries.queries;
            
        return this.patch<operationIdDto>(url + this.urlEncodeArgs(args), JSON.stringify(payload), this.db)
            .done((response: operationIdDto) => {
                if (!this.options.test) {
                    this.reportSuccess("Scheduled patch based on query");
                }
                
                return response;
            })
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to ${this.options.test ? 'test' : 'schedule' } patch`, response.responseText, response.statusText);
            });
    }

}

export = patchCommand; 

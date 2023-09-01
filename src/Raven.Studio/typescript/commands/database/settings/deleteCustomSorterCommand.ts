import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteCustomSorterCommand extends commandBase {
    private readonly db: database;
    private readonly name: string;

    constructor(db: database, name: string) {
        super();
        this.db = db;
        this.name = name;
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.name
        };
        
        const url = endpoints.databases.adminSorters.adminSorters + this.urlEncodeArgs(args);
        
        return this.del<void>(url, null, this.db)
            .done(() => {
                this.reportSuccess(`Deleted custom sorter: ${this.name}`);
            })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to delete custom sorter: " + this.name, response.responseText, response.statusText); 
            });
    }
}

export = deleteCustomSorterCommand; 


import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getSingleAuthTokenCommand extends commandBase {

    constructor(private db: database, private checkIfMachineAdmin :boolean = false) {
        super();
    }

    execute(): JQueryPromise<singleAuthToken> {
        let args: { CheckIfMachineAdmin: boolean } = null;

        if (this.checkIfMachineAdmin) {
            args = {
                CheckIfMachineAdmin:true
            };
        }
            
        return this.query(endpoints.databases.singleAuthToken.singleAuthToken, args, this.db);
    }
}

export = getSingleAuthTokenCommand;

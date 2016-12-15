import commandBase = require("commands/commandBase");
import resource = require("models/resources/resource");
import endpoints = require("endpoints");

class getSingleAuthTokenCommand extends commandBase {

    constructor(private resource: resource, private checkIfMachineAdmin :boolean = false) {
        super();
    }

    execute(): JQueryPromise<singleAuthToken> {
        var args: { CheckIfMachineAdmin: boolean } = null;

        if (this.checkIfMachineAdmin) {
            args = {
                CheckIfMachineAdmin:true
            };
        }
            
        return this.query(endpoints.databases.singleAuthToken.singleAuthToken, args, this.resource);
    }
}

export = getSingleAuthTokenCommand;

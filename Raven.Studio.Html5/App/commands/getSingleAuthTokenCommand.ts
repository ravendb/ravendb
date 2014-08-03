import commandBase = require("commands/commandBase");
import resource = require("models/resource");
import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");
import counterStorage = require("models/counter/counterStorage");

class getSingleAuthTokenCommand extends commandBase {

    constructor(private resourcePath: string, private checkIfMachineAdmin :boolean = false) {
        super();

        if (resourcePath == null) {
            throw new Error("Must specify resourcePath");
        }
    }

    execute(): JQueryPromise<singleAuthToken> {
        var url = this.resourcePath + "/singleAuthToken";
        var args = null;

        if (this.checkIfMachineAdmin) {
            args = {
                CheckIfMachineAdmin:true
            };
        }
            
        var getTask = this.query(url, args, null);

        return getTask;
    }
}

export = getSingleAuthTokenCommand;
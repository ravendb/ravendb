import commandBase = require("commands/commandBase");
import resource = require("models/resource");
import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");
import counterStorage = require("models/counter/counterStorage");

class getSingleAuthTokenCommand extends commandBase {

    constructor(private rs: resource) {
        super();

        if (!rs) {
            throw new Error("Must specify resource");
        }
    }

    execute(): JQueryPromise<singleAuthToken> {
        var url = this.getPrefixdForResourcePath() + this.rs.name + "/singleAuthToken";

        var getTask = this.query(url, null, null);

        return getTask;
    }

    private getPrefixdForResourcePath(): string {
        var path: string;

        if (this.rs instanceof database) {
            path = "/databases/";
        }
        else if (this.rs instanceof filesystem) {
            path = "/fs/";
        }
        else { // this.rs instanceof counterStorage
            path = "/counters/";
        }

        return path;
    }
}

export = getSingleAuthTokenCommand;
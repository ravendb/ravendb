import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class addNodeToClusterCommand extends commandBase {


    constructor(private serverUrl: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            url: this.serverUrl
        };
        const url = endpoints.global.rachisAdmin.adminClusterAddNode + this.urlEncodeArgs(args);

        return this.post(url, null, null, { dataType: undefined });
    }
}

export = addNodeToClusterCommand;

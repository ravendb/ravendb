import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class removeNodeFromClusterCommand extends commandBase {

    constructor(private nodeTag: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            nodeTag: this.nodeTag
        };
        const url = endpoints.global.rachisAdmin.adminClusterRemoveNode + this.urlEncodeArgs(args);

        return this.del<void>(url, null, null, { dataType: undefined });
    }
}

export = removeNodeFromClusterCommand;

import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getClusterNodeInfoCommand extends commandBase {

    constructor() {
        super();
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.Commands.NodeInfo> {
        const url = endpoints.global.rachisAdmin.clusterNodeInfo;

        return this.query(url, null);
    }
}

export = getClusterNodeInfoCommand;

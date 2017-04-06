import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import clusterTopology = require("models/database/cluster/clusterTopology");

class getClusterTopologyCommand extends commandBase {

    execute(): JQueryPromise<clusterTopology> {
        const url = endpoints.global.rachisAdmin.adminClusterTopology;

        return this.query(url, null, null, dto => new clusterTopology(dto));
    }
}

export = getClusterTopologyCommand;

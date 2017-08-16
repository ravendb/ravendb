/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTask = require("models/database/tasks/ongoingTaskModel"); 
import clusterTopologyManager = require("common/shell/clusterTopologyManager");

class ongoingTaskRavenEtlModel extends ongoingTask {

    // TODO...

    protected generateTaskName(dto: Raven.Client.ServerWide.Operations.OngoingTask): string {
        throw new Error("Method not implemented.");
    }
}

export = ongoingTaskRavenEtlModel;

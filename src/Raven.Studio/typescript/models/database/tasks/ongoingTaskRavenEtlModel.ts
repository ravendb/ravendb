/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTask = require("models/database/tasks/ongoingTaskModel"); 
import clusterTopologyManager = require("common/shell/clusterTopologyManager");

class ongoingTaskRavenEtlModel extends ongoingTask {
    
    // TODO... 
  
}

export = ongoingTaskRavenEtlModel;

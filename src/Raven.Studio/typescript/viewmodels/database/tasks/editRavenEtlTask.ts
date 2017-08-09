import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import saveExternalReplicationTaskCommand = require("commands/database/tasks/saveExternalReplicationTaskCommand");
import ongoingTaskReplication = require("models/database/tasks/ongoingTaskReplicationModel");
import ongoingTaskRavenEtl = require("models/database/tasks/ongoingTaskRavenEtlModel");
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import eventsCollector = require("common/eventsCollector");
import testClusterNodeConnectionCommand = require("commands/database/cluster/testClusterNodeConnectionCommand");

class editRavenEtlTask extends viewModelBase {
    
    editedRavenEtl = ko.observable<ongoingTaskRavenEtl>();

    // TODO...
    
}

export = editRavenEtlTask;
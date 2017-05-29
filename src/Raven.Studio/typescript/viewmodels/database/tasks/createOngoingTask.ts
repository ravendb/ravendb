import eventsCollector = require("common/eventsCollector");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import dialogViewModelBase = require("viewModels/dialogViewModelBase"); 


class createOngoingTask extends dialogViewModelBase {

    compositionComplete() {
        super.compositionComplete();
    }

    newReplicationTask(task: createOngoingTask) {
        eventsCollector.default.reportEvent("externalReplication", "new");
        const url = appUrl.forNewExternalReplication(this.activeDatabase());
        router.navigate(url);
        this.close();
    }

    urlForExternalReplication() {
       return appUrl.forNewExternalReplication(this.activeDatabase());
        
    }

    newEtlTask() {
        alert("NewEtlTask");
        // ...
    }

    newSqlEtlTask() {
        alert("NewSqlEtlTask");
        // ...
    }

    newBackupTask() {
        alert("NewBackupTask");
        // ...
    }

    newSubscriptionTask() {
        alert("NewSubscriptionTask");
        // ... 
    }
}

export = createOngoingTask;

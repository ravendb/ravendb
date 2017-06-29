import eventsCollector = require("common/eventsCollector");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase"); 


class createOngoingTask extends dialogViewModelBase {

    compositionComplete() {
        super.compositionComplete();
    }

    newReplicationTask(task: createOngoingTask) {
        eventsCollector.default.reportEvent("ExternalReplication", "new");
        const url = appUrl.forNewExternalReplication(this.activeDatabase());
        router.navigate(url);
        this.close();
    }

    newSubscriptionTask(task: createOngoingTask) {
        eventsCollector.default.reportEvent("Subscription", "new");
        const url = appUrl.forNewSubscription(this.activeDatabase());
        router.navigate(url);
        this.close();
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
}

export = createOngoingTask;

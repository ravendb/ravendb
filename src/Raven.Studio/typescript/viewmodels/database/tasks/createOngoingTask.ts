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
        const url = appUrl.forEditExternalReplication(this.activeDatabase());
        router.navigate(url);
        this.close();
    }

    newBackupTask() {
        eventsCollector.default.reportEvent("PeriodicBackup", "new");
        const url = appUrl.forEditPeriodicBackupTask(this.activeDatabase());
        router.navigate(url);
        this.close();
    }

    newSubscriptionTask(task: createOngoingTask) {
        eventsCollector.default.reportEvent("Subscription", "new");
        const url = appUrl.forEditSubscription(this.activeDatabase());
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
}

export = createOngoingTask;

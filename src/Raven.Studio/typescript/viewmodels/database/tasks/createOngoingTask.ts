import eventsCollector = require("common/eventsCollector");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase"); 


class createOngoingTask extends dialogViewModelBase {

    compositionComplete() {
        super.compositionComplete();
    }

    newReplicationTask() {
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

    newSubscriptionTask() {
        eventsCollector.default.reportEvent("Subscription", "new");
        const url = appUrl.forEditSubscription(this.activeDatabase());
        router.navigate(url);
        this.close();
    }

    newRavenEtlTask() {
        eventsCollector.default.reportEvent("RavenETL", "new");
        const url = appUrl.forEditRavenEtl(this.activeDatabase());
        router.navigate(url);
        this.close();
    }

    newSqlEtlTask() {
        eventsCollector.default.reportEvent("SqlETL", "new");
        const url = appUrl.forEditSqlEtl(this.activeDatabase());
        router.navigate(url);
        this.close();
    }

    newPullReplicationHubTask() {
        eventsCollector.default.reportEvent("PullReplicationHub", "new");
        const url = appUrl.forEditPullReplicationHub(this.activeDatabase());
        router.navigate(url);
        this.close();
    }
    
    newPullReplicationSinkTask() {
        eventsCollector.default.reportEvent("PullReplicationSink", "new");
        const url = appUrl.forEditPullReplicationSink(this.activeDatabase());
        router.navigate(url);
        this.close();
    }
}

export = createOngoingTask;

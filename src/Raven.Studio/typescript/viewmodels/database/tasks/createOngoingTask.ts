import eventsCollector = require("common/eventsCollector");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database from "models/resources/database"; 

class createOngoingTask extends dialogViewModelBase {

    view = require("views/database/tasks/createOngoingTask.html");
    
    private readonly db: database;
    
    constructor(db: database) {
        super();
        
        this.db = db;
    }

    newReplicationTask() {
        eventsCollector.default.reportEvent("ExternalReplication", "new");
        const url = appUrl.forEditExternalReplication(this.db);
        router.navigate(url);
        this.close();
    }

    newBackupTask() {
        eventsCollector.default.reportEvent("PeriodicBackup", "new");
        const url = appUrl.forEditPeriodicBackupTask(this.db);
        router.navigate(url);
        this.close();
    }

    newSubscriptionTask() {
        eventsCollector.default.reportEvent("Subscription", "new");
        const url = appUrl.forEditSubscription(this.db);
        router.navigate(url);
        this.close();
    }

    newRavenEtlTask() {
        eventsCollector.default.reportEvent("RavenETL", "new");
        const url = appUrl.forEditRavenEtl(this.db);
        router.navigate(url);
        this.close();
    }

    newSqlEtlTask() {
        eventsCollector.default.reportEvent("SqlETL", "new");
        const url = appUrl.forEditSqlEtl(this.db);
        router.navigate(url);
        this.close();
    }

    newOlapEtlTask() {
        eventsCollector.default.reportEvent("OlapETL", "new");
        const url = appUrl.forEditOlapEtl(this.db);
        router.navigate(url);
        this.close();
    }

    newElasticSearchEtlTask() {
        eventsCollector.default.reportEvent("ElasticSearchETL", "new");
        const url = appUrl.forEditElasticSearchEtl(this.db);
        router.navigate(url);
        this.close();
    }

    newKafkaEtlTask() {
        eventsCollector.default.reportEvent("KafkaETL", "new");
        const url = appUrl.forEditKafkaEtl(this.activeDatabase());
        router.navigate(url);
        this.close();
    }

    newRabbitMqEtlTask() {
        eventsCollector.default.reportEvent("RabbitMqETL", "new");
        const url = appUrl.forEditRabbitMqEtl(this.activeDatabase());
        router.navigate(url);
        this.close();
    }

    newReplicationHubTask() {
        eventsCollector.default.reportEvent("ReplicationHub", "new");
        const url = appUrl.forEditReplicationHub(this.db);
        router.navigate(url);
        this.close();
    }

    newReplicationSinkTask() {
        eventsCollector.default.reportEvent("ReplicationSink", "new");
        const url = appUrl.forEditReplicationSink(this.db);
        router.navigate(url);
        this.close();
    }
}

export = createOngoingTask;

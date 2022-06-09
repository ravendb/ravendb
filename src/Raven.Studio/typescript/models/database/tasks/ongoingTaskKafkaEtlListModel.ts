/// <reference path="../../../../typings/tsd.d.ts"/>
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import abstractOngoingTaskEtlListModel = require("models/database/tasks/abstractOngoingTaskEtlListModel");
import appUrl = require("common/appUrl");

class ongoingTaskKafkaEtlListModel extends abstractOngoingTaskEtlListModel {
    kafkaServerUrl = ko.observable<string>();
    connectionStringDefined = ko.observable<boolean>(true); // needed for template in the ongoing tasks list view
    
    get studioTaskType(): StudioTaskType {
        return "KafkaQueueEtl";
    }
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlListView) {
        super();

        this.update(dto);
        this.initializeObservables();

        this.connectionStringsUrl = appUrl.forConnectionStrings(activeDatabaseTracker.default.database(), "Kafka", this.connectionStringName());
    }

    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editKafkaEtl(this.taskId);
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlListView) {
        super.update(dto);

        this.connectionStringName(dto.ConnectionStringName);
        this.kafkaServerUrl(dto.Url);
    }
}

export = ongoingTaskKafkaEtlListModel;

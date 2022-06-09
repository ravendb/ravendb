/// <reference path="../../../../typings/tsd.d.ts"/>
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import abstractOngoingTaskEtlListModel = require("models/database/tasks/abstractOngoingTaskEtlListModel");
import appUrl = require("common/appUrl");

class ongoingTaskRabbitMqEtlListModel extends abstractOngoingTaskEtlListModel {
    connectionStringDefined = ko.observable<boolean>(true); // needed for template in the ongoing tasks list view
    
    get studioTaskType(): StudioTaskType {
        return "RabbitQueueEtl";
    }
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlListView) {
        super();

        this.update(dto);
        this.initializeObservables();

        this.connectionStringsUrl = appUrl.forConnectionStrings(activeDatabaseTracker.default.database(), "RabbitMQ", this.connectionStringName());
    }

    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editRabbitMqEtl(this.taskId);
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlListView) {
        super.update(dto);

        this.connectionStringName(dto.ConnectionStringName);
    }
}

export = ongoingTaskRabbitMqEtlListModel;

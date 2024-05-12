/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskQueueEtlEditModel = require("models/database/tasks/ongoingTaskQueueEtlEditModel");

class ongoingTaskAzureQueueStorageEtlEditModel extends ongoingTaskQueueEtlEditModel {
    get studioTaskType(): StudioTaskType {
        return "AzureQueueStorageQueueEtl";
    }

    get destinationType(): TaskDestinationType {
        return "Queue";
    }

    toDto(): Raven.Client.Documents.Operations.ETL.Queue.QueueEtlConfiguration {
        return super.toDto("AzureQueueStorage");
    }

    static empty(): ongoingTaskAzureQueueStorageEtlEditModel {
        return new ongoingTaskAzureQueueStorageEtlEditModel(
            {
                TaskName: "",
                TaskType: "QueueEtl",
                TaskState: "Enabled",
                TaskConnectionStatus: "Active",
                Configuration: {
                    EtlType: "Queue",
                    Transforms: [],
                    ConnectionStringName: "",
                    Name: ""
                },
            } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtl);
    }
}

export = ongoingTaskAzureQueueStorageEtlEditModel;

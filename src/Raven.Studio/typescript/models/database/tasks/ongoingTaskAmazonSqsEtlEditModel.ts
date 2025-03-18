/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskQueueEtlEditModel = require("models/database/tasks/ongoingTaskQueueEtlEditModel");

class ongoingTaskAmazonSqsEtlEditModel extends ongoingTaskQueueEtlEditModel {
    get studioTaskType(): StudioTaskType {
        return "AmazonSqsQueueEtl";
    }

    get destinationType(): TaskDestinationType {
        return "Queue";
    }

    toDto(): Raven.Client.Documents.Operations.ETL.Queue.QueueEtlConfiguration {
        return super.toDto("AmazonSqs");
    }

    static empty(): ongoingTaskAmazonSqsEtlEditModel {
        return new ongoingTaskAmazonSqsEtlEditModel(
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

export = ongoingTaskAmazonSqsEtlEditModel;

/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskQueueEtlEditModel = require("models/database/tasks/ongoingTaskQueueEtlEditModel");

class ongoingTaskRabbitMqEtlEditModel extends ongoingTaskQueueEtlEditModel {

    get studioTaskType(): StudioTaskType {
        return "RabbitQueueEtl";
    }

    get destinationType(): TaskDestinationType {
        return "Queue";
    }

    toDto(): Raven.Client.Documents.Operations.ETL.Queue.QueueEtlConfiguration {
        return super.toDto("RabbitMq");
    }

    static empty(): ongoingTaskRabbitMqEtlEditModel {
        return new ongoingTaskRabbitMqEtlEditModel(
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
            } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlDetails);
    }
}

export = ongoingTaskRabbitMqEtlEditModel;

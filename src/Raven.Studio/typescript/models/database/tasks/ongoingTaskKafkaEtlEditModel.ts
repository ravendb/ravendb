/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskQueueEtlEditModel = require("models/database/tasks/ongoingTaskQueueEtlEditModel");

class ongoingTaskKafkaEtlEditModel extends ongoingTaskQueueEtlEditModel {

    get studioTaskType(): StudioTaskType {
        return "KafkaQueueEtl";
    }

    toDto(): Raven.Client.Documents.Operations.ETL.Queue.QueueEtlConfiguration { 
        return super.toDto("Kafka");
    }
    
    static empty(): ongoingTaskKafkaEtlEditModel {
        return new ongoingTaskKafkaEtlEditModel(
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

export = ongoingTaskKafkaEtlEditModel;

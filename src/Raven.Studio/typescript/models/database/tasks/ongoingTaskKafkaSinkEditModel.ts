/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskQueueSinkEditModel from "models/database/tasks/ongoingTaskQueueSinkEditModel";

class ongoingTaskKafkaSinkEditModel extends ongoingTaskQueueSinkEditModel {

    get studioTaskType(): StudioTaskType {
        return "KafkaQueueSink";
    }

    get destinationType(): TaskDestinationType {
        return "Topic";
    }

    toDto(): Raven.Client.Documents.Operations.QueueSink.QueueSinkConfiguration { 
        return super.toDto("Kafka");
    }
    
    static empty(): ongoingTaskKafkaSinkEditModel {
        return new ongoingTaskKafkaSinkEditModel(
            {
                TaskName: "",
                TaskType: "QueueSink",
                TaskState: "Enabled", 
                TaskConnectionStatus: "Active",
                Configuration: {
                    TaskId: null,
                    BrokerType: "Kafka",
                    PinToMentorNode: false,
                    MentorNode: null,
                    Disabled: false,
                    Scripts: [],
                }
            } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueSinkDetails);
    }
}

export = ongoingTaskKafkaSinkEditModel;

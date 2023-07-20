/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskQueueSinkEditModel from "models/database/tasks/ongoingTaskQueueSinkEditModel";

class ongoingTaskRabbitMqSinkEditModel extends ongoingTaskQueueSinkEditModel {

    get studioTaskType(): StudioTaskType {
        return "RabbitQueueSink";
    }

    get destinationType(): TaskDestinationType {
        return "Topic";
    }

    toDto(): Raven.Client.Documents.Operations.QueueSink.QueueSinkConfiguration { 
        return super.toDto("RabbitMq");
    }
    
    static empty(): ongoingTaskRabbitMqSinkEditModel {
        return new ongoingTaskRabbitMqSinkEditModel(
            {
                TaskName: "",
                TaskType: "QueueSink",
                TaskState: "Enabled", 
                TaskConnectionStatus: "Active",
                Configuration: {
                    TaskId: null,
                    BrokerType: "RabbitMq",
                    PinToMentorNode: false,
                    MentorNode: null,
                    Disabled: false,
                    Scripts: [],
                }
            } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueSink);
    }
}

export = ongoingTaskRabbitMqSinkEditModel;

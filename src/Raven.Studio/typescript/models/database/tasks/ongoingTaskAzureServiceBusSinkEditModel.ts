/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskQueueSinkEditModel = require("models/database/tasks/ongoingTaskQueueSinkEditModel");

class ongoingTaskAzureServiceBusSinkEditModel extends ongoingTaskQueueSinkEditModel {

    get studioTaskType(): StudioTaskType {
        return "AzureServiceBusQueueSink";
    }

    get destinationType(): TaskDestinationType {
        return "Queue";
    }

    toDto(): Raven.Client.Documents.Operations.QueueSink.QueueSinkConfiguration {
        return super.toDto("AzureServiceBus");
    }

    static empty(): ongoingTaskAzureServiceBusSinkEditModel {
        return new ongoingTaskAzureServiceBusSinkEditModel(
            {
                TaskName: "",
                TaskType: "QueueSink",
                TaskState: "Enabled",
                TaskConnectionStatus: "Active",
                Configuration: {
                    TaskId: null,
                    BrokerType: "AzureServiceBus",
                    PinToMentorNode: false,
                    MentorNode: null,
                    Disabled: false,
                    Scripts: [],
                }
            } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueSink);
    }
}

export = ongoingTaskAzureServiceBusSinkEditModel;

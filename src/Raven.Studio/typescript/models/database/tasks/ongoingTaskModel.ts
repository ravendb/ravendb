/// <reference path="../../../../typings/tsd.d.ts"/>

import genUtils from "common/generalUtils";

abstract class ongoingTaskModel { 

    taskId: number;
    taskName = ko.observable<string>();
    
    taskType = ko.observable<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType>();   // raw type from server
    taskState = ko.observable<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState>();

    mentorNode = ko.observable<string>();
    responsibleNode = ko.observable<Raven.Client.ServerWide.Operations.NodeId>();
    
    taskConnectionStatus = ko.observable<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskConnectionStatus>();    
    
    badgeText: KnockoutComputed<string>;
    badgeClass: KnockoutComputed<string>;
    stateText: KnockoutComputed<string>;
    
    usingNotPreferredNode = ko.pureComputed(() => {
        const preferredMentor = this.mentorNode();
        const currentNode = this.responsibleNode() ? this.responsibleNode().NodeTag : null;
        
        return (preferredMentor && currentNode) ? preferredMentor !== currentNode : false;
    });
    
    abstract get studioTaskType(): StudioTaskType;
    
    static formatStudioTaskType(taskType: StudioTaskType): string {
        switch (taskType) {
            case "RavenEtl":
                return "RavenDB ETL";
            case "Replication":
                return "External Replication";
            case "SqlEtl":
                return "SQL ETL";
            case "OlapEtl":
                return "OLAP ETL";
            case "ElasticSearchEtl":
                return "Elasticsearch ETL";
            case "PullReplicationAsHub":
                return "Replication Hub";
            case "PullReplicationAsSink":
                return "Replication Sink";
            case "KafkaQueueEtl":
                return "Kafka ETL";
            case "RabbitQueueEtl":
                return "RabbitMQ ETL";
            default:
                return taskType;
        }
    }

    static getStudioTaskTypeFromServerType(taskListItem: Raven.Client.Documents.Operations.OngoingTasks.OngoingTask): StudioTaskType {
        if (taskListItem.TaskType === "QueueEtl") {
            const task = (taskListItem as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlListView);

            switch (task.BrokerType) {
                case "Kafka":
                    return "KafkaQueueEtl";
                case "RabbitMq":
                    return "RabbitQueueEtl";
                case "None":
                    throw new Error("Expected non-null broker type");
                default:
                    genUtils.assertUnreachable(task.BrokerType, "Unknown BrokerType: " + task.BrokerType);
            }
        }
        return taskListItem.TaskType;
    }

    static getServerEtlTypeFromStudioType(studioEtlType: StudioEtlType): Raven.Client.Documents.Operations.ETL.EtlType {
        if (studioEtlType === "Kafka" || studioEtlType === "RabbitMQ") {
            return "Queue";
        }
        
        return studioEtlType;
    }
    
    static getStudioEtlTypeFromServerType(serverEtlType: Raven.Client.Documents.Operations.ETL.EtlType, serverEtlSubType: string): StudioEtlType {
        switch (serverEtlType) {
            case "Raven": return "Raven";
            case "Sql": return "Sql";
            case "Olap": return "Olap";
            case "ElasticSearch": return "ElasticSearch";
            case "Queue": {
                switch (serverEtlSubType) {
                    case "Kafka": return "Kafka";
                    case "RabbitMq": return "RabbitMQ";
                    default: genUtils.assertUnreachable(serverEtlSubType as never, "Unknown serverEtlSubType: " + serverEtlType);
                }
            } break;
            
            default: genUtils.assertUnreachable(serverEtlType, "Unknown serverEtlType: " + serverEtlType);
        }
        
        return null;
    }

    static getStudioEtlTypeFromTaskType(studioTaskType: StudioTaskType): StudioEtlType | null {
        switch (studioTaskType) {
            case "RavenEtl": return "Raven";
            case "SqlEtl": return "Sql";
            case "OlapEtl": return "Olap";
            case "ElasticSearchEtl": return "ElasticSearch";
            case "KafkaQueueEtl": return "Kafka";
            case "RabbitQueueEtl": return "RabbitMQ";

            case "PullReplicationAsSink":
            case "PullReplicationAsHub":
            case "Replication":
            case "Backup":
            case "Subscription":
                return null;

            default: genUtils.assertUnreachable(studioTaskType, "Unknown studioTaskType: " + studioTaskType);
        }
        
        return null;
    }

    protected initializeObservables() {
        
        this.badgeClass = ko.pureComputed(() => {
            switch (this.taskConnectionStatus()) {
                case 'Active': {
                    return "state-success";
                }
                case 'Reconnect': {
                    return "state-warning";
                }
                case 'NotActive': {
                    return "state-warning";
                }
                case 'NotOnThisNode': {
                    return "state-offline";
                }
                case 'None': {
                    return "state-offline";
                }
            }
        });

        this.badgeText = ko.pureComputed(() => {

            switch (this.taskConnectionStatus()) {
                case 'Active': {
                    return "Active";
                }
                case 'Reconnect': {
                    return "Reconnect";
                }
                case 'NotActive': {
                    return "Not Active";
                }
                case 'NotOnThisNode': {
                    return "Not On Node";
                }
                case 'None': {
                    return "None";
                }
            }
        });

        this.stateText = ko.pureComputed(() => {
            if (this.taskState() === 'Enabled') {
                return "Enabled";
            }
            if (this.taskState() === "Disabled") {
                return "Disabled";
            }
            if (this.taskState() === "PartiallyEnabled") {
                return "Partial";
                // Relevant only for Etl tasks with some disabled scripts...
                // Todo: to be handled in issue 8880
            }
        });
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTask) {
        this.taskName(dto.TaskName);
        this.taskId = dto.TaskId;
        this.taskType(dto.TaskType);
        this.responsibleNode(dto.ResponsibleNode);
        this.taskState(dto.TaskState);
        this.taskConnectionStatus(dto.TaskConnectionStatus);
        this.mentorNode(dto.MentorNode);
    }
}

export = ongoingTaskModel;

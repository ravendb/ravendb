/// <reference path="../../../typings/tsd.d.ts" />

import database from "models/resources/database";
import getOngoingTasksCommand from "commands/database/tasks/getOngoingTasksCommand";
import deleteOngoingTaskCommand from "commands/database/tasks/deleteOngoingTaskCommand";
import toggleOngoingTaskCommand from "commands/database/tasks/toggleOngoingTaskCommand";
import etlProgressCommand from "commands/database/tasks/etlProgressCommand";
import { OngoingTaskSharedInfo } from "../models/tasks";
import TaskUtils from "../utils/TaskUtils";
import getManualBackupCommand from "commands/database/tasks/getManualBackupCommand";
import getOngoingTaskInfoCommand from "commands/database/tasks/getOngoingTaskInfoCommand";
import getSubscriptionConnectionDetailsCommand from "commands/database/tasks/getSubscriptionConnectionDetailsCommand";
import dropSubscriptionConnectionCommand from "commands/database/tasks/dropSubscriptionConnectionCommand";
import createSampleDataClassCommand from "commands/database/studio/createSampleDataClassCommand";
import createSampleDataCommand from "commands/database/studio/createSampleDataCommand";
import getCollectionsStatsCommand from "commands/database/documents/getCollectionsStatsCommand";
import collectionsStats from "models/database/documents/collectionsStats";
import getDatabaseForStudioCommand from "commands/resources/getDatabaseForStudioCommand";
import collectionsTracker from "common/helpers/database/collectionsTracker";
import testClusterNodeConnectionCommand from "commands/database/cluster/testClusterNodeConnectionCommand";
import testElasticSearchNodeConnectionCommand from "commands/database/cluster/testElasticSearchNodeConnectionCommand";
import testKafkaServerConnectionCommand from "commands/database/cluster/testKafkaServerConnectionCommand";
import testRabbitMqServerConnectionCommand from "commands/database/cluster/testRabbitMqServerConnectionCommand";
import testSqlConnectionStringCommand from "commands/database/cluster/testSqlConnectionStringCommand";
import deleteConnectionStringCommand from "commands/database/settings/deleteConnectionStringCommand";
import getConnectionStringsCommand from "commands/database/settings/getConnectionStringsCommand";
import saveConnectionStringCommand from "commands/database/settings/saveConnectionStringCommand";
import { ConnectionStringDto } from "components/pages/database/settings/connectionStrings/connectionStringsTypes";

export default class TasksService {
    async getOngoingTasks(db: database, location: databaseLocationSpecifier) {
        return new getOngoingTasksCommand(db, location).execute();
    }

    async dropSubscription(
        db: database,
        taskId: number,
        taskName: string,
        nodeTag: string = undefined,
        workerId: string = null
    ) {
        return new dropSubscriptionConnectionCommand(db, taskId, taskName, nodeTag, workerId).execute();
    }

    async getSubscriptionTaskInfo(
        db: database,
        taskId: number,
        taskName: string,
        nodeTag?: string
    ): Promise<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription> {
        return getOngoingTaskInfoCommand.forSubscription(db, taskId, taskName, nodeTag).execute();
    }

    async getSubscriptionConnectionDetails(db: database, taskId: number, taskName: string, nodeTag: string) {
        return new getSubscriptionConnectionDetailsCommand(db, taskId, taskName, nodeTag).execute();
    }

    async deleteOngoingTask(db: database, task: OngoingTaskSharedInfo) {
        const taskType = TaskUtils.studioTaskTypeToTaskType(task.taskType);
        return new deleteOngoingTaskCommand(db, taskType, task.taskId, task.taskName).execute();
    }

    async toggleOngoingTask(db: database, task: OngoingTaskSharedInfo, enable: boolean) {
        const taskType = TaskUtils.studioTaskTypeToTaskType(task.taskType);
        return new toggleOngoingTaskCommand(db, taskType, task.taskId, task.taskName, !enable).execute();
    }

    async getProgress(db: database, location: databaseLocationSpecifier) {
        return new etlProgressCommand(db, location, false).execute();
    }

    async getManualBackup(db: database) {
        return new getManualBackupCommand(db.name).execute();
    }

    async getSampleDataClasses(db: database): Promise<string> {
        return new createSampleDataClassCommand(db).execute();
    }

    async createSampleData(db: database): Promise<void> {
        return new createSampleDataCommand(db).execute().done(() => {
            if (!db.hasRevisionsConfiguration()) {
                new getDatabaseForStudioCommand(db.name).execute().done((dbInfo) => {
                    if (dbInfo.HasRevisionsConfiguration) {
                        db.hasRevisionsConfiguration(true);
                        collectionsTracker.default.configureRevisions(db);
                    }
                });
            }
        });
    }

    async fetchCollectionsStats(db: database): Promise<collectionsStats> {
        return new getCollectionsStatsCommand(db).execute();
    }

    async getConnectionStrings(db: database) {
        return new getConnectionStringsCommand(db).execute();
    }

    async saveConnectionString(db: database, connectionString: ConnectionStringDto) {
        return new saveConnectionStringCommand(db, connectionString).execute();
    }

    async deleteConnectionString(
        db: database,
        type: Raven.Client.Documents.Operations.ETL.EtlType,
        connectionStringName: string
    ) {
        return new deleteConnectionStringCommand(db, type, connectionStringName).execute();
    }

    async testClusterNodeConnection(serverUrl: string, databaseName?: string, bidirectional = true) {
        return new testClusterNodeConnectionCommand(serverUrl, databaseName, bidirectional).execute();
    }

    async testSqlConnectionString(db: database, connectionString: string, factoryName: string) {
        return new testSqlConnectionStringCommand(db, connectionString, factoryName).execute();
    }

    async testRabbitMqServerConnection(db: database, connectionString: string) {
        return new testRabbitMqServerConnectionCommand(db, connectionString).execute();
    }

    async testKafkaServerConnection(
        db: database,
        bootstrapServers: string,
        useServerCertificate: boolean,
        connectionOptionsDto: {
            [optionKey: string]: string;
        }
    ) {
        return new testKafkaServerConnectionCommand(
            db,
            bootstrapServers,
            useServerCertificate,
            connectionOptionsDto
        ).execute();
    }

    async testElasticSearchNodeConnection(
        db: database,
        serverUrl: string,
        authenticationDto: Raven.Client.Documents.Operations.ETL.ElasticSearch.Authentication
    ) {
        return new testElasticSearchNodeConnectionCommand(db, serverUrl, authenticationDto).execute();
    }
}

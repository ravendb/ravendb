/// <reference path="../../../typings/tsd.d.ts" />

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
import testClusterNodeConnectionCommand from "commands/database/cluster/testClusterNodeConnectionCommand";
import testElasticSearchNodeConnectionCommand from "commands/database/cluster/testElasticSearchNodeConnectionCommand";
import testKafkaServerConnectionCommand from "commands/database/cluster/testKafkaServerConnectionCommand";
import testRabbitMqServerConnectionCommand from "commands/database/cluster/testRabbitMqServerConnectionCommand";
import testSqlConnectionStringCommand from "commands/database/cluster/testSqlConnectionStringCommand";
import deleteConnectionStringCommand from "commands/database/settings/deleteConnectionStringCommand";
import getConnectionStringsCommand from "commands/database/settings/getConnectionStringsCommand";
import saveConnectionStringCommand from "commands/database/settings/saveConnectionStringCommand";
import { ConnectionStringDto } from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import getFolderPathOptionsCommand from "commands/resources/getFolderPathOptionsCommand";
import getBackupLocationCommand from "commands/database/tasks/getBackupLocationCommand";
import testAzureQueueStorageServerConnectionCommand from "commands/database/cluster/testAzureQueueStorageServerConnectionCommand";

export default class TasksService {
    async getOngoingTasks(databaseName: string, location: databaseLocationSpecifier) {
        return new getOngoingTasksCommand(databaseName, location).execute();
    }

    async dropSubscription(
        databaseName: string,
        taskId: number,
        taskName: string,
        nodeTag: string = undefined,
        workerId: string = null
    ) {
        return new dropSubscriptionConnectionCommand(databaseName, taskId, taskName, nodeTag, workerId).execute();
    }

    async getSubscriptionTaskInfo(
        databaseName: string,
        taskId: number,
        taskName: string,
        nodeTag?: string
    ): Promise<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription> {
        return getOngoingTaskInfoCommand.forSubscription(databaseName, taskId, taskName, nodeTag).execute();
    }

    async getSubscriptionConnectionDetails(databaseName: string, taskId: number, taskName: string, nodeTag: string) {
        return new getSubscriptionConnectionDetailsCommand(databaseName, taskId, taskName, nodeTag).execute();
    }

    async deleteOngoingTask(databaseName: string, task: OngoingTaskSharedInfo) {
        const taskType = TaskUtils.studioTaskTypeToTaskType(task.taskType);
        return new deleteOngoingTaskCommand(databaseName, taskType, task.taskId, task.taskName).execute();
    }

    async toggleOngoingTask(databaseName: string, task: OngoingTaskSharedInfo, enable: boolean) {
        const taskType = TaskUtils.studioTaskTypeToTaskType(task.taskType);
        return new toggleOngoingTaskCommand(databaseName, taskType, task.taskId, task.taskName, !enable).execute();
    }

    async getProgress(databaseName: string, location: databaseLocationSpecifier) {
        return new etlProgressCommand(databaseName, location, false).execute();
    }

    async getManualBackup(databaseName: string) {
        return new getManualBackupCommand(databaseName).execute();
    }

    async getSampleDataClasses(databaseName: string): Promise<string> {
        return new createSampleDataClassCommand(databaseName).execute();
    }

    async createSampleData(databaseName: string): Promise<void> {
        return new createSampleDataCommand(databaseName).execute();
    }

    async getDatabaseForStudio(databaseName: string) {
        return new getDatabaseForStudioCommand(databaseName).execute();
    }

    async fetchCollectionsStats(databaseName: string): Promise<collectionsStats> {
        return new getCollectionsStatsCommand(databaseName).execute();
    }

    async getConnectionStrings(databaseName: string) {
        return new getConnectionStringsCommand(databaseName).execute();
    }

    async saveConnectionString(databaseName: string, connectionString: ConnectionStringDto) {
        return new saveConnectionStringCommand(databaseName, connectionString).execute();
    }

    async deleteConnectionString(
        databaseName: string,
        type: Raven.Client.Documents.Operations.ETL.EtlType,
        connectionStringName: string
    ) {
        return new deleteConnectionStringCommand(databaseName, type, connectionStringName).execute();
    }

    async testClusterNodeConnection(serverUrl: string, databaseName?: string, bidirectional = true) {
        return new testClusterNodeConnectionCommand(serverUrl, databaseName, bidirectional).execute();
    }

    async testSqlConnectionString(databaseName: string, connectionString: string, factoryName: string) {
        return new testSqlConnectionStringCommand(databaseName, connectionString, factoryName).execute();
    }

    async testRabbitMqServerConnection(databaseName: string, connectionString: string) {
        return new testRabbitMqServerConnectionCommand(databaseName, connectionString).execute();
    }

    async testAzureQueueStorageServerConnection(
        databaseName: string,
        authentication: Raven.Client.Documents.Operations.ETL.Queue.AzureQueueStorageConnectionSettings
    ) {
        return new testAzureQueueStorageServerConnectionCommand(databaseName, authentication).execute();
    }

    async testKafkaServerConnection(
        databaseName: string,
        bootstrapServers: string,
        useServerCertificate: boolean,
        connectionOptionsDto: {
            [optionKey: string]: string;
        }
    ) {
        return new testKafkaServerConnectionCommand(
            databaseName,
            bootstrapServers,
            useServerCertificate,
            connectionOptionsDto
        ).execute();
    }

    async testElasticSearchNodeConnection(
        databaseName: string,
        serverUrl: string,
        authenticationDto: Raven.Client.Documents.Operations.ETL.ElasticSearch.Authentication
    ) {
        return new testElasticSearchNodeConnectionCommand(databaseName, serverUrl, authenticationDto).execute();
    }

    async getLocalFolderPathOptions(path: string, databaseName: string) {
        return getFolderPathOptionsCommand.forServerLocal(path, true, null, databaseName).execute();
    }

    async getBackupLocation(path: string, databaseName: string) {
        return new getBackupLocationCommand(path, databaseName).execute();
    }
}

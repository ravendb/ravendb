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
import replicationProgressCommand from "commands/database/tasks/replicationProgressCommand";
import internalReplicationProgressCommand from "commands/database/tasks/internalReplicationProgressCommand";
import testSnowflakeConnectionStringCommand from "commands/database/cluster/testSnowflakeConnectionStringCommand";
import testAmazonSqsServerConnectionCommand from "commands/database/cluster/testAmazonSqsServerConnectionCommand";
import testAiConnectionStringCommand from "commands/database/cluster/testAiConnectionStringCommand";
import saveEtlTaskCommand from "commands/database/tasks/saveEtlTaskCommand";
import testGenAiCommand from "commands/database/tasks/testGenAiCommand";
import geAiModelsCommand from "commands/database/tasks/geAiModelsCommand";
import getJsonSchemaFromSampleObjectCommand from "commands/database/tasks/getJsonSchemaFromSampleObjectCommand";
import getEtlErrorsCommand from "commands/database/tasks/getEtlErrorsCommand";
import getEtlStatsCommand from "commands/database/tasks/getEtlStatsCommand";
import deleteEtlErrorsCommand from "commands/database/tasks/deleteEtlErrorsCommand";
import retryBatchEtlCommand from "commands/database/tasks/retryBatchEtlCommand";
import fetchSqlDatabaseSchemaCommand from "commands/database/tasks/fetchSqlDatabaseSchemaCommand";

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

    async getEtlProgress(databaseName: string, location: databaseLocationSpecifier) {
        return new etlProgressCommand(databaseName, location, false).execute();
    }

    async getReplicationProgress(databaseName: string, location: databaseLocationSpecifier) {
        return new replicationProgressCommand(databaseName, location, false).execute();
    }

    async getInternalReplicationProgress(databaseName: string, location: databaseLocationSpecifier) {
        return new internalReplicationProgressCommand(databaseName, location, false).execute();
    }

    async getManualBackup(databaseName: string) {
        return new getManualBackupCommand(databaseName).execute();
    }

    async getCdcSinkTaskInfo(...args: Parameters<typeof getOngoingTaskInfoCommand.forCdcSink>) {
        return getOngoingTaskInfoCommand.forCdcSink(...args).execute();
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

    async deleteConnectionString(...args: ConstructorParameters<typeof deleteConnectionStringCommand>) {
        return new deleteConnectionStringCommand(...args).execute();
    }

    async testClusterNodeConnection(serverUrl: string, databaseName?: string, bidirectional = true) {
        return new testClusterNodeConnectionCommand(serverUrl, databaseName, bidirectional).execute();
    }

    async testSqlConnectionString(databaseName: string, connectionString: string, factoryName: string) {
        return new testSqlConnectionStringCommand(databaseName, connectionString, factoryName).execute();
    }

    async testSnowflakeConnectionString(databaseName: string, connectionString: string) {
        return new testSnowflakeConnectionStringCommand(databaseName, connectionString).execute();
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

    async testAmazonSqsServerConnection(
        databaseName: string,
        authentication: Raven.Client.Documents.Operations.ETL.Queue.AmazonSqsConnectionSettings
    ) {
        return new testAmazonSqsServerConnectionCommand(databaseName, authentication).execute();
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

    async testAiConnectionString(...args: ConstructorParameters<typeof testAiConnectionStringCommand>) {
        return new testAiConnectionStringCommand(...args).execute();
    }

    async getGenAiTaskInfo(...args: Parameters<typeof getOngoingTaskInfoCommand.forGenAi>) {
        return getOngoingTaskInfoCommand.forGenAi(...args).execute();
    }

    async saveGenAiTask(...args: Parameters<typeof saveEtlTaskCommand.forGenAi>) {
        return saveEtlTaskCommand.forGenAi(...args).execute();
    }

    async testGenAi(...args: ConstructorParameters<typeof testGenAiCommand>) {
        return new testGenAiCommand(...args).execute();
    }

    async getAiModels(...args: ConstructorParameters<typeof geAiModelsCommand>) {
        return new geAiModelsCommand(...args).execute();
    }

    async getJsonSchemaFromSampleObject(...args: ConstructorParameters<typeof getJsonSchemaFromSampleObjectCommand>) {
        return new getJsonSchemaFromSampleObjectCommand(...args).execute();
    }

    async getEtlErrors(...args: ConstructorParameters<typeof getEtlErrorsCommand>) {
        return new getEtlErrorsCommand(...args).execute();
    }

    async getEtlStats(...args: ConstructorParameters<typeof getEtlStatsCommand>) {
        return new getEtlStatsCommand(...args).execute();
    }

    async deleteEtlErrors(...args: ConstructorParameters<typeof deleteEtlErrorsCommand>) {
        return new deleteEtlErrorsCommand(...args).execute();
    }

    async retryBatch(...args: ConstructorParameters<typeof retryBatchEtlCommand>) {
        return new retryBatchEtlCommand(...args).execute();
    }

    async fetchSqlDatabaseSchema(...args: ConstructorParameters<typeof fetchSqlDatabaseSchemaCommand>) {
        return new fetchSqlDatabaseSchemaCommand(...args).execute();
    }
}

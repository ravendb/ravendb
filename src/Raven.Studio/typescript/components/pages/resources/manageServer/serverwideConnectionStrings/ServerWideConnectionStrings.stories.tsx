import React from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5, withForceRerender, securityClearanceArgType } from "test/storybookTestUtils";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { SharedStubs } from "test/stubs/SharedStubs";
import ServerWideConnectionStrings from "./ServerWideConnectionStrings";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

export default {
    title: "Pages/Manage Server/Server-Wide Connection Strings",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

interface ServerWideConnectionStringsStoryArgs {
    isEmpty: boolean;
    isTestSuccess: boolean;
    securityClearance: securityClearance;
    licenseType: Raven.Server.Commercial.LicenseType;
    hasRavenEtl: boolean;
    hasSqlEtl: boolean;
    hasSnowflakeEtl: boolean;
    hasOlapEtl: boolean;
    hasElasticSearchEtl: boolean;
    hasQueueEtl: boolean;
    hasEmbeddingsGeneration: boolean;
}

export const ServerWideConnectionStringsStory: StoryObj<ServerWideConnectionStringsStoryArgs> = {
    name: "Server-Wide Connection Strings",
    render: (args: ServerWideConnectionStringsStoryArgs) => {
        const { accessManager, license, databases } = mockStore;
        const { tasksService } = mockServices;

        const clusterDb = DatabasesStubs.nonShardedClusterDatabase().toDto();
        const shardedDb = DatabasesStubs.shardedDatabase().toDto();

        accessManager.with_securityClearance(args.securityClearance);
        databases.withDatabases([clusterDb, shardedDb]);
        tasksService.withServerWideConnectionStrings(args.isEmpty ? { Results: [] } : undefined);
        tasksService.withAiModels();
        tasksService.withGetTasks();
        tasksService.withLocalFolderPathOptions();
        tasksService.withBackupLocation();

        mockTestResults(args.isTestSuccess);

        license.with_LicenseLimited({
            Type: args.licenseType,
            HasRavenEtl: args.hasRavenEtl,
            HasSqlEtl: args.hasSqlEtl,
            HasSnowflakeEtl: args.hasSnowflakeEtl,
            HasOlapEtl: args.hasOlapEtl,
            HasElasticSearchEtl: args.hasElasticSearchEtl,
            HasQueueEtl: args.hasQueueEtl,
            HasEmbeddingsGeneration: args.hasEmbeddingsGeneration,
        });

        return <ServerWideConnectionStrings />;
    },
    args: {
        isEmpty: false,
        isTestSuccess: true,
        securityClearance: "ClusterAdmin",
        licenseType: "Enterprise",
        hasRavenEtl: true,
        hasSqlEtl: true,
        hasSnowflakeEtl: true,
        hasOlapEtl: true,
        hasElasticSearchEtl: true,
        hasQueueEtl: true,
        hasEmbeddingsGeneration: true,
    },
    argTypes: {
        securityClearance: securityClearanceArgType,
    },
};

function mockTestResults(isSuccess: boolean) {
    const { tasksService, manageServerService } = mockServices;

    tasksService.withTestClusterNodeConnection(isSuccess ? undefined : SharedStubs.nodeConnectionTestErrorResult());
    tasksService.withTestSqlConnectionString(isSuccess ? undefined : SharedStubs.nodeConnectionTestErrorResult());
    tasksService.withTestSnowflakeConnectionString(isSuccess ? undefined : SharedStubs.nodeConnectionTestErrorResult());
    tasksService.withTestKafkaServerConnection(isSuccess ? undefined : SharedStubs.nodeConnectionTestErrorResult());
    tasksService.withTestRabbitMqServerConnection(isSuccess ? undefined : SharedStubs.nodeConnectionTestErrorResult());
    tasksService.withTestAzureQueueStorageServerConnection(
        isSuccess ? undefined : SharedStubs.nodeConnectionTestErrorResult()
    );
    tasksService.withTestAmazonSqsServerConnection(isSuccess ? undefined : SharedStubs.nodeConnectionTestErrorResult());
    tasksService.withTestElasticSearchNodeConnection(
        isSuccess ? undefined : SharedStubs.nodeConnectionTestErrorResult()
    );
    tasksService.withTestAiConnectionString(isSuccess ? undefined : SharedStubs.nodeConnectionTestErrorResult());
    manageServerService.withTestPeriodicBackupCredentials(
        isSuccess ? undefined : SharedStubs.nodeConnectionTestErrorResult()
    );
}

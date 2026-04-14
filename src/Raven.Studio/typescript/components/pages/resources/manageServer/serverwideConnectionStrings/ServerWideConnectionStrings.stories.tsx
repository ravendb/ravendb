import React from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import {
    withStorybookContexts,
    withBootstrap5,
    securityClearanceArgType,
} from "test/storybookTestUtils";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { SharedStubs } from "test/stubs/SharedStubs";
import ServerWideConnectionStrings from "./ServerWideConnectionStrings";

export default {
    title: "Pages/Manage Server/Server-Wide Connection Strings",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

interface ServerWideConnectionStringsStoryArgs {
    isEmpty: boolean;
    isTestSuccess: boolean;
    securityClearance: securityClearance;
}

export const ServerWideConnectionStringsStory: StoryObj<ServerWideConnectionStringsStoryArgs> = {
    name: "Server-Wide Connection Strings",
    render: (args: ServerWideConnectionStringsStoryArgs) => {
        const { accessManager } = mockStore;
        const { tasksService } = mockServices;

        accessManager.with_securityClearance(args.securityClearance);

        tasksService.withServerWideConnectionStrings(args.isEmpty ? { Results: [] } : undefined);
        tasksService.withAiModels();
        tasksService.withLocalFolderPathOptions();
        tasksService.withBackupLocation();

        mockTestResults(args.isTestSuccess);

        return <ServerWideConnectionStrings />;
    },
    args: {
        isEmpty: false,
        isTestSuccess: true,
        securityClearance: "ClusterAdmin",
    },
    argTypes: {
        securityClearance: securityClearanceArgType,
    },
};

function mockTestResults(isSuccess: boolean) {
    const { tasksService, manageServerService } = mockServices;

    if (isSuccess) {
        tasksService.withTestClusterNodeConnection();
        tasksService.withTestSqlConnectionString();
        tasksService.withTestSnowflakeConnectionString();
        tasksService.withTestKafkaServerConnection();
        tasksService.withTestRabbitMqServerConnection();
        tasksService.withTestAzureQueueStorageServerConnection();
        tasksService.withTestAmazonSqsServerConnection();
        tasksService.withTestElasticSearchNodeConnection();
        tasksService.withTestAiConnectionString();
        manageServerService.withTestPeriodicBackupCredentials();
    } else {
        tasksService.withTestClusterNodeConnection(SharedStubs.nodeConnectionTestErrorResult());
        tasksService.withTestSqlConnectionString(SharedStubs.nodeConnectionTestErrorResult());
        tasksService.withTestSnowflakeConnectionString(SharedStubs.nodeConnectionTestErrorResult());
        tasksService.withTestKafkaServerConnection(SharedStubs.nodeConnectionTestErrorResult());
        tasksService.withTestRabbitMqServerConnection(SharedStubs.nodeConnectionTestErrorResult());
        tasksService.withTestAzureQueueStorageServerConnection(SharedStubs.nodeConnectionTestErrorResult());
        tasksService.withTestAmazonSqsServerConnection(SharedStubs.nodeConnectionTestErrorResult());
        tasksService.withTestElasticSearchNodeConnection(SharedStubs.nodeConnectionTestErrorResult());
        tasksService.withTestAiConnectionString(SharedStubs.nodeConnectionTestErrorResult());
        manageServerService.withTestPeriodicBackupCredentials(SharedStubs.nodeConnectionTestErrorResult());
    }
}

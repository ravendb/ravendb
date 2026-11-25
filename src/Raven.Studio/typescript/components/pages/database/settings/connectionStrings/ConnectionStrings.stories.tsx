import React from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5, databaseAccessArgType, licenseArgType } from "test/storybookTestUtils";
import ConnectionStrings from "./ConnectionStrings";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { SharedStubs } from "test/stubs/SharedStubs";

type AiConnectorType = Raven.Client.Documents.Operations.AI.AiConnectorType;
type AiConnectionStringDto = Raven.Client.Documents.Operations.AI.AiConnectionString;

export default {
    title: "Pages/Settings/Connection Strings",
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/S2r3Mrar3nB8OlZOGvHrlD/Pages---Connection-Strings?node-id=0-1&t=VTJvJLrCI0FvZpXO-1",
        },
    },
} satisfies Meta;

interface DefaultConnectionStringsProps {
    isEmpty: boolean;
    isTestSuccess: boolean;
    isSecuredServer: boolean;
    licenseType: Raven.Server.Commercial.LicenseType;
    databaseAccess: databaseAccessLevel;
    hasRavenEtl: boolean;
    hasSqlEtl: boolean;
    hasSnowflakeEtl: boolean;
    hasOlapEtl: boolean;
    hasElasticSearchEtl: boolean;
    hasQueueEtl: boolean;
    hasEmbeddingsGeneration: boolean;
}

export const DefaultConnectionStrings: StoryObj<DefaultConnectionStringsProps> = {
    name: "Connection Strings",
    render: (props: DefaultConnectionStringsProps) => {
        const { accessManager, license, databases } = mockStore;
        const { tasksService } = mockServices;

        accessManager.with_isServerSecure(props.isSecuredServer);

        tasksService.withAiModels();
        tasksService.withLocalFolderPathOptions();
        tasksService.withBackupLocation();
        tasksService.withGetTasks((x) => {
            (
                x.OngoingTasks.find(
                    (x) => x.TaskType === "RavenEtl"
                ) as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtl
            ).ConnectionStringName = Object.keys(DatabasesStubs.connectionStrings().RavenConnectionStrings)[0];
        });

        mockTestResults(props.isTestSuccess);

        tasksService.withConnectionStrings(
            props.isEmpty ? DatabasesStubs.emptyConnectionStrings() : DatabasesStubs.connectionStrings()
        );

        const db = databases.withActiveDatabase_NonSharded_SingleNode();

        accessManager.with_securityClearance("ValidUser");
        accessManager.with_databaseAccess({
            [db.name]: props.databaseAccess,
        });

        license.with_LicenseLimited({
            Type: props.licenseType,
            HasRavenEtl: props.hasRavenEtl,
            HasSqlEtl: props.hasSqlEtl,
            HasSnowflakeEtl: props.hasSnowflakeEtl,
            HasOlapEtl: props.hasOlapEtl,
            HasElasticSearchEtl: props.hasElasticSearchEtl,
            HasQueueEtl: props.hasQueueEtl,
            HasEmbeddingsGeneration: props.hasEmbeddingsGeneration,
        });

        return <ConnectionStrings />;
    },
    args: {
        isEmpty: false,
        isTestSuccess: true,
        isSecuredServer: true,
        licenseType: "Enterprise",
        databaseAccess: "DatabaseAdmin",
        hasRavenEtl: true,
        hasSqlEtl: true,
        hasSnowflakeEtl: true,
        hasOlapEtl: true,
        hasElasticSearchEtl: true,
        hasQueueEtl: true,
        hasEmbeddingsGeneration: true,
    },
    argTypes: {
        licenseType: licenseArgType,
        databaseAccess: databaseAccessArgType,
    },
};

type OpenAiChatApiConnectorType = Extract<AiConnectorType, "OpenAi" | "AzureOpenAi" | "Ollama">;

interface AiChatConnectionStringProps {
    connectorType: OpenAiChatApiConnectorType;
}

export const AiChatConnectionString: StoryObj<AiChatConnectionStringProps> = {
    name: "AI Chat Connection String",
    render: ({ connectorType }: AiChatConnectionStringProps) => {
        const { accessManager, license, databases } = mockStore;
        const { tasksService } = mockServices;

        tasksService.withAiModels();
        tasksService.withGetTasks();
        tasksService.withTestAiConnectionString();
        tasksService.withConnectionStrings({
            ...DatabasesStubs.emptyConnectionStrings(),
            AiConnectionStrings: { "ai-chat": aiChatConnectionString(connectorType) },
        });

        const db = databases.withActiveDatabase_NonSharded_SingleNode();
        accessManager.with_securityClearance("ValidUser");
        accessManager.with_databaseAccess({ [db.name]: "DatabaseAdmin" });
        license.with_License();

        return <ConnectionStrings />;
    },
    args: {
        connectorType: "OpenAi",
    },
    argTypes: {
        connectorType: {
            control: "select",
            options: ["OpenAi", "AzureOpenAi", "Ollama"] satisfies OpenAiChatApiConnectorType[],
        },
    },
};

function aiChatConnectionString(connectorType: OpenAiChatApiConnectorType): AiConnectionStringDto {
    const chatApiSettings = {
        ApiKey: "api-key",
        Model: "gpt-5.5",
        ReasoningEffort: "high",
        ChatApi: "ChatCompletions",
    } satisfies Partial<Raven.Client.Documents.Operations.AI.OpenAiChatApiSettings>;

    return {
        Type: "Ai",
        Name: "ai-chat",
        Identifier: "ai-chat",
        ModelType: "Chat",
        UsedBy: [],
        OpenAiSettings:
            connectorType === "OpenAi"
                ? {
                      ...chatApiSettings,
                      Endpoint: "https://api.openai.com/v1/",
                      OrganizationId: null,
                      ProjectId: null,
                  }
                : null,
        AzureOpenAiSettings:
            connectorType === "AzureOpenAi"
                ? { ...chatApiSettings, Endpoint: "https://example.openai.azure.com/", DeploymentName: "gpt-5.5" }
                : null,
        OllamaSettings:
            connectorType === "Ollama"
                ? { ...chatApiSettings, Endpoint: "http://localhost:11434", Uri: "http://localhost:11434" }
                : null,
        AnthropicSettings: null,
        EmbeddedSettings: null,
        GoogleSettings: null,
        HuggingFaceSettings: null,
        MistralAiSettings: null,
        VertexSettings: null,
    };
}

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
        tasksService.withTestAzureServiceBusServerConnection();
        tasksService.withTestElasticSearchNodeConnection();
        manageServerService.withTestPeriodicBackupCredentials();
    } else {
        tasksService.withTestClusterNodeConnection(SharedStubs.nodeConnectionTestErrorResult());
        tasksService.withTestSqlConnectionString(SharedStubs.nodeConnectionTestErrorResult());
        tasksService.withTestSnowflakeConnectionString(SharedStubs.nodeConnectionTestErrorResult());
        tasksService.withTestKafkaServerConnection(SharedStubs.nodeConnectionTestErrorResult());
        tasksService.withTestRabbitMqServerConnection(SharedStubs.nodeConnectionTestErrorResult());
        tasksService.withTestAzureQueueStorageServerConnection(SharedStubs.nodeConnectionTestErrorResult());
        tasksService.withTestAmazonSqsServerConnection(SharedStubs.nodeConnectionTestErrorResult());
        tasksService.withTestAzureServiceBusServerConnection(SharedStubs.nodeConnectionTestErrorResult());
        tasksService.withTestElasticSearchNodeConnection(SharedStubs.nodeConnectionTestErrorResult());
        manageServerService.withTestPeriodicBackupCredentials(SharedStubs.nodeConnectionTestErrorResult());
    }
}

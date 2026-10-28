import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { NotificationStubs } from "test/stubs/NotificationStubs";
import AiAgentExceededTokenThresholdDetails from "../alerts/AiAgentExceededTokenThresholdDetails";
import BlockingTombstonesDetails from "../alerts/BlockingTombstonesDetails";
import ComplexFieldsAlertDetails from "../alerts/ComplexFieldsAlertDetails";
import ConflictExceededDetails from "../alerts/ConflictExceededDetails";
import CpuCreditsBalanceDetails from "../alerts/CpuCreditsBalanceDetails";
import EtlTaskHealthChangeDetails from "../alerts/EtlTaskHealthChangeDetails";
import GenericAlertDetails from "../alerts/GenericAlertDetails";
import MismatchedReferenceLoadDetails from "../alerts/MismatchedReferenceLoadDetails";
import NewVersionAvailableDetails from "../alerts/NewVersionAvailableDetails";
import QueueSinkErrorDetails from "../alerts/QueueSinkErrorDetails";
import RemoteAttachmentErrorDetails from "../alerts/RemoteAttachmentErrorDetails";
import ServerLimitsDetails from "../alerts/ServerLimitsDetails";
import { storyActions, storyDate, times } from "./notificationDetailsStoryUtils";

type TombstoneDeletionBlockerType = Raven.Server.Documents.ITombstoneAware.TombstoneDeletionBlockerType;

export default {
    title: "Shell/Notification Details/Alerts",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const AiAgentExceededTokenThreshold: StoryObj = {
    render: () => {
        const alert = NotificationStubs.alert(
            "AiAgent_ExceededTokenThreshold",
            {
                AgentName: "Support assistant",
                ConversationId: "chats/0000000000000000123-A",
                TokenCount: 152_340,
                TokenThreshold: 100_000,
                Recommendation: "Summarize the conversation history or lower the number of tool results.",
                ToolCalls: [
                    {
                        Id: "call_1",
                        Name: "FindOrders",
                        Type: "Query",
                        Arguments: '{"company":"companies/1-A"}',
                        Query: "from Orders where Company = $company",
                    },
                    {
                        Id: "call_2",
                        Name: "SendEmail",
                        Type: "Action",
                        Arguments: '{"to":"ann@example.com"}',
                        Query: null,
                    },
                    { Id: "call_3", Name: "Summarize", Type: "SubAgent", Arguments: null, Query: null },
                ],
            },
            {
                Title: "AI agent exceeded the token threshold",
                Message: "A conversation used more tokens than the configured threshold.",
            }
        );

        return <AiAgentExceededTokenThresholdDetails notification={alert} actions={storyActions} />;
    },
};

export const BlockingTombstones: StoryObj = {
    render: () => {
        const blockerTypes: TombstoneDeletionBlockerType[] = [
            "Index",
            "Backup",
            "RavenEtl",
            "SqlEtl",
            "ExternalReplication",
            "PullReplicationAsSink",
        ];

        const alert = NotificationStubs.alert(
            "BlockingTombstones",
            {
                BlockingTombstones: blockerTypes.map((blockerType, index) => ({
                    BlockerType: blockerType,
                    BlockerTaskId: index + 1,
                    Source: blockerType === "Index" ? "Orders/ByCompany" : `Task ${index + 1}`,
                    Collection: index % 2 === 0 ? "Orders" : "Companies",
                    NumberOfTombstones: 1_000 * (index + 1),
                    SizeOfTombstonesInBytes: 250_000 * (index + 1),
                    SizeOfTombstonesHumane: "",
                })),
            },
            {
                Title: "Tombstone deletion is blocked",
                Message: "Some tasks and indexes did not process the tombstones yet, so they can't be deleted.",
            }
        );

        return <BlockingTombstonesDetails notification={alert} actions={storyActions} />;
    },
};

export const ComplexFields: StoryObj = {
    render: () => {
        const alert = NotificationStubs.alert(
            "Indexing_CoraxComplexItem",
            {
                Fields: {
                    "Orders/ByShipTo": ["ShipTo", "Lines"],
                    "Companies/ByAddress": ["Address"],
                },
            },
            {
                Title: "Complex fields in a Corax index",
                Message: "Corax can't index complex fields. These fields are not searchable.",
            }
        );

        return <ComplexFieldsAlertDetails notification={alert} actions={storyActions} />;
    },
};

export const ConflictExceeded: StoryObj = {
    render: () => {
        const alert = NotificationStubs.alert(
            "ConflictRevisionsExceeded",
            {
                Details: times(15, (index) => ({
                    Id: `orders/${index + 1}-A`,
                    Reason: index % 2 === 0 ? "MaxRevisions" : "MaxAge",
                    Deleted: 5 + index,
                    Time: storyDate(index * 3),
                })),
            },
            {
                Title: "Conflict revisions exceeded",
                Message: "Some documents have more conflict revisions than the configured limit.",
            }
        );

        return <ConflictExceededDetails notification={alert} actions={storyActions} />;
    },
};

export const CpuCreditsBalance: StoryObj = {
    render: () => {
        const alert = NotificationStubs.alert(
            "Throttling_CpuCreditsBalance",
            { IndexNames: ["Orders/ByCompany", "Orders/Totals", "Products/ByCategory"] },
            {
                Title: "CPU credits balance is low",
                Message: "Indexing was paused to save CPU credits.",
            }
        );

        return <CpuCreditsBalanceDetails notification={alert} actions={storyActions} />;
    },
};

export const EtlTaskHealthChange: StoryObj = {
    render: () => {
        const alert = NotificationStubs.alert(
            "Etl_HealthStatusChange",
            {
                HealthStatus: "Failed",
                PreviousHealthStatus: "Healthy",
                PreviousHealthStatusChangeAt: storyDate(90),
            },
            {
                Title: "ETL task 'Orders to SQL' is failing",
                Message: "The task health changed from Healthy to Failed.",
                Severity: "Error",
            }
        );

        return <EtlTaskHealthChangeDetails notification={alert} actions={storyActions} />;
    },
};

export const GenericWithMessage: StoryObj = {
    render: () => {
        const alert = NotificationStubs.alert(
            "Indexing_CouldNotGetStats",
            {
                Message:
                    "Could not get the stats of the index 'Orders/ByCompany'.\nThe server will try again in a minute.",
            },
            { Title: "Could not get index stats", Message: "The server could not read the index stats." }
        );

        return <GenericAlertDetails notification={alert} actions={storyActions} />;
    },
};

export const GenericWithException: StoryObj = {
    render: () => {
        const alert = NotificationStubs.alert(
            "IndexStore_IndexCouldNotBeOpened",
            {
                Exception:
                    "System.InvalidOperationException: Could not open the index 'Orders/ByCompany'.\n" +
                    "   at Raven.Server.Documents.Indexes.Index.Open()\n" +
                    "   at Raven.Server.Documents.Indexes.IndexStore.OpenIndex(String name)",
            },
            { Title: "Could not open index", Message: "The server failed to open an index.", Severity: "Error" }
        );

        return <GenericAlertDetails notification={alert} actions={storyActions} />;
    },
};

export const MismatchedReferenceLoad: StoryObj = {
    render: () => {
        const alert = NotificationStubs.alert(
            "MismatchedReferenceLoad",
            {
                IndexName: "Orders/ByEmployee",
                Warnings: {
                    "orders/1-A": times(6, (index) => ({
                        SourceId: `orders/${index + 1}-A`,
                        ReferenceId: `employees/${index + 1}-A`,
                        ActualCollection: "Companies",
                        MismatchedCollections: ["Employees"],
                    })),
                },
            },
            {
                Title: "Loading documents with a mismatched collection",
                Message: "The index 'Orders/ByEmployee' loads documents from a different collection than expected.",
            }
        );

        return <MismatchedReferenceLoadDetails notification={alert} actions={storyActions} />;
    },
};

export const NewVersionAvailable: StoryObj = {
    render: () => {
        const alert = NotificationStubs.alert(
            "Server_NewVersionAvailable",
            {
                VersionInfo: {
                    Version: "7.2.3",
                    BuildNumber: 72003,
                    BuildType: "Stable",
                    PublishedAt: storyDate(60 * 24),
                    UpdateSeverity: "Recommended",
                },
            },
            {
                Title: "RavenDB 7.2.3 is available",
                Message: "A new version of RavenDB is available.",
                Severity: "Info",
                Database: null,
            }
        );

        return <NewVersionAvailableDetails notification={alert} actions={storyActions} />;
    },
};

export const QueueSinkError: StoryObj = {
    render: () => {
        const alert = NotificationStubs.alert(
            "QueueSink_ConsumeError",
            {
                Errors: times(8, (index) => ({
                    Date: storyDate(index * 2),
                    Error: `Failed to consume a message from 'orders-queue': connection reset by peer (attempt ${index + 1})`,
                })),
            },
            {
                Title: "Queue Sink 'Orders' failed to consume messages",
                Message: "The Queue Sink task could not read messages from the broker.",
                Severity: "Error",
            }
        );

        return <QueueSinkErrorDetails notification={alert} actions={storyActions} />;
    },
};

export const RemoteAttachmentError: StoryObj = {
    render: () => {
        const alert = NotificationStubs.alert(
            "Attachments_RemoteAttachmentErroredIdentifier",
            {
                Errors: times(5, (index) => ({
                    Date: storyDate(index * 12),
                    Identifier: "azure-storage",
                    Hash: `Q2hhbmdlIHRoaXMgaGFzaCB${index}`,
                    Ids: [`orders/${index + 1}-A`, `orders/${index + 2}-A`],
                    Error:
                        "Azure.RequestFailedException: The specified container does not exist.\n" +
                        "Status: 404 (The specified container does not exist.)",
                })),
            },
            {
                Title: "Remote attachments upload failed",
                Message: "Some attachments could not be uploaded to the remote storage. Click a row to see the error.",
                Severity: "Error",
            }
        );

        return <RemoteAttachmentErrorDetails notification={alert} actions={storyActions} />;
    },
};

export const ServerLimits: StoryObj = {
    render: () => {
        const alert = NotificationStubs.alert(
            "ServerLimits",
            {
                Limits: [
                    { Name: "Max open files", Limit: "nofile", Value: 61_000, Max: 65_536, Date: storyDate(1) },
                    {
                        Name: "Max memory map areas",
                        Limit: "max_map_count",
                        Value: 64_000,
                        Max: 65_530,
                        Date: storyDate(4),
                    },
                    { Name: "Max threads", Limit: "threads-max", Value: 30_000, Max: 32_768, Date: storyDate(9) },
                ],
            },
            {
                Title: "Running close to OS limits",
                Message: "The server is close to some of the operating system limits.",
                Database: null,
            }
        );

        return <ServerLimitsDetails notification={alert} actions={storyActions} />;
    },
};

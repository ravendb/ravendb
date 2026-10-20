import operation = require("common/notifications/models/operation");
import smugglerDatabaseDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/smugglerDatabaseDetails");

type OperationChanged = Raven.Server.NotificationCenter.Notifications.OperationChanged;
type OperationState = Raven.Client.Documents.Operations.OperationState;
type SmugglerResult = Raven.Client.Documents.Smuggler.SmugglerResult;
type ShardedSmugglerResult = Raven.Client.Documents.Smuggler.ShardedSmugglerResult;
type Counts = Raven.Client.Documents.Smuggler.SmugglerProgressBase.Counts;
type DatabaseRecordProgress = Raven.Client.Documents.Smuggler.SmugglerProgressBase.DatabaseRecordProgress;

const counts = (readCount: number): Counts => ({
    Processed: true,
    Skipped: false,
    ReadCount: readCount,
    ErroredCount: 0,
    SizeInBytes: 0,
});

function smugglerResult(readCount: number, messages: string[]): SmugglerResult {
    const withEtag = { ...counts(readCount), LastEtag: 0 };
    const withSkipped = { ...withEtag, SkippedCount: 0 };
    return {
        CanMerge: false,
        CompareExchange: withEtag,
        CompareExchangeTombstones: counts(readCount),
        Conflicts: withEtag,
        Counters: withSkipped,
        DatabaseRecord: counts(readCount) as DatabaseRecordProgress,
        Documents: { ...withSkipped, Attachments: counts(readCount) },
        Identities: withEtag,
        Indexes: counts(readCount),
        ReplicationHubCertificates: counts(readCount),
        RevisionDocuments: { ...withSkipped, Attachments: counts(readCount) },
        Subscriptions: counts(readCount),
        TimeSeries: withSkipped,
        TimeSeriesDeletedRanges: withSkipped,
        Tombstones: withEtag,
        Elapsed: null,
        LegacyLastAttachmentEtag: null,
        LegacyLastDocumentEtag: null,
        Message: null,
        Messages: messages,
        Progress: null,
        ShouldPersist: false,
    };
}

function importChanged(state: OperationState): OperationChanged {
    return {
        Id: "op/1",
        Type: "OperationChanged",
        CreatedAt: "2026-10-16T10:00:00.000Z",
        StartTime: "2026-10-16T10:00:00.000Z",
        EndTime: null,
        OperationId: 1,
        TaskType: "DatabaseImport",
        State: state,
    } as OperationChanged;
}

function importOperation(state: OperationState): operation {
    return new operation("db", importChanged(state));
}

describe("smugglerDatabaseDetails.importResultModalView", () => {
    it("keeps the last progress for a failed import reopened from the notification center", () => {
        const op = importOperation({ Status: "InProgress", Progress: smugglerResult(5, ["Reading"]), Result: null });
        const error = { Message: "Import failed", Error: "stack" };

        op.updateWith(importChanged({ Status: "Faulted", Progress: null, Result: error }));
        const { props } = smugglerDatabaseDetails.importResultModalView(op, jest.fn())();

        expect(props.progress.Documents.ReadCount).toBe(5);
        expect(props.progress.Messages).toEqual(["Reading"]);
        expect(props.error).toEqual(error);
    });

    it("merges sharded results", () => {
        const shardedResult: ShardedSmugglerResult & { $type: string } = {
            $type: "Raven.Client.Documents.Smuggler.ShardedSmugglerResult",
            CanMerge: false,
            Message: null,
            ShouldPersist: false,
            Results: [
                { ShardNumber: 0, NodeTag: "A", CanMerge: false, Message: null, ShouldPersist: false, Result: smugglerResult(2, ["Shard 0"]) },
                { ShardNumber: 1, NodeTag: "B", CanMerge: false, Message: null, ShouldPersist: false, Result: smugglerResult(3, ["Shard 1"]) },
            ],
        };

        const op = importOperation({ Status: "Completed", Progress: null, Result: shardedResult });
        const { props } = smugglerDatabaseDetails.importResultModalView(op, jest.fn())();

        expect(props.progress.Documents.ReadCount).toBe(5);
        expect(props.progress.Messages).toEqual(["Shard 0", "Shard 1"]);
        expect(props.error).toBeNull();
    });
});

import React from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import ImportResultModal from "../ImportResultModal";

type SmugglerResult = Raven.Client.Documents.Smuggler.SmugglerResult;
type Counts = Raven.Client.Documents.Smuggler.SmugglerProgressBase.Counts;
type OperationStatus = Raven.Client.Documents.Operations.OperationStatus;

const counts = (overrides: Partial<Counts> = {}): Counts => ({
    Processed: false,
    Skipped: false,
    ReadCount: 0,
    ErroredCount: 0,
    SizeInBytes: 0,
    ...overrides,
});

const processed = (readCount = 0, erroredCount = 0) =>
    counts({ Processed: true, ReadCount: readCount, ErroredCount: erroredCount });
const skipped = () => counts({ Skipped: true });
const processing = (readCount = 0) => counts({ StartTime: "2026-09-15T10:00:00.000Z", ReadCount: readCount });
const pending = () => counts();

// Documents / RevisionDocuments carry a nested Attachments row (rendered with the ↳ icon)
const withAttachments = (base: Counts, attachments: Counts): any => ({
    ...base,
    LastEtag: 0,
    SkippedCount: 0,
    Attachments: attachments,
});

function smugglerResult(overrides: Partial<SmugglerResult>): SmugglerResult {
    return {
        CanMerge: false,
        CompareExchange: null,
        CompareExchangeTombstones: null,
        Conflicts: null,
        Counters: null,
        DatabaseRecord: null,
        Documents: null,
        Identities: null,
        Indexes: null,
        ReplicationHubCertificates: null,
        RevisionDocuments: null,
        Subscriptions: null,
        TimeSeries: null,
        TimeSeriesDeletedRanges: null,
        Tombstones: null,
        Elapsed: null,
        LegacyLastAttachmentEtag: null,
        LegacyLastDocumentEtag: null,
        Message: null,
        Messages: [],
        Progress: null,
        ShouldPersist: false,
        ...overrides,
    };
}

const startTime = new Date("2026-09-15T10:00:00.000Z");
const endTime = new Date("2026-09-15T10:01:16.000Z"); // -> "1 minute 16 seconds"

function render(progress: SmugglerResult, status: OperationStatus, ended: Date | null) {
    return (
        <ImportResultModal
            progress={progress}
            status={status}
            startTime={startTime}
            endTime={ended}
            onClose={() => {
                /* noop in stories */
            }}
        />
    );
}

export default {
    title: "Pages/Tasks/Import Data/Import Result Modal",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const InProgress: StoryObj = {
    render: () =>
        render(
            smugglerResult({
                DatabaseRecord: processed(1),
                Documents: withAttachments(processing(191_112), processing(3_950)),
                Counters: pending(),
                TimeSeries: pending(),
                Tombstones: pending(),
                RevisionDocuments: withAttachments(pending(), pending()),
                Conflicts: pending(),
                Indexes: pending(),
                Identities: pending(),
                CompareExchange: pending(),
                CompareExchangeTombstones: pending(),
                Subscriptions: pending(),
                TimeSeriesDeletedRanges: pending(),
            }),
            "InProgress",
            null
        ),
};

export const Completed: StoryObj = {
    render: () =>
        render(
            smugglerResult({
                DatabaseRecord: processed(1),
                Documents: withAttachments(processed(1_059), processed(17)),
                Counters: processed(77),
                TimeSeries: processed(55_581),
                Tombstones: skipped(),
                RevisionDocuments: withAttachments(processed(6_305), processed(0)),
                Conflicts: processed(0),
                Indexes: processed(7),
                Identities: processed(0),
                CompareExchange: processed(77),
                CompareExchangeTombstones: skipped(),
                Subscriptions: processed(0),
                TimeSeriesDeletedRanges: processed(0),
            }),
            "Completed",
            endTime
        ),
};

export const Failed: StoryObj = {
    render: () =>
        render(
            smugglerResult({
                DatabaseRecord: processed(1),
                Documents: withAttachments(processed(26_792, 12), processed(17)),
                Counters: processed(77),
                TimeSeries: processed(864, 12),
                Tombstones: pending(),
                RevisionDocuments: withAttachments(pending(), pending()),
                Conflicts: skipped(),
                Indexes: skipped(),
                Identities: skipped(),
                CompareExchange: skipped(),
                CompareExchangeTombstones: pending(),
                Subscriptions: skipped(),
                Messages: [
                    "2026-09-15T10:00:03.000Z, Documents, Read: 26792, Errored: 12",
                    "2026-09-15T10:00:05.000Z, Time Series, Read: 864, Errored: 12",
                    "2026-09-15T10:00:05.500Z, Import failed",
                ],
            }),
            "Faulted",
            endTime
        ),
};

import React from "react";
import { fireEvent, rtlRender, waitFor } from "test/rtlTestUtils";
import ImportResultModal from "./ImportResultModal";

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

const processed = (erroredCount = 0) => counts({ Processed: true, ReadCount: 10, ErroredCount: erroredCount });
const started = () => counts({ StartTime: "2026-09-15T10:00:00.000Z", ReadCount: 5 });

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

const modal = (progress: SmugglerResult, status: OperationStatus = "InProgress") => (
    <ImportResultModal progress={progress} status={status} startTime={new Date()} endTime={null} onClose={jest.fn()} />
);

describe("ImportResultModal", () => {
    beforeAll(() => {
        Object.defineProperty(HTMLElement.prototype, "scrollHeight", { configurable: true, value: 1000 });
        Object.defineProperty(HTMLElement.prototype, "clientHeight", { configurable: true, value: 300 });
    });

    afterAll(() => {
        Reflect.deleteProperty(HTMLElement.prototype, "scrollHeight");
        Reflect.deleteProperty(HTMLElement.prototype, "clientHeight");
    });

    it("marks a row processed with errors as a warning rather than a failure", () => {
        const { screen } = rtlRender(modal(smugglerResult({ Subscriptions: processed(2) }), "Completed"));

        const statusIcon = screen.getByText("Processed with errors").querySelector("i");
        expect(statusIcon).toHaveClass("text-warning");
        expect(statusIcon).not.toHaveClass("text-danger");
    });

    it("shows pending for phases the import has not reached yet", () => {
        const { screen } = rtlRender(
            modal(
                smugglerResult({
                    Documents: { ...started(), LastEtag: 0, SkippedCount: 0, Attachments: counts() },
                    Subscriptions: counts(),
                })
            )
        );

        expect(screen.getAllByText("Processing")).toHaveLength(2);
        expect(screen.getByText("Pending").querySelector("i")).toHaveClass("icon-waiting");
        expect(screen.queryByText("Not processed")).not.toBeInTheDocument();
    });

    it("keeps the log pinned to the bottom until the user scrolls up", async () => {
        const { screen, fireClick, rerender } = rtlRender(modal(smugglerResult({ Messages: ["first"] })));
        await fireClick(screen.getByRole("button", { name: /Show details/ }));

        const log = screen.getByRole("log");
        await waitFor(() => expect(log.closest(".collapse")).toHaveClass("show"));
        expect(log.scrollTop).toBe(1000);

        fireEvent.scroll(log, { target: { scrollTop: 100 } });
        rerender(modal(smugglerResult({ Messages: ["first", "second"] })));
        expect(log.scrollTop).toBe(100);

        fireEvent.scroll(log, { target: { scrollTop: 700 } });
        rerender(modal(smugglerResult({ Messages: ["first", "second", "third"] })));
        expect(log.scrollTop).toBe(1000);
    });
});

import { Row } from "@tanstack/react-table";
import { act, fireEvent, rtlRender, waitFor } from "test/rtlTestUtils";
import { NotificationStubs } from "test/stubs/NotificationStubs";
import fileDownloader from "common/fileDownloader";
import copyToClipboard from "common/copyToClipboard";
import type notificationCenter from "common/notifications/notificationCenter";
import moment from "moment";
import { createDetailsProvider, isAlert, isPerformanceHint } from "./notificationDetailsProvider";
import { getCsvFileName } from "./notificationDetailsCsv";
import { dateColumnOptions } from "./NotificationDetailsCells";
import { mockDetailsActions, mockTableWidth } from "./notificationDetailsTestUtils";
import GenericAlertDetails from "./alerts/GenericAlertDetails";
import PagingDetails from "./performanceHints/PagingDetails";
import HugeDocumentsDetails from "./performanceHints/HugeDocumentsDetails";
import SlowIoDetails from "./performanceHints/SlowIoDetails";
import SlowSqlDetails from "./performanceHints/SlowSqlDetails";
import ServerLimitsDetails from "./alerts/ServerLimitsDetails";
import DumpRawIndexDataDetails from "./operations/DumpRawIndexDataDetails";

function mockCenter() {
    return {
        reactModalView: ko.observable(null),
        dismiss: jest.fn(),
        postpone: jest.fn(() => $.Deferred<void>().resolve().promise()),
    };
}

describe("NotificationDetails", () => {
    mockTableWidth();

    describe("createDetailsProvider", () => {
        it("supports only the matching notifications", () => {
            const provider = createDetailsProvider(isPerformanceHint("Paging"), PagingDetails);

            expect(provider.supportsDetailsFor(NotificationStubs.performanceHint("Paging", { Actions: {} }))).toBe(
                true
            );
            expect(provider.supportsDetailsFor(NotificationStubs.performanceHint("SlowIO", { Writes: {} }))).toBe(
                false
            );
            expect(provider.supportsDetailsFor(NotificationStubs.alert("LowDiskSpace", {}))).toBe(false);
        });

        it("does not compile when the component expects a different reason", () => {
            // @ts-expect-error SlowIoDetails needs a SlowIO hint
            createDetailsProvider(isPerformanceHint("Paging"), SlowIoDetails);
            // @ts-expect-error ServerLimitsDetails needs a ServerLimits alert
            createDetailsProvider(isAlert("BlockingTombstones"), ServerLimitsDetails);
        });

        it("opens the modal in the notification center and closes it after dismiss", () => {
            const center = mockCenter();
            const alert = NotificationStubs.alert("LowDiskSpace", { Message: "x" });
            const provider = createDetailsProvider(isAlert(), GenericAlertDetails);

            provider.showDetailsFor(alert, center as unknown as notificationCenter);

            const { component, props } = center.reactModalView()();
            expect(component).toBe(GenericAlertDetails);
            expect(props.notification).toBe(alert);

            props.actions.dismiss();

            expect(center.dismiss).toHaveBeenCalledWith(alert);
            expect(center.reactModalView()).toBeNull();
        });

        it("closes the modal once the postpone request succeeds", async () => {
            const center = mockCenter();
            const alert = NotificationStubs.alert("LowDiskSpace", {});
            createDetailsProvider(isAlert(), GenericAlertDetails).showDetailsFor(
                alert,
                center as unknown as notificationCenter
            );

            await center.reactModalView()().props.actions.postpone(3600);

            expect(center.postpone).toHaveBeenCalledWith(alert, 3600);
            expect(center.reactModalView()).toBeNull();
        });

        it("keeps the newer modal open when the replaced one closes", () => {
            const center = mockCenter();
            const provider = createDetailsProvider(isAlert(), GenericAlertDetails);
            const showDetails = (reason: "LowDiskSpace" | "LowSwapSize") =>
                provider.showDetailsFor(NotificationStubs.alert(reason, {}), center as unknown as notificationCenter);

            showDetails("LowDiskSpace");
            const replacedView = center.reactModalView();
            showDetails("LowSwapSize");
            const currentView = center.reactModalView();

            replacedView().props.actions.close();

            expect(center.reactModalView()).toBe(currentView);
        });

        it("uses the notification id as the component key", () => {
            const center = mockCenter();
            const alert = NotificationStubs.alert("LowDiskSpace", {});

            createDetailsProvider(isAlert(), GenericAlertDetails).showDetailsFor(
                alert,
                center as unknown as notificationCenter
            );

            expect(center.reactModalView()().props.key).toBe(alert.id);
        });
    });

    describe("footers", () => {
        it("lets the user postpone a persistent alert", async () => {
            const actions = mockDetailsActions();
            const alert = NotificationStubs.alert("LowDiskSpace", { Message: "Limit reached" });

            const { screen } = rtlRender(<GenericAlertDetails notification={alert} actions={actions} />);

            fireEvent.click(screen.getByRole("button", { name: /Postpone/ }));
            fireEvent.click(await screen.findByText("1 day"));

            expect(actions.postpone).toHaveBeenCalledWith(24 * 3600);
        });

        it("hides postpone for an alert that is not persistent", () => {
            const alert = NotificationStubs.alert("LowDiskSpace", {}, { IsPersistent: false });

            const { screen } = rtlRender(<GenericAlertDetails notification={alert} actions={mockDetailsActions()} />);

            expect(screen.queryByRole("button", { name: /Postpone/ })).not.toBeInTheDocument();
            expect(screen.getByRole("button", { name: /Dismiss/ })).toBeInTheDocument();
        });

        it("stores don't show again on the performance hint", async () => {
            const hint = NotificationStubs.performanceHint("HugeDocuments", { HugeDocuments: {} });

            const { screen } = rtlRender(<HugeDocumentsDetails notification={hint} actions={mockDetailsActions()} />);

            fireEvent.click(await screen.findByLabelText("Don't show again"));

            expect(hint.dontShowAgain()).toBe(true);
        });

        it("hides don't show again when the performance hint cannot be dismissed", async () => {
            const hint = NotificationStubs.performanceHint("HugeDocuments", { HugeDocuments: {} });
            hint.readOnly = true;

            const { screen } = rtlRender(<HugeDocumentsDetails notification={hint} actions={mockDetailsActions()} />);

            expect(await screen.findByText("Performance hint title")).toBeInTheDocument();
            expect(screen.queryByLabelText("Don't show again")).not.toBeInTheDocument();
            expect(screen.queryByRole("button", { name: /Dismiss/ })).not.toBeInTheDocument();
        });
    });

    describe("table", () => {
        afterEach(() => jest.restoreAllMocks());

        it("exports raw values with units in the header", async () => {
            const downloadAsCsv = jest.spyOn(fileDownloader, "downloadAsCsv").mockImplementation(() => undefined);
            const hint = NotificationStubs.performanceHint("Paging", {
                Actions: {
                    Query: [
                        {
                            Details: "=cmd",
                            Duration: 15,
                            NumberOfResults: 1500,
                            Occurrence: "2026-10-20T10:00:00.0000000Z",
                            PageSize: 2147483647,
                            TotalDocumentsSizeInBytes: 2048,
                        },
                    ],
                },
            });

            const { screen } = rtlRender(<PagingDetails notification={hint} actions={mockDetailsActions()} />);

            expect(await screen.findByText("Not set")).toBeInTheDocument();
            fireEvent.click(screen.getByRole("button", { name: /Export to CSV/ }));

            await waitFor(() => expect(downloadAsCsv).toHaveBeenCalled());
            const [csv, fileName] = downloadAsCsv.mock.calls[0];
            expect(csv).toBe(
                "Action,# of results,Page size,Docs size (bytes),Date,Duration (ms),Details\r\n" +
                    "Query,1500,Not set,2048,2026-10-20T10:00:00.0000000Z,15,'=cmd"
            );
            expect(fileName).toMatch(/^db1-performance-hint-title-\d{4}-\d{2}-\d{2}-\d{4}\.csv$/);
        });

        it("copies the table to clipboard as CSV", async () => {
            const copy = jest.spyOn(copyToClipboard, "copy").mockImplementation(() => undefined);
            const hint = NotificationStubs.performanceHint("HugeDocuments", {
                HugeDocuments: {
                    "orders/1": { Id: "orders/1", Size: 2048, Date: "2026-10-20T10:00:00.0000000Z" },
                },
            });

            const { screen } = rtlRender(<HugeDocumentsDetails notification={hint} actions={mockDetailsActions()} />);

            fireEvent.click(await screen.findByRole("button", { name: /Copy as CSV/ }));

            expect(copy).toHaveBeenCalledWith(
                "Document ID,Document size (bytes),Last access\r\norders/1,2048,2026-10-20T10:00:00.0000000Z",
                expect.any(String)
            );
        });

        it("builds a file name from the database, the title and the date", () => {
            expect(getCsvFileName("db1", "Server limits: CPU / Memory", moment("2026-10-20T10:05:00"))).toBe(
                "db1-server-limits-cpu-memory-2026-10-20-1005.csv"
            );
        });

        it("keeps non-latin names and skips empty parts in the file name", () => {
            expect(getCsvFileName("База", "!!!", moment("2026-10-20T10:05:00"))).toBe("база-2026-10-20-1005.csv");
        });

        it("filters dates by the text shown in the table", () => {
            const row = { getValue: () => "2026-10-20T10:00:00.0000000Z" } as unknown as Row<unknown>;
            const filter = (value: string) => dateColumnOptions.filterFn(row, "Date", value, () => undefined);

            expect(filter("October")).toBe(true);
            expect(filter("2026-10-20")).toBe(true);
            expect(filter("March")).toBe(false);
        });

        it("shows new rows when the notification details change", async () => {
            const hugeDocument = (id: string) => ({ Id: id, Size: 2048, Date: "2026-10-20T10:00:00.0000000Z" });
            const hint = NotificationStubs.performanceHint("HugeDocuments", {
                HugeDocuments: { "orders/1": hugeDocument("orders/1") },
            });

            const { screen } = rtlRender(<HugeDocumentsDetails notification={hint} actions={mockDetailsActions()} />);
            expect(await screen.findByText("1 document")).toBeInTheDocument();

            act(() =>
                hint.details({
                    HugeDocuments: { "orders/1": hugeDocument("orders/1"), "orders/2": hugeDocument("orders/2") },
                })
            );

            expect(await screen.findByText("2 documents")).toBeInTheDocument();
        });
    });

    describe("preview", () => {
        const statement = (sql: string) => ({ Duration: 1500, Date: "2026-10-20T10:00:00.0000000Z", Statement: sql });

        it("follows the previewed item when the notification details change", async () => {
            const hint = NotificationStubs.performanceHint("SqlEtl_SlowSql", { Statements: [statement("select 1")] });

            const { screen } = rtlRender(<SlowSqlDetails notification={hint} actions={mockDetailsActions()} />);
            fireEvent.click(await screen.findByTitle("Show details below the table"));
            expect(screen.getByRole("heading", { name: "Details" })).toBeInTheDocument();

            await act(async () => hint.details({ Statements: [statement("select 1"), statement("select 2")] }));
            expect(screen.getByRole("heading", { name: "Details" })).toBeInTheDocument();

            await act(async () => hint.details({ Statements: [statement("select 2")] }));
            expect(screen.queryByRole("heading", { name: "Details" })).not.toBeInTheDocument();
        });
    });

    describe("operations", () => {
        type OperationState = Raven.Client.Documents.Operations.OperationState;

        it("keeps the details of an already canceled operation open", () => {
            const actions = mockDetailsActions();
            const operation = NotificationStubs.operation("DumpRawIndexData", { Status: "Canceled" } as OperationState);

            rtlRender(<DumpRawIndexDataDetails notification={operation} actions={actions} />);

            expect(actions.close).not.toHaveBeenCalled();
        });

        it("closes the details when the operation gets canceled", () => {
            const actions = mockDetailsActions();
            const operation = NotificationStubs.operation("DumpRawIndexData", {
                Status: "Completed",
            } as OperationState);

            rtlRender(<DumpRawIndexDataDetails notification={operation} actions={actions} />);
            expect(actions.close).not.toHaveBeenCalled();

            act(() => operation.status("Canceled"));

            expect(actions.close).toHaveBeenCalled();
        });
    });

    describe("message", () => {
        const notPersistent = { IsPersistent: false };

        it("does not repeat the details message when it is the same as the alert message", () => {
            const message = "Configure the 'System.GC.HeapCount' setting in Raven.Server.runtimeconfig.json / env.";
            const alert = NotificationStubs.alert(
                "GcThreadContention",
                { Message: message },
                { ...notPersistent, Message: message }
            );

            const { screen } = rtlRender(<GenericAlertDetails notification={alert} actions={mockDetailsActions()} />);

            expect(screen.getAllByText(message)).toHaveLength(1);
        });

        it("shows the details message when it adds information", () => {
            const alert = NotificationStubs.alert(
                "LowDiskSpace",
                { Message: "Only 1 GB left on disk C" },
                notPersistent
            );

            const { screen } = rtlRender(<GenericAlertDetails notification={alert} actions={mockDetailsActions()} />);

            expect(screen.getByText("Alert message")).toBeInTheDocument();
            expect(screen.getByText("Only 1 GB left on disk C")).toBeInTheDocument();
        });

        it("does not render HTML from the message", () => {
            const alert = NotificationStubs.alert(
                "LowDiskSpace",
                {},
                { ...notPersistent, Message: "Expected List<string> <b>value</b>" }
            );

            const { screen } = rtlRender(<GenericAlertDetails notification={alert} actions={mockDetailsActions()} />);

            expect(screen.getByText("Expected List<string> <b>value</b>")).toBeInTheDocument();
        });
    });
});

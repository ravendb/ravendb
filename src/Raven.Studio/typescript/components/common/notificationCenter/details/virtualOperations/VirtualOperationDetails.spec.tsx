import { act } from "@testing-library/react";
import { fireEvent, rtlRender, waitFor } from "test/rtlTestUtils";
import database from "models/resources/database";
import fileDownloader from "common/fileDownloader";
import virtualDeleteByQuery from "common/notifications/models/virtualDeleteByQuery";
import virtualBulkInsertFailures from "common/notifications/models/virtualBulkInsertFailures";
import { VirtualBulkInsertFailuresDetails, VirtualDeleteByQueryDetails } from "./VirtualOperationDetails";
import { mockDetailsActions, mockTableWidth } from "../notificationDetailsTestUtils";

function deleteByQuery(id: string, query: string): queryBasedVirtualBulkOperationItem {
    return {
        id,
        operationId: 1,
        date: "2026-10-20T10:00:00.0000000Z",
        duration: 120,
        totalItemsProcessed: 10,
        documentsProcessed: 10,
        attachmentsProcessed: 0,
        countersProcessed: 0,
        timeSeriesProcessed: 0,
        query,
        indexOrCollectionUsed: "Orders",
    };
}

describe("VirtualOperationDetails", () => {
    mockTableWidth();

    afterEach(() => jest.restoreAllMocks());

    it("shows operations that complete while the dialog is open", async () => {
        const notification = new virtualDeleteByQuery({ name: "db1" } as database);
        notification.operations([deleteByQuery("1", "from Orders")]);

        const { screen } = rtlRender(
            <VirtualDeleteByQueryDetails notification={notification} actions={mockDetailsActions()} />
        );

        expect(await screen.findByText("Delete by query")).toBeInTheDocument();
        expect(screen.getByText("1 operation")).toBeInTheDocument();

        act(() => notification.operations.unshift(deleteByQuery("2", "from Users")));

        expect(await screen.findByText("2 operations")).toBeInTheDocument();
        expect(screen.getByText("from Users")).toBeInTheDocument();
    });

    it("names the exported file after the dialog title", async () => {
        const downloadAsCsv = jest.spyOn(fileDownloader, "downloadAsCsv").mockImplementation(() => undefined);
        const notification = new virtualBulkInsertFailures({ name: "db1" } as database);
        notification.operations([
            {
                id: "1",
                operationId: 1,
                date: "2026-10-20T10:00:00.0000000Z",
                duration: 120,
                errorMsg: "Bulk insert failed",
                error: "System.Exception",
            },
        ]);

        const { screen } = rtlRender(
            <VirtualBulkInsertFailuresDetails notification={notification} actions={mockDetailsActions()} />
        );
        fireEvent.click(await screen.findByRole("button", { name: /Export to CSV/ }));

        await waitFor(() => expect(downloadAsCsv).toHaveBeenCalled());
        expect(downloadAsCsv.mock.calls[0][1]).toMatch(/^db1-bulk-inserts-failures-\d{4}-\d{2}-\d{2}-\d{4}\.csv$/);
    });
});

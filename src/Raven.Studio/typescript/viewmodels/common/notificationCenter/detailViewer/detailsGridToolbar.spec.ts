import detailsGridToolbar from "viewmodels/common/notificationCenter/detailViewer/detailsGridToolbar";
import fileDownloader from "common/fileDownloader";
import textColumn from "widgets/virtualGrid/columns/textColumn";
import type virtualGridController from "widgets/virtualGrid/virtualGridController";
import type virtualGridSelection from "widgets/virtualGrid/virtualGridSelection";
import type abstractNotification from "common/notifications/models/abstractNotification";

interface request {
    action: string;
}

function gridWith(totalCount: number | null, items: request[] = []) {
    const selection = ko.observable<virtualGridSelection<request>>({
        mode: "inclusive",
        included: [],
        excluded: [],
        count: 0,
        totalCount,
    });

    const controller = {
        selection,
        getColumns: () => [new textColumn<request>(null, (x) => x.action, "Action", "100%")],
        getItems: () => items,
    } as unknown as virtualGridController<request>;

    return { grid: ko.observable(controller), selection };
}

function notification(databaseName: string | undefined, title: string) {
    return { databaseName, title: ko.observable(title) } as unknown as abstractNotification;
}

describe("detailsGridToolbar", () => {
    it("summarizes the row count with the item name", () => {
        const { grid, selection } = gridWith(22);
        const toolbar = new detailsGridToolbar(grid, notification("db1", "Paging"), "request", "requests");

        expect(toolbar.summary()).toBe("22 requests");

        selection({ ...selection(), totalCount: 1 });
        expect(toolbar.summary()).toBe("1 request");
    });

    it("has no summary and cannot export before the grid has rows", () => {
        const { grid } = gridWith(null);
        const toolbar = new detailsGridToolbar(grid, notification("db1", "Paging"), "request", "requests");

        expect(toolbar.summary()).toBe("");
        expect(toolbar.canExport()).toBe(false);
    });

    describe("exportToCsv", () => {
        let downloadAsCsv: jest.SpyInstance;

        beforeEach(() => {
            jest.useFakeTimers().setSystemTime(new Date(2026, 9, 16, 13, 5));
            downloadAsCsv = jest.spyOn(fileDownloader, "downloadAsCsv").mockImplementation(() => undefined);
        });

        afterEach(() => {
            downloadAsCsv.mockRestore();
            jest.useRealTimers();
        });

        it("downloads the rows named after the database, the notification and the time", () => {
            const { grid } = gridWith(1, [{ action: "Query (Orders/Totals)" }]);
            const toolbar = new detailsGridToolbar(grid, notification("db1", "Page size too big (queries)"), "request", "requests");

            toolbar.exportToCsv();

            expect(downloadAsCsv).toHaveBeenCalledWith(
                "Action\r\nQuery (Orders/Totals)",
                "db1-page-size-too-big-queries-2026-10-16-1305.csv"
            );
        });

        it("leaves the database out of server-wide export names", () => {
            const { grid } = gridWith(0);
            const toolbar = new detailsGridToolbar(grid, notification(undefined, "Server limits"), "limit", "limits");

            toolbar.exportToCsv();

            expect(downloadAsCsv).toHaveBeenCalledWith("Action", "server-limits-2026-10-16-1305.csv");
        });
    });
});

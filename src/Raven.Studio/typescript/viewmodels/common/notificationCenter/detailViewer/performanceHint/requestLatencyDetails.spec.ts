import requestLatencyDetails from "viewmodels/common/notificationCenter/detailViewer/performanceHint/requestLatencyDetails";
import columnPreviewPlugin from "widgets/virtualGrid/columnPreviewPlugin";
import type textColumn from "widgets/virtualGrid/columns/textColumn";
import type performanceHint from "common/notifications/models/performanceHint";
import type notificationCenter from "common/notifications/notificationCenter";

jest.mock("views/common/notificationCenter/detailViewer/performanceHint/requestLatencyDetails.html", () => ({
    default: "<div></div>",
}));
jest.mock("views/common/notificationCenter/detailViewer/performanceHint/footerPartial.html", () => ({
    default: "<div></div>",
}));

type RequestLatencyInfo = Raven.Server.NotificationCenter.Notifications.Details.RequestLatencyInfo;

const latency: RequestLatencyInfo = {
    Action: "Query",
    Date: "2026-10-16T10:43:43.7097982Z",
    Duration: 1500,
    Query: "from Orders where Freight > 10",
};

function openDialog() {
    const install = jest.spyOn(columnPreviewPlugin.prototype, "install").mockImplementation(() => undefined);
    const grid = { headerVisible: jest.fn(), init: jest.fn() };
    const hint = {
        details: () => ({ RequestLatencies: { Query: [latency] } }),
        title: ko.observable("Request latency"),
        databaseName: "db1",
    } as unknown as performanceHint;

    const dialog = new requestLatencyDetails(hint, {} as notificationCenter);
    (dialog as unknown as { gridController: KnockoutObservable<typeof grid> }).gridController(grid);
    dialog.compositionComplete();

    const columns = grid.init.mock.calls[0][1]() as textColumn<RequestLatencyInfo>[];
    const preview = install.mock.calls[0][2];
    install.mockRestore();

    const previewOf = (header: string) => {
        const onValue = jest.fn();
        preview(latency, columns.find((x) => x.header === header), null, onValue);
        return onValue;
    };

    return { previewOf };
}

describe("requestLatencyDetails", () => {
    it("previews and copies the action of a request", () => {
        const { previewOf } = openDialog();

        expect(previewOf("Action")).toHaveBeenCalledWith("Query", "Query");
    });

    it("previews and copies the duration of a request", () => {
        const { previewOf } = openDialog();

        expect(previewOf("Duration")).toHaveBeenCalledWith(expect.stringContaining("1 second"), expect.stringContaining("1 second"));
    });

    it("previews and copies the query of a request", () => {
        const { previewOf } = openDialog();

        expect(previewOf("Query")).toHaveBeenCalledWith("from Orders where Freight &gt; 10", "from Orders where Freight > 10");
    });
});

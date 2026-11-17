import getDocumentRevisionsCountCommand = require("commands/database/documents/getDocumentRevisionsCountCommand");
import messagePublisher from "common/messagePublisher";

describe("getDocumentRevisionsCountCommand", () => {
    const failedRequest = { status: 500, statusText: "Internal Server Error", responseText: "The server is down" };

    let reportError: jest.SpyInstance;

    const getRevisionsCount = (reportFailure?: boolean) =>
        new getDocumentRevisionsCountCommand("orders/1-A", "db1", reportFailure).execute().then(
            () => "loaded",
            () => "failed"
        );

    beforeEach(() => {
        jest.spyOn($, "ajax").mockImplementation(
            () => $.Deferred().reject(failedRequest, "error", failedRequest.statusText).promise() as JQueryXHR
        );
        reportError = jest.spyOn(messagePublisher, "reportError").mockImplementation(() => undefined);
    });

    afterEach(() => {
        jest.restoreAllMocks();
    });

    it("reports a failure", async () => {
        expect(await getRevisionsCount()).toBe("failed");
        expect(reportError).toHaveBeenCalledWith(
            "Failed to get revisions count",
            failedRequest.responseText,
            failedRequest.statusText
        );
    });

    it("leaves a failure unreported when asked to", async () => {
        expect(await getRevisionsCount(false)).toBe("failed");
        expect(reportError).not.toHaveBeenCalled();
    });
});

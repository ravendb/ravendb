import adoptOrphanedRevisionsDetails from "viewmodels/common/notificationCenter/detailViewer/operations/adoptOrphanedRevisionsDetails";
import enforceRevisionsConfigurationDetails from "viewmodels/common/notificationCenter/detailViewer/operations/enforceRevisionsConfigurationDetails";
import revertRevisionsDetails from "viewmodels/common/notificationCenter/detailViewer/operations/revertRevisionsDetails";
import validateSchemaDetails from "viewmodels/common/notificationCenter/detailViewer/operations/validateSchemaDetails";
import columnPreviewPlugin from "widgets/virtualGrid/columnPreviewPlugin";
import type textColumn from "widgets/virtualGrid/columns/textColumn";
import type operation from "common/notifications/models/operation";
import type notificationCenter from "common/notifications/notificationCenter";

jest.mock("views/common/notificationCenter/detailViewer/operations/adoptOrphanedRevisionsDetails.html", () => ({
    default: "<div></div>",
}));
jest.mock("views/common/notificationCenter/detailViewer/operations/enforceRevisionsConfigurationDetails.html", () => ({
    default: "<div></div>",
}));
jest.mock("views/common/notificationCenter/detailViewer/operations/revertRevisionsDetails.html", () => ({
    default: "<div></div>",
}));
jest.mock("views/common/notificationCenter/detailViewer/operations/validateSchemaDetails.html", () => ({
    default: "<div></div>",
}));
jest.mock("views/common/notificationCenter/detailViewer/operations/footerPartial.html", () => ({
    default: "<div></div>",
}));

interface gridItem {
    Id: string;
}

type dialogClass = new (op: operation, center: notificationCenter) => { compositionComplete(): void };

function previewOfDocumentId(dialogType: dialogClass, progress: object) {
    const install = jest.spyOn(columnPreviewPlugin.prototype, "install").mockImplementation(() => undefined);
    const grid = { headerVisible: jest.fn(), init: jest.fn(), reset: jest.fn() };
    const op = {
        status: ko.observable("InProgress"),
        isCompleted: ko.observable(false),
        result: ko.observable(null),
        progress: ko.observable(null),
        title: ko.observable("Operation"),
        databaseName: "db1",
    };

    const dialog = new dialogType(op as unknown as operation, {} as notificationCenter);
    (dialog as unknown as { gridController: KnockoutObservable<typeof grid> }).gridController(grid);
    dialog.compositionComplete();
    op.progress(progress);

    const columns = grid.init.mock.calls[0][1]() as textColumn<gridItem>[];
    const preview = install.mock.calls[0][2];
    install.mockRestore();

    const onValue = jest.fn();
    preview({ Id: "orders/1-A" }, columns.find((x) => x.header === "Document ID"), null, onValue);
    return onValue;
}

describe("operation details grids", () => {
    it.each([
        ["adopt orphaned revisions", adoptOrphanedRevisionsDetails, { Warnings: { "orders/1-A": "Orphaned revision" } }],
        ["enforce revisions configuration", enforceRevisionsConfigurationDetails, { Warnings: { "orders/1-A": "Skipped" } }],
        ["revert revisions", revertRevisionsDetails, { Warnings: { "orders/1-A": "Not found" } }],
        ["validate schema", validateSchemaDetails, { Errors: { "orders/1-A": "Missing Name" } }],
    ])("%s copies the document ID as plain text", (_, dialogType, progress) => {
        expect(previewOfDocumentId(dialogType as unknown as dialogClass, progress)).toHaveBeenCalledWith(
            "orders&#x2F;1-A",
            "orders/1-A"
        );
    });
});

import app = require("durandal/app");
import eventsCollector = require("common/eventsCollector");
import viewModelBase = require("viewmodels/viewModelBase");

jest.mock("views/common/confirmationDialog.html", () => ({ default: "<div></div>" }));

ko.DirtyFlag = require("external/dirtyFlag").DirtyFlag;
eventsCollector.default.reportViewModel = jest.fn();

class pageWithUnsavedChanges extends viewModelBase {
    view = { default: "<div></div>" };

    constructor() {
        super();
        this.dirtyFlag().forceDirty();
    }
}

function answerUnsavedChangesDialog(answer: string | null | undefined) {
    app.showBootstrapDialog = () => $.Deferred().resolve(answer);
}

describe("viewModelBase", () => {
    describe("canDeactivate with unsaved changes", () => {
        it("leaves the page and drops the changes on Discard changes", async () => {
            answerUnsavedChangesDialog("Discard changes");
            const page = new pageWithUnsavedChanges();

            expect(await page.canDeactivate(false)).toEqual({ can: true });
            expect(page.dirtyFlag().isDirty()).toBeFalse();
        });

        it("stays on the page and keeps the changes on Stay on this page", async () => {
            answerUnsavedChangesDialog("Stay on this page");
            const page = new pageWithUnsavedChanges();

            expect(await page.canDeactivate(false)).toEqual({ can: false });
            expect(page.dirtyFlag().isDirty()).toBeTrue();
        });

        it.each([
            ["the close button", null],
            ["Escape or a backdrop click", undefined],
        ])("stays on the page and keeps the changes when the dialog is dismissed with %s", async (_, answer) => {
            answerUnsavedChangesDialog(answer);
            const page = new pageWithUnsavedChanges();

            expect(await page.canDeactivate(false)).toEqual({ can: false });
            expect(page.dirtyFlag().isDirty()).toBeTrue();
        });
    });
});

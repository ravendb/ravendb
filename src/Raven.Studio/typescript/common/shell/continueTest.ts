
import document = require("models/database/documents/document");
import saveDocumentCommand = require("commands/database/documents/saveDocumentCommand");
import databasesManager = require("common/shell/databasesManager");

class continueTest {
    static default = new continueTest();

    private initialized = false;

    constructor() {
        _.bindAll(this, "continue");
    }

    showContinueButton = ko.observable<boolean>(false);
    private databaseName = ko.observable<string>();

    init(args: { withStop: string; database: string }) {
        if (!this.initialized) {
            this.showContinueButton("withStop" in args);
            if (this.showContinueButton()) {
                this.databaseName(args.database);
            }
            this.initialized = true;
        }
    }

    continue() {
        const doc = document.empty();
        const db = databasesManager.default.getDatabaseByName(this.databaseName());
        new saveDocumentCommand("Debug/Done", doc, db, false)
            .execute()
            .done(() => this.showContinueButton(false));
    }
}

export = continueTest;
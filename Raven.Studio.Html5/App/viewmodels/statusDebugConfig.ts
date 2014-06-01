import getStatusDebugConfigCommand = require("commands/getStatusDebugConfigCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");


class statusDebugConfig extends viewModelBase {
    data = ko.observable<string>();

    constructor() {
        super();

        aceEditorBindingHandler.install();
    }

    activate(args) {
        super.activate(args);

        this.activeDatabase.subscribe(() => this.fetchStatusDebugConfig());
        return this.fetchStatusDebugConfig();
    }

    fetchStatusDebugConfig(): JQueryPromise<any> {
        var db = this.activeDatabase();
        if (db) {
            return new getStatusDebugConfigCommand(db)
                .execute()
                .done((results: any) =>
                    this.data(JSON.stringify(results, null, 4)));
        }

        return null;
    }
}

export = statusDebugConfig;
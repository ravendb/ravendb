import getStatusDebugRoutesCommand = require("commands/getStatusDebugRoutesCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");


class statusDebugRoutes extends viewModelBase {
    data = ko.observable<string>();

    constructor() {
        super();

        aceEditorBindingHandler.install();
    }

    activate(args) {
        super.activate(args);
        return this.fetchRoutes();
    }

    fetchRoutes(): JQueryPromise<any> {
        return new getStatusDebugRoutesCommand()
            .execute()
            .done((results: any) =>
                this.data(JSON.stringify(results, null, 4)));
    }
}

export = statusDebugRoutes;
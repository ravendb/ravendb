import getStatusDebugRoutesCommand = require("commands/getStatusDebugRoutesCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");


class statusDebugRoutes extends viewModelBase {
    data = ko.observable<string>();
    editor: AceAjax.Editor;

    constructor() {
        super();

        aceEditorBindingHandler.install();
    }

    compositionComplete() {
        super.compositionComplete();

        var editorElement = $("#statusDebugRoutesEditor");
        if (editorElement.length > 0) {
            this.editor = ko.utils.domData.get(editorElement[0], "aceEditor");
        }

        $("#statusDebugRoutesEditor").on('DynamicHeightSet', () => this.editor.resize());
    }

    detached() {
        super.detached();
        $("#statusDebugRoutesEditor").off('DynamicHeightSet');
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
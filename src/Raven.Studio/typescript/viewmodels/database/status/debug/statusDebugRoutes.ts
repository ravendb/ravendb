import getStatusDebugRoutesCommand = require("commands/database/debug/getStatusDebugRoutesCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import shell = require('viewmodels/shell');

class statusDebugRoutes extends viewModelBase {
    data = ko.observable<string>();
    editor: AceAjax.Editor;
    isGlobalAdmin = shell.isGlobalAdmin;

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

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('JHZ574');

        if (this.isGlobalAdmin()) {
            this.fetchRoutes();
            this.activeDatabase.subscribe(() => this.fetchRoutes());
        }
    }

    private fetchRoutes(): JQueryPromise<any> {
        return new getStatusDebugRoutesCommand()
            .execute()
            .done((results: any) =>
                this.data(JSON.stringify(results, null, 4)));
    }
}

export = statusDebugRoutes;

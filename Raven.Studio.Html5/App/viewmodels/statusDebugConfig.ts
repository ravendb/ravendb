import getStatusDebugConfigCommand = require("commands/getStatusDebugConfigCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");


class statusDebugConfig extends viewModelBase {
    data = ko.observable<string>();

   editor: AceAjax.Editor;

    constructor() {
        super();

        aceEditorBindingHandler.install();
    }

    compositionComplete() {
        super.compositionComplete();

        var editorElement = $("#statusDebugConfigEditor");
        if (editorElement.length > 0) {
            this.editor = ko.utils.domData.get(editorElement[0], "aceEditor");
        }

        $("#statusDebugConfigEditor").on('DynamicHeightSet', () => this.editor.resize());
    }

    detached() {
        super.detached();
        $("#statusDebugConfigEditor").off('DynamicHeightSet');
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('JHZ574');
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
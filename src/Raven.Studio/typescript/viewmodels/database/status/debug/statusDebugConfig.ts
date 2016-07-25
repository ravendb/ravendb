import getStatusDebugConfigCommand = require("commands/database/debug/getStatusDebugConfigCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

class statusDebugConfig extends viewModelBase {
    data = ko.observable<string>("");
    editor: AceAjax.Editor;
    isForbidden: KnockoutComputed<boolean>;

    constructor() {
        super();

        aceEditorBindingHandler.install();
        this.isForbidden = ko.computed(() => !this.data());
    }

    canActivate(args: any) {
        super.canActivate(args);

        var deffered = $.Deferred();
        this.fetchStatusDebugConfig().always(() => deffered.resolve({ can: true }));
        return deffered;
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('JHZ574');
        this.activeDatabase.subscribe(() => this.fetchStatusDebugConfig());
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

    private fetchStatusDebugConfig(): JQueryPromise<any> {
        var db = this.activeDatabase();
        return new getStatusDebugConfigCommand(db)
            .execute()
            .done((results: any) => this.data(JSON.stringify(results, null, 4)));
    }
}

export = statusDebugConfig;

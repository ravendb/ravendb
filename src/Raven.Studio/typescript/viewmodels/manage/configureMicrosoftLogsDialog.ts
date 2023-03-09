import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import aceEditorBindingHandler = require("../../common/bindingHelpers/aceEditorBindingHandler");

interface ConfigureMicrosoftLogsDialogResult {
    isEnabled: boolean;
    configuration: string;
}

class configureMicrosoftLogsDialog extends dialogViewModelBase {
    
    code = ko.observable<string>("");
    isEnabled = ko.observable<boolean>(false);
    
    constructor(isEnabled:boolean, configuration: string) {
        super();

        aceEditorBindingHandler.install();

        this.code(configuration);
        this.isEnabled(isEnabled);
    }

    attached() {
        
    }

    deactivate() {
        super.deactivate(null);
    }

    compositionComplete() {
        super.compositionComplete();
    }

    close() {
        dialog.close(this);
    }
    
    save() {
        const result: ConfigureMicrosoftLogsDialogResult = {
            isEnabled: this.isEnabled(),
            configuration: this.isEnabled() ? this.code() : null
        };
        
        dialog.close(this, result);
    }
}

export = configureMicrosoftLogsDialog;

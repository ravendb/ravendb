import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

class configureMicrosoftLogsDialog extends dialogViewModelBase {
    
    view = require("views/manage/configureMicrosoftLogsDialog.html");

    code = ko.observable<string>("");
    isEnabled = ko.observable<boolean>(false);
    persist = ko.observable<boolean>(false);

    validationGroup: KnockoutValidationGroup;
    
    constructor(isEnabled:boolean, configuration: string) {
        super();

        aceEditorBindingHandler.install();

        this.code(configuration);
        this.isEnabled(isEnabled);

        this.code.extend({
            required: {
                onlyIf: () => this.isEnabled()
            },
            aceValidation: true,
            validation: [
                {
                    validator: (val: string) => {
                        if (!this.isEnabled()) {
                            return true;
                        }
                        try {
                            const parsedJson = JSON.parse(val);
                            return _.isPlainObject(parsedJson);
                        } catch {
                            return false;
                        }
                    },
                    message: "Code must be valid JSON object"
                }
            ]
        });
        
        this.validationGroup = ko.validatedObservable({
            code: this.code
        });
    }

    attached() {
        // empty by design, so that pressing enter does not call save button click
    }

    close() {
        dialog.close(this);
    }
    
    save() {
        if (!this.isValid(this.validationGroup)) {
            return ;
        }
        
        const codeAsJson = JSON.parse(this.code());
        
        const result: ConfigureMicrosoftLogsDialogResult = {
            isEnabled: this.isEnabled(),
            configuration: this.isEnabled() ? codeAsJson : null,
            persist: this.persist()
        };
        
        dialog.close(this, result);
    }
}

export = configureMicrosoftLogsDialog;

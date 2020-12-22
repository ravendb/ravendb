import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class documentWarningsConfirm extends dialogViewModelBase {

    private warnings = ko.observableArray<AceAjax.Annotation>();
    private readonly onGoto: (warning: AceAjax.Annotation) => void;

    constructor(warnings: Array<AceAjax.Annotation>, onGoto: (warning: AceAjax.Annotation) => void) {
        super(null);

        this.warnings(warnings);
        this.onGoto = onGoto;
    }

    confirm() {
        dialog.close(this, true);
    }

    cancel() {
        dialog.close(this, false);
    }

    goTo(warning: AceAjax.Annotation) { 
        dialog.close(this, false);
        this.onGoto(warning);
    }

    warningLocation(warning: AceAjax.Annotation) {
        return ko.pureComputed(() => {
            return `Line: ${warning.row + 1}, Column: ${warning.column}`;
        })
    }
}

export = documentWarningsConfirm;

import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class warningsData {
    source: string;
    warnings = ko.observableArray<AceAjax.Annotation>();
}

class editorWarningsConfirm extends dialogViewModelBase {

    private textForTitle: string;
    private warningsList = ko.observableArray<warningsData>();
    private readonly onGoto: (warning: AceAjax.Annotation, source?: string) => void;

    constructor(title: string,
                warningsList: Array<warningsData>, 
                onGoto: (warning: AceAjax.Annotation, source?: string) => void) {
        super(null);

        this.textForTitle = title;
        this.warningsList(warningsList);
        this.onGoto = onGoto;
    }

    confirm() {
        dialog.close(this, true);
    }

    cancel() {
        dialog.close(this, false);
    }

    goTo(warning: AceAjax.Annotation, source: string) {
        dialog.close(this, false);
        this.onGoto(warning, source);
    }

    warningLocation(warning: AceAjax.Annotation) {
        return ko.pureComputed(() => {
            return `Line: ${warning.row + 1}, Column: ${warning.column}`;
        })
    }
}

export = editorWarningsConfirm;

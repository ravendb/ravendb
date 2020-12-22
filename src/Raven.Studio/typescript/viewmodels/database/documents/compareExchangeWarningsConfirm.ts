import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class compareExchangeWarningsConfirm extends dialogViewModelBase {

    private valueWarnings = ko.observableArray<AceAjax.Annotation>();
    private metadataWarnings = ko.observableArray<AceAjax.Annotation>();
    
    private readonly onGotoValue: (warning: AceAjax.Annotation) => void;
    private readonly onGotoMetadata: (warning: AceAjax.Annotation) => void;

    constructor(valueWarnings: Array<AceAjax.Annotation>,
                metadataWarnings: Array<AceAjax.Annotation>,
                onGotoValue: (warning: AceAjax.Annotation) => void,
                onGotoMetadata: (warning: AceAjax.Annotation) => void) {
        super(null);

        this.valueWarnings(valueWarnings);
        this.metadataWarnings(metadataWarnings);
        this.onGotoValue = onGotoValue;
        this.onGotoMetadata = onGotoMetadata;

        this.bindToCurrentInstance("goToValue", "goToMetadata");
    }

    confirm() {
        dialog.close(this, true);
    }

    cancel() {
        dialog.close(this, false);
    }

    goToValue(valueWarning: AceAjax.Annotation) { 
        dialog.close(this, false);
        this.onGotoValue(valueWarning);
    }

    goToMetadata(metadataWarning: AceAjax.Annotation) {
        dialog.close(this, false);
        this.onGotoMetadata(metadataWarning);
    }

    warningLocation(warning: AceAjax.Annotation) {
        return ko.pureComputed(() => {
            return `Line: ${warning.row + 1}, Column: ${warning.column}`;
        })
    }
}

export = compareExchangeWarningsConfirm;

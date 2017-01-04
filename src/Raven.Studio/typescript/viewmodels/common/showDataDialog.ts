import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

class showDataDialog extends dialogViewModelBase {

    width = ko.observable<string>("");
    inputData = ko.observable<string>();

    constructor(private title: string, inputData: string, elementToFocusOnDismissal?: string, calcWidth: boolean = false) {
        super(elementToFocusOnDismissal);

        this.inputData(inputData);
        if (calcWidth) {
            this.width(this.calcWidth(inputData) + "px");
        }  
    }

    calcWidth(code: string): number {
        const arrayOfLines = code.match(/[^\r\n]+/g);
        const maxLineLength = Math.max.apply(Math, $.map(arrayOfLines, el => el.length));
        return Math.max(Math.min(maxLineLength * 12, $(window).width() - 200), 598 /*default*/);
    }

    attached() {
        super.attached();
        aceEditorBindingHandler.install();
        //this.selectText();

        //TODO: create createKeyboardShortcut in dialog view model base and use it
        /*jwerty.key("CTRL+C, enter", e => {
            e.preventDefault();
            this.close();
        }, this, "#documentsText");*/
    }

    /*deactivate() {
        //TODO: call super?
        $("#inputData").unbind('keydown.jwerty');
    }

    selectText() {
        $("#inputData").select();
    }*/

    close() { //TODO: move to base class?
        dialog.close(this);
    }

    copyToClipboard() {
        copyToClipboard.copy(this.inputData(), this.title + " was copied to clipboard");
        this.close();
    }
}

export = showDataDialog; 

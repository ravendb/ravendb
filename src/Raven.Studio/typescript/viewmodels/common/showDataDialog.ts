import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");

type supportedLangs = "javascript" | "csharp";

class showDataDialog extends dialogViewModelBase {

    width = ko.observable<string>("");
    inputData = ko.observable<string>();

    inputDataFormatted = ko.pureComputed(() => {
        const input = this.inputData();
        if (_.isUndefined(input)) {
            return "";
        }
        return Prism.highlight(input, (Prism.languages as any)[this.lang]);
    });

    constructor(private title: string, inputData: string, private lang: supportedLangs, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);

        this.inputData(inputData);
    }

    close() {
        dialog.close(this);
    }

    copyToClipboard() {
        this.close();
        copyToClipboard.copy(this.inputData(), this.title + " was copied to clipboard");
    }
}

export = showDataDialog; 

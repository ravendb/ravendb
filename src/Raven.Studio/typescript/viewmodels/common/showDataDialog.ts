import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");
import generalUtils = require("common/generalUtils");
import { highlight, languages } from "prismjs";

type supportedLangs = "javascript" | "csharp" | "plain";

class showDataDialog extends dialogViewModelBase {

    view = require("views/common/showDataDialog.html");

    width = ko.observable<string>("");
    inputData = ko.observable<string>();

    inputDataFormatted = ko.pureComputed(() => {
        const input = this.inputData();
        if (_.isUndefined(input)) {
            return "";
        }
        if (this.lang === "plain") {
            return generalUtils.escapeHtml(input);
        }
        
        return highlight(input, languages[this.lang]);
    });

    constructor(private title: string, inputData: string, private lang: supportedLangs, elementToFocusOnDismissal?: string) {
        super({ elementToFocusOnDismissal: elementToFocusOnDismissal });

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

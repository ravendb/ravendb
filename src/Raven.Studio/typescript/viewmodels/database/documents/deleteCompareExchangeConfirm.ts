import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class deleteCompareExchangeConfirm extends dialogViewModelBase {

    private itemKeys = ko.observableArray<string>();
    
    title: string;
    subTitleHtml: string;

    constructor(itemsKeys: Array<string>) {
        super(null);

        if (itemsKeys.length === 0) {
            throw new Error("Must have at least one compare exchange item to delete.");
        }

        this.itemKeys(itemsKeys);

        if (this.itemKeys().length === 1) {
            this.title = "Delete compare exchange item?";
            this.subTitleHtml = `You're deleting a compare exchange item with key:`;
        } else {
            this.title = "Delete compare exchange items?";
            this.subTitleHtml = `You're deleting <strong>${this.itemKeys().length}</strong> compare exchange items with keys:`;
        }
    }

    deleteItems() {
        dialog.close(this, true);
    }

    cancel() {
        dialog.close(this, false);
    }
}

export = deleteCompareExchangeConfirm;

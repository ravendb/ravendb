import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class deleteCompareExchangeConfirm extends dialogViewModelBase {

    private itemIds = ko.observableArray<string>();

    constructor(itemsIds: Array<string>) {
        super(null);

        if (itemsIds.length === 0) {
            throw new Error("Must have at least one compare exchange value to delete.");
        }

        this.itemIds(itemsIds);
    }

    deleteItems() {
        dialog.close(this, true);
    }

    cancel() {
        dialog.close(this, false);
    }
}

export = deleteCompareExchangeConfirm;

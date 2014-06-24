import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/database");
import router = require("plugins/router"); 
import appUrl = require("common/appUrl");


class showSampleDataDialog extends dialogViewModelBase {
    data = ko.observable<string>();


    constructor(private inputData: string, private db: database,private isPaste:boolean = false, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);
        this.data(inputData);
    }
    
    canActivate(args: any): any {
         return true;
    }
    attached() {
        super.attached();
        this.selectText();
    }

    deactivate() {
        $("#classData").unbind('keydown.jwerty');
    }

    selectText() {
        $("#classData").select();
    }


  

    close() {
        dialog.close(this);
    }

    activateDocs() {
        this.selectText();
    }


}

export = showSampleDataDialog; 
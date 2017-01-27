import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

class tempStatDialog extends dialogViewModelBase {

    json = ko.observable("");

    constructor(private obj: any, replacer: (key: string, value: string) => any = null) {
        super(null);

        aceEditorBindingHandler.install();

        if (ko.isObservable(obj)) {
            const dynamicObj = obj as KnockoutObservable<any>;
            this.json(JSON.stringify(obj(), replacer, 4));
            dynamicObj.subscribe(v => {
                this.json(JSON.stringify(v, replacer, 4));
            });
        } else {
            this.json(JSON.stringify(obj, replacer, 4));
        }
    }

    close() {
        dialog.close(this);
    }
}

export = tempStatDialog; 

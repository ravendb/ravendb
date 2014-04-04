import viewModelBase = require("viewmodels/viewModelBase");
import patchDocuments = require("models/patchDocuments");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import patchParam = require("models/patchParam");

class patch extends viewModelBase {

    displayName = "patch";

    patchDocuments = ko.observable<patchDocuments>();

    constructor() {
        super();

        aceEditorBindingHandler.install();
    }

    activate() {
        this.patchDocuments(patchDocuments.empty());
    }

    setSelectedPatchOnOption(patchOnOption: string) {
        this.patchDocuments().patchOnOption(patchOnOption);
    }

}

export = patch;
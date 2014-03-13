import viewModelBase = require("viewmodels/viewModelBase");
import transformer = require("models/transformer");
import getTransformersCommand = require("commands/getTransformersCommand");
import appUrl = require("common/appUrl");

//todo: implement refresh from db
class Transformers extends viewModelBase {

    newTransformerUrl = appUrl.forCurrentDatabase().newTransformer;

    constructor() {
        super();
    }



    toggleGrouping() {

    }

    collapseAll() {

    }

    expandAll() {

    }
    deleteAllTransformers() {

    }
}

export = Transformers;
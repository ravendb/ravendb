import getStatusDebugIndexFieldsCommand = require("commands/getStatusDebugIndexFieldsCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");


class statusDebugIndexFields extends viewModelBase {
    result = ko.observable<statusDebugIndexFieldsDto>();
    indexStr = ko.observable("");

    constructor() {
        super();
        aceEditorBindingHandler.install();
    }

    fetchIndexFields(): JQueryPromise<statusDebugIndexFieldsDto> {
        this.result(null);
        var db = this.activeDatabase();
        if (db) {
            return new getStatusDebugIndexFieldsCommand(db, this.indexStr())
                .execute()
                .done((results: statusDebugIndexFieldsDto) => this.result(results));
                
            //TODO: how do we handle failure? 
        }

        return null;
    }
}

export = statusDebugIndexFields;
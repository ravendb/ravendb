/// <reference path="../models/dto.ts" />

import viewModelBase = require("viewmodels/viewModelBase");
import transformer = require("models/transformer");
import saveTransrormerCommand = require("commands/saveTransformerCommand");
import getSingleTransformerCommand = require("commands/getSingleTransformerCommand");

class editTransformer extends  viewModelBase{

    editedTransformer = ko.observable<transformer>();
    isEditingExistingTransformer = ko.observable(false);

    constructor() {
        super();
    }

    activate(transformerToEditName: string) {
        super.activate(transformerToEditName);

        if (transformerToEditName) {
            this.isEditingExistingTransformer(true);
            this.editExistingTransformer(transformerToEditName);
        } else {
            this.editedTransformer(transformer.empty());
        }
    }

    editExistingTransformer(unescapedTransformerName: string) {
        var indexName = decodeURIComponent(unescapedTransformerName);
        this.fetchTransformerToEdit(indexName)
            .done((trans: savedTransformerDto) => this.editedTransformer(new transformer().initFromSave(trans)));
    }
    
    fetchTransformerToEdit(transformerName: string): JQueryPromise<savedTransformerDto> {
        return new getSingleTransformerCommand(transformerName, this.activeDatabase()).execute();
    }

    saveTransformer() {
        new saveTransrormerCommand(this.editedTransformer(), this.activeDatabase())
            .execute()
            .done(()=> {
                if (!this.isEditingExistingTransformer()) {
                    this.isEditingExistingTransformer(true);
                }
        });
    }

    deleteTransformer() {
        //todo: implement delete transformer
    }

}



export = editTransformer;
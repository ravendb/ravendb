/// <reference path="../models/dto.ts" />

import viewModelBase = require("viewmodels/viewModelBase");
import transformer = require("models/transformer");
import saveTransformerCommand = require("commands/saveTransformerCommand");
import getSingleTransformerCommand = require("commands/getSingleTransformerCommand");
import deleteTransformerCommand = require("commands/deleteTransformerCommand");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import popoverBindingHandler = require("common/popoverBindingHandler");
import deleteTransformerConfirm = require("viewmodels/deleteTransformerConfirm");
import saveTransformerWithNewNameConfirm = require("viewmodels/saveTransformerWithNewNameConfirm");
import dialog = require("plugins/dialog");
import appUrl = require("common/appUrl");
import router = require("plugins/router");

class editTransformer extends  viewModelBase{

    editedTransformer = ko.observable<transformer>();
    isEditingExistingTransformer = ko.observable(false);
    popoverOptions = ko.observable<any>();
    containerSelector = "#editTransformerContainer";

    constructor() {
        super();
        aceEditorBindingHandler.install();
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

    attached() {
        this.addTransformerHelpPopover();
    }

    addTransformerHelpPopover() {
        $("#transformerResultsLabel").popover({
            html: true,
            trigger: 'hover',
            content: 'The Transform function allows you to change the shape of individual result documents before the server returns them. It uses C# LINQ query syntax <br/> <br/> Example: <pre> <br/> <span class="code-keyword">from</span> order <span class="code-keyword">in</span> orders <br/> <span class="code-keyword">let</span> region = Database.Load(result.RegionId) <br/> <span class="code-keyword">select new</span> { <br/> result.Date, <br/> result.Amount, <br/> Region = region.Name, <br/> Manager = region.Manager <br/>}</pre>',
        });
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
        if (this.isEditingExistingTransformer() && this.editedTransformer().wasNameChanged()) {
            var db = this.activeDatabase();
            var saveTransformerWithNewNameViewModel = new saveTransformerWithNewNameConfirm(this.editedTransformer(), db);
            saveTransformerWithNewNameViewModel.saveTask.done((trans: transformer) => this.editedTransformer(trans));
            dialog.show(saveTransformerWithNewNameViewModel);

        } else {

            new saveTransformerCommand(this.editedTransformer(), this.activeDatabase())
                .execute()
                .done((trans: transformer) => {
                    this.editedTransformer(trans);
                    if (!this.isEditingExistingTransformer()) {
                        this.isEditingExistingTransformer(true);
                    }
                });
        }
    }

    deleteTransformer() {
        var transformer = this.editedTransformer();
        
        if (transformer) {
            var db = this.activeDatabase();
            var deleteViewmodel = new deleteTransformerConfirm([transformer.name()], db);
            deleteViewmodel.deleteTask.done(() => router.navigate(appUrl.forTransformers(db)));
            dialog.show(deleteViewmodel);
        }
    
    }

}



export = editTransformer;
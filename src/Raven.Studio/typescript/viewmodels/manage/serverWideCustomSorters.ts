import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import sorterListItemModel = require("models/database/settings/sorterListItemModel");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import generalUtils = require("common/generalUtils");
import router = require("plugins/router");
import getServerWideCustomSortersCommand = require("commands/serverWide/sorters/getServerWideCustomSortersCommand");
import deleteServerWideCustomSorterCommand = require("commands/serverWide/sorters/deleteServerWideCustomSorterCommand");

class serverWideCustomSorters extends viewModelBase {
    serverWideSorters = ko.observableArray<sorterListItemModel>([]);

    addUrl = ko.pureComputed(() => appUrl.forEditServerWideCustomSorter());

    constructor() {
        super();

        aceEditorBindingHandler.install();
        this.bindToCurrentInstance("confirmRemoveServerWideSorter", "editServerWideSorter");
    }

    activate(args: any) {
        super.activate(args);
        
        return this.loadServerWideSorters();
    }

    private loadServerWideSorters() {
        return new getServerWideCustomSortersCommand()
            .execute()
            .done(sorters => {
                this.serverWideSorters(sorters.map(x => new sorterListItemModel(x)));
            });
    }

    editServerWideSorter(sorter: sorterListItemModel) {
        const url = appUrl.forEditServerWideCustomSorter(sorter.name);
        router.navigate(url);
    }

    confirmRemoveServerWideSorter(sorter: sorterListItemModel) {
        this.confirmationMessage("Delete Server-Wide Custom Sorter",
            `You're deleting server-wide custom sorter: <br><ul><li><strong>${generalUtils.escapeHtml(sorter.name)}</strong></li></ul>`, {
                buttons: ["Cancel", "Delete"],
                html: true
            })
            .done(result => {
                if (result.can) {
                    this.serverWideSorters.remove(sorter);
                    this.deleteServerWideSorter(sorter.name);
                }
            })
    }   

    private deleteServerWideSorter(name: string) {
        return new deleteServerWideCustomSorterCommand(name)
            .execute()
            .always(() => {
                this.loadServerWideSorters();
            })
    }
}

export = serverWideCustomSorters;

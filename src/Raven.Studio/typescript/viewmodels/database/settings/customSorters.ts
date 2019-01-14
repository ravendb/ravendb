import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import getCustomSortersCommand = require("commands/database/settings/getCustomSortersCommand");
import deleteCustomSorterCommand = require("commands/database/settings/deleteCustomSorterCommand");
import database = require("models/resources/database");
import router = require("plugins/router");

class customSorters extends viewModelBase {
    
    sorters = ko.observableArray<Raven.Client.Documents.Queries.Sorting.SorterDefinition>([]);
    
    addUrl = ko.pureComputed(() => appUrl.forEditCustomSorter(this.activeDatabase()));
    
    constructor() {
        super();
        
        this.bindToCurrentInstance("confirmRemoveSorter");
    }
    
    activate(args: any) {
        super.activate(args);
        
        return this.loadSorters();
    }
    
    private loadSorters() {
        return new getCustomSortersCommand(this.activeDatabase())
            .execute()
            .done(sorters => {
                this.sorters(sorters);
            });
    }
    
    editSorter(sorter: Raven.Client.Documents.Queries.Sorting.SorterDefinition) {
        const url = appUrl.forEditCustomSorter(this.activeDatabase(), sorter.Name);
        router.navigate(url);
    }
    
    confirmRemoveSorter(sorter: Raven.Client.Documents.Queries.Sorting.SorterDefinition) {
        this.confirmationMessage("Delete Custom Sorter", "You're deleting custom sorter with name: " + sorter.Name, ["Cancel", "Delete"])
            .done(result => {
                if (result.can) {
                    this.sorters.remove(sorter);
                    this.deleteSorter(this.activeDatabase(), sorter.Name);
                }
            })
    }
    
    private deleteSorter(db: database, name: string) {
        return new deleteCustomSorterCommand(db, name)
            .execute()
            .always(() => {
                this.loadSorters();
            })
    }

}

export = customSorters;

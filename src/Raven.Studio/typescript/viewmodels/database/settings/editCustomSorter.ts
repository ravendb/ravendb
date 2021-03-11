import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import customSorter = require("models/database/settings/customSorter");
import saveCustomSorterCommand = require("commands/database/settings/saveCustomSorterCommand");
import getCustomSortersCommand = require("commands/database/settings/getCustomSortersCommand");
import messagePublisher = require("common/messagePublisher");
import fileImporter = require("common/fileImporter");

class editCustomSorter extends viewModelBase {

    editedSorter = ko.observable<customSorter>();
    
    usedSorterNames = ko.observableArray<string>([]);

    isAddingNewSorter = ko.observable<boolean>();
    
    spinners = {
        save: ko.observable<boolean>(false)
    };
    
    constructor() {
        super();
        aceEditorBindingHandler.install();
    }

    activate(args: any) {
        super.activate(args);
        
        const db = this.activeDatabase();
        
        return new getCustomSortersCommand(db)
            .execute()
            .then(sorters => {
                this.isAddingNewSorter(!args.name);
                
                if (args.name) {
                    const matchedSorter = sorters.find(x => x.Name === args.name);
                    if (matchedSorter) {
                        this.editedSorter(new customSorter(matchedSorter));
                        this.dirtyFlag = this.editedSorter().dirtyFlag;
                    } else {
                        messagePublisher.reportWarning("Unable to find custom sorter named: " + args.name);
                        router.navigate(appUrl.forCustomSorters(db));
                        
                        return false;
                    }
                } else {
                    this.usedSorterNames(sorters.map(x => x.Name));
                    this.editedSorter(customSorter.empty());
                    this.dirtyFlag = this.editedSorter().dirtyFlag;
                }
            });
    }

    cancelOperation() {
        this.goToCustomSortersView();
    }

    private goToCustomSortersView() {
        router.navigate(appUrl.forCustomSorters(this.activeDatabase()));
    }

    fileSelected(fileInput: HTMLInputElement) {
        fileImporter.readAsText(fileInput, (data, fileName) => {
            this.editedSorter().code(data);

            if (fileName.endsWith(".cs")) {
                fileName = fileName.substr(0, fileName.length - 3);
            }

            if (this.isAddingNewSorter()) {
                this.editedSorter().name(fileName);
            }
        });
    }
    
    save() {
        if (this.isValid(this.editedSorter().validationGroup)) {
            this.spinners.save(true);
            
            new saveCustomSorterCommand(this.activeDatabase(), this.editedSorter().toDto())
                .execute()
                .done(() => {
                    this.dirtyFlag().reset();
                    this.goToCustomSortersView();
                })
                .always(() => this.spinners.save(false));
        }
    }
}

export = editCustomSorter;

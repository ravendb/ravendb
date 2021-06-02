import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import fileImporter = require("common/fileImporter");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import customSorter = require("models/database/settings/customSorter");
import messagePublisher = require("common/messagePublisher");
import getServerWideCustomSortersCommand = require("commands/serverWide/sorters/getServerWideCustomSortersCommand");
import saveServerWideCustomSorterCommand = require("commands/serverWide/sorters/saveServerWideCustomSorterCommand");

class editServerWideCustomSorter extends viewModelBase {

    editedServerWideSorter = ko.observable<customSorter>();

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

        return new getServerWideCustomSortersCommand()
            .execute()
            .then(sorters => {
                this.isAddingNewSorter(!args);

                if (args && args.sorterName) {
                    const matchedSorter = sorters.find(x => x.Name === args.sorterName);
                    if (matchedSorter) {
                        this.editedServerWideSorter(new customSorter(matchedSorter));
                        this.dirtyFlag = this.editedServerWideSorter().dirtyFlag;
                    } else {
                        messagePublisher.reportWarning("Unable to find server-wide custom sorter named: " + args.sorterName);
                        router.navigate(appUrl.forServerWideCustomSorters());

                        return false;
                    }
                } else {
                    this.usedSorterNames(sorters.map(x => x.Name));
                    this.editedServerWideSorter(customSorter.empty());
                    this.dirtyFlag = this.editedServerWideSorter().dirtyFlag;
                }
            });
    }

    cancelOperation() {
        this.goToServerWideCustomSortersView();
    }

    private goToServerWideCustomSortersView() {
        router.navigate(appUrl.forServerWideCustomSorters());
    }

    fileSelected(fileInput: HTMLInputElement) {
        fileImporter.readAsText(fileInput, (data, fileName) => {
            this.editedServerWideSorter().code(data);

            if (fileName.endsWith(".cs")) {
                fileName = fileName.substr(0, fileName.length - 3);
            }

            if (this.isAddingNewSorter()) {
                this.editedServerWideSorter().name(fileName);
            }
        });
    }

    save() {
        if (this.isValid(this.editedServerWideSorter().validationGroup)) {
            this.spinners.save(true);

            new saveServerWideCustomSorterCommand(this.editedServerWideSorter().toDto())
                .execute()
                .done(() => {
                    this.dirtyFlag().reset();
                    this.goToServerWideCustomSortersView();
                })
                .always(() => this.spinners.save(false));
        }
    }
}

export = editServerWideCustomSorter;

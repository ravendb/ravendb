import copyToClipboard = require("common/copyToClipboard");
import viewModelBase = require("viewmodels/viewModelBase");
import createSampleDataCommand = require("commands/database/studio/createSampleDataCommand");
import createSampleDataClassCommand = require("commands/database/studio/createSampleDataClassCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

class createSampleData extends viewModelBase {

    classData = ko.observable<string>();

    constructor() {
        super();
        aceEditorBindingHandler.install();
    }

    generateSampleData() {
        this.isBusy(true);
        
        new createSampleDataCommand(this.activeDatabase())
            .execute()
            .always(() => this.isBusy(false));
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('OGRN53');

        return this.fetchSampleDataClasses();
    }

    copyClasses() {
        copyToClipboard.copy(this.classData(), "Copied C# classes to clipboard.");
    }

    private fetchSampleDataClasses(): JQueryPromise<string> {
        return new createSampleDataClassCommand(this.activeDatabase())
            .execute()
            .done((results: string) => {
                this.classData(results);
            });
    }
}

export = createSampleData; 

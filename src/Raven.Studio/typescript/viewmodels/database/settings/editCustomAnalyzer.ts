import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import customAnalyzer = require("models/database/settings/customAnalyzer");
import saveCustomAnalyzerCommand = require("commands/database/settings/saveCustomAnalyzerCommand");
import getCustomAnalyzersCommand = require("commands/database/settings/getCustomAnalyzersCommand");
import messagePublisher = require("common/messagePublisher");
import fileImporter = require("common/fileImporter");

class editCustomAnalyzer extends viewModelBase {

    editedAnalyzer = ko.observable<customAnalyzer>();
    usedAnalyzerNames = ko.observableArray<string>([]);
    isAddingNewAnalyzer = ko.observable<boolean>();
    
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
        
        return new getCustomAnalyzersCommand(db)
            .execute()
            .then(analyzers => {
                this.isAddingNewAnalyzer(!args.name);
                
                if (args.name) {
                    const matchedAnalyzer = analyzers.find(x => x.Name === args.name);
                    if (matchedAnalyzer) {
                        this.editedAnalyzer(new customAnalyzer(matchedAnalyzer));
                        this.dirtyFlag = this.editedAnalyzer().dirtyFlag;
                    } else {
                        messagePublisher.reportWarning("Unable to find custom analyzer named: " + args.name);
                        router.navigate(appUrl.forCustomAnalyzers(db));
                        
                        return false;
                    }
                } else {
                    this.usedAnalyzerNames(analyzers.map(x => x.Name));
                    this.editedAnalyzer(customAnalyzer.empty());
                    this.dirtyFlag = this.editedAnalyzer().dirtyFlag;
                }
            });
    }

    cancelOperation() {
        this.goToCustomAnalyzersView();
    }

    private goToCustomAnalyzersView() {
        router.navigate(appUrl.forCustomAnalyzers(this.activeDatabase()));
    }

    fileSelected(fileInput: HTMLInputElement) {
        fileImporter.readAsText(fileInput, (data, fileName) => {
            this.editedAnalyzer().code(data);

            if (fileName.endsWith(".cs")) {
                fileName = fileName.substr(0, fileName.length - 3);
            }

            if (this.isAddingNewAnalyzer()) {
                this.editedAnalyzer().name(fileName);
            }
        });
    }
    
    save() {
        if (this.isValid(this.editedAnalyzer().validationGroup)) {
            this.spinners.save(true);
            
            new saveCustomAnalyzerCommand(this.activeDatabase(), this.editedAnalyzer().toDto())
                .execute()
                .done(() => {
                    this.dirtyFlag().reset();
                    this.goToCustomAnalyzersView();
                })
                .always(() => this.spinners.save(false));
        }
    }
}

export = editCustomAnalyzer;

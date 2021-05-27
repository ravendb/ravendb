import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import customAnalyzer = require("models/database/settings/customAnalyzer");
import saveServerWideCustomAnalyzerCommand = require("commands/serverWide/analyzers/saveServerWideCustomAnalyzerCommand");
import getServerWideCustomAnalyzersCommand = require("commands/serverWide/analyzers/getServerWideCustomAnalyzersCommand");
import messagePublisher = require("common/messagePublisher");
import fileImporter = require("common/fileImporter");
import editCustomAnalyzer = require("viewmodels/database/settings/editCustomAnalyzer");

class editServerWideCustomAnalyzer extends viewModelBase {

    editedServerWideAnalyzer = ko.observable<customAnalyzer>();
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

        return new getServerWideCustomAnalyzersCommand()
            .execute()
            .then((analyzers: Array<Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition>) => {
                this.isAddingNewAnalyzer(!args);

                if (args && args.analyzerName) {
                    const matchedAnalyzer = analyzers.find(x => x.Name === args.analyzerName);
                    if (matchedAnalyzer) {
                        this.editedServerWideAnalyzer(new customAnalyzer(matchedAnalyzer));
                        this.dirtyFlag = this.editedServerWideAnalyzer().dirtyFlag;
                    } else {
                        messagePublisher.reportWarning("Unable to find server-wide custom analyzer named: " + args.analyzerName);
                        router.navigate(appUrl.forServerWideCustomAnalyzers());

                        return false;
                    }
                } else {
                    this.usedAnalyzerNames(analyzers.map(x => x.Name));
                    this.editedServerWideAnalyzer(customAnalyzer.empty());
                    this.dirtyFlag = this.editedServerWideAnalyzer().dirtyFlag;
                }
            });
    }

    cancelOperation() {
        this.goToServerWideCustomAnalyzersView();
    }

    private goToServerWideCustomAnalyzersView() {
        router.navigate(appUrl.forServerWideCustomAnalyzers());
    }

    fileSelected(fileInput: HTMLInputElement) {
        fileImporter.readAsText(fileInput, (data, fileName) => {
            this.editedServerWideAnalyzer().code(data);

            if (fileName.endsWith(".cs")) {
                fileName = fileName.substr(0, fileName.length - 3);
            }

            if (this.isAddingNewAnalyzer()) {
                this.editedServerWideAnalyzer().name(fileName);
            }
        });
    }

    save() {
        if (this.isValid(this.editedServerWideAnalyzer().validationGroup)) {
            editCustomAnalyzer.maybeShowIndexResetNotice(this.isAddingNewAnalyzer())
                .done(() => {
                    this.spinners.save(true);

                    new saveServerWideCustomAnalyzerCommand(this.editedServerWideAnalyzer().toDto())
                        .execute()
                        .done(() => {
                            this.dirtyFlag().reset();
                            this.goToServerWideCustomAnalyzersView();
                        })
                        .always(() => this.spinners.save(false));
                });
        }
    }
}

export = editServerWideCustomAnalyzer;

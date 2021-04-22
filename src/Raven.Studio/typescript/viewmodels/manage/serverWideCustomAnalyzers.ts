import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import analyzerListItemModel = require("models/database/settings/analyzerListItemModel");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import getServerWideCustomAnalyzersCommand = require("commands/serverWide/analyzers/getServerWideCustomAnalyzersCommand");
import router = require("plugins/router");
import generalUtils = require("common/generalUtils");
import deleteServerWideCustomAnalyzerCommand = require("commands/serverWide/analyzers/deleteServerWideCustomAnalyzerCommand");

class serverWideCustomAnalyzers extends viewModelBase {
    serverWideAnalyzers = ko.observableArray<analyzerListItemModel>([]);

    addUrl = ko.pureComputed(() => appUrl.forEditServerWideCustomAnalyzer());

    clientVersion = viewModelBase.clientVersion;

    constructor() {
        super();

        aceEditorBindingHandler.install();
        this.bindToCurrentInstance("confirmRemoveServerWideAnalyzer", "editServerWideAnalyzer");
    }

    activate(args: any) {
        super.activate(args);

        return this.loadServerWideAnalyzers();
    }

    private loadServerWideAnalyzers() {
        return new getServerWideCustomAnalyzersCommand()
            .execute()
            .done(analyzers => {
                this.serverWideAnalyzers(analyzers.map(x => new analyzerListItemModel(x)));
            });
    }

    editServerWideAnalyzer(analyzer: analyzerListItemModel) {
        const url = appUrl.forEditServerWideCustomAnalyzer(analyzer.name);
        router.navigate(url);
    }

    confirmRemoveServerWideAnalyzer(analyzer: analyzerListItemModel) {
        this.confirmationMessage("Delete Server-Wide Custom Analyzer",
            `You're deleting server-wide custom analyzer: <br><ul><li><strong>${generalUtils.escapeHtml(analyzer.name)}</strong></li></ul>`, {
                buttons: ["Cancel", "Delete"],
                html: true
            })
            .done(result => {
                if (result.can) {
                    this.serverWideAnalyzers.remove(analyzer);
                    this.deleteServerWideAnalyzer(analyzer.name);
                }
            })
    }

    private deleteServerWideAnalyzer(name: string) {
        return new deleteServerWideCustomAnalyzerCommand(name)
            .execute()
            .always(() => {
                this.loadServerWideAnalyzers();
            })
    }
}

export = serverWideCustomAnalyzers;

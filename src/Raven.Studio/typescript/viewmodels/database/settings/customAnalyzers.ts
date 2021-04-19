import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import getCustomAnalyzersCommand = require("commands/database/settings/getCustomAnalyzersCommand");
import deleteCustomAnalyzerCommand = require("commands/database/settings/deleteCustomAnalyzerCommand");
import getServerWideCustomAnalyzersCommand = require("commands/serverWide/analyzers/getServerWideCustomAnalyzersCommand");
import database = require("models/resources/database");
import router = require("plugins/router");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import generalUtils = require("common/generalUtils");
import analyzerListItemModel = require("models/database/settings/analyzerListItemModel");
import accessManager = require("common/shell/accessManager");


class customAnalyzers extends viewModelBase {
    analyzers = ko.observableArray<analyzerListItemModel>([]);
    serverWideAnalyzers = ko.observableArray<analyzerListItemModel>([]);
    
    addUrl = ko.pureComputed(() => appUrl.forEditCustomAnalyzer(this.activeDatabase()));
    
    serverWideCustomAnalyzersUrl = appUrl.forServerWideCustomAnalyzers();
    canNavigateToServerWideCustomAnalyzers: KnockoutComputed<boolean>;
    
    constructor() {
        super();
        
        aceEditorBindingHandler.install();
        this.bindToCurrentInstance("confirmRemoveAnalyzer", "editAnalyzer");

        this.canNavigateToServerWideCustomAnalyzers = accessManager.default.isClusterAdminOrClusterNode;
    }
    
    activate(args: any) {
        super.activate(args);
        
        return $.when<any>(this.loadAnalyzers(), this.loadServerWideAnalyzers())
            .done(() => {
                const serverWideAnalyzerNames = this.serverWideAnalyzers().map(x => x.name);
                
                this.analyzers().forEach(analyzer => {
                    if (_.includes(serverWideAnalyzerNames, analyzer.name)) {
                        analyzer.overrideServerWide(true);
                    }
                })
            })
    }
    
    private loadAnalyzers() {
        return new getCustomAnalyzersCommand(this.activeDatabase())
            .execute()
            .done(analyzers => this.analyzers(analyzers.map(x => new analyzerListItemModel(x))));
    }

    private loadServerWideAnalyzers() {
        return new getServerWideCustomAnalyzersCommand()
            .execute()
            .done(analyzers => this.serverWideAnalyzers(analyzers.map(x => new analyzerListItemModel(x))));
    }

    compositionComplete() {
        super.compositionComplete();

        $('.custom-analyzers [data-toggle="tooltip"]').tooltip();
    }
    
    editAnalyzer(analyzer: analyzerListItemModel) {
        const url = appUrl.forEditCustomAnalyzer(this.activeDatabase(), analyzer.name);
        router.navigate(url);
    }
    
    confirmRemoveAnalyzer(analyzer: analyzerListItemModel) {
        this.confirmationMessage("Delete Custom Analyzer",
            `You're deleting custom analyzer: <br><ul><li><strong>${generalUtils.escapeHtml(analyzer.name)}</strong></li></ul>`, {
            buttons: ["Cancel", "Delete"],
            html: true
        })
            .done(result => {
                if (result.can) {
                    this.analyzers.remove(analyzer);
                    this.deleteAnalyzer(this.activeDatabase(), analyzer.name);
                }
            })
    }
    
    private deleteAnalyzer(db: database, name: string) {
        return new deleteCustomAnalyzerCommand(db, name)
            .execute()
            .always(() => {
                this.loadAnalyzers();
            })
    }
}

export = customAnalyzers;

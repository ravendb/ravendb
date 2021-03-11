import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import getCustomAnalyzersCommand = require("commands/database/settings/getCustomAnalyzersCommand");
import deleteCustomAnalyzerCommand = require("commands/database/settings/deleteCustomAnalyzerCommand");
import database = require("models/resources/database");
import router = require("plugins/router");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import generalUtils = require("common/generalUtils");

class analyzerListItem {
    
    definition: Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition;
    
    constructor(dto: Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition) {
        this.definition = dto;
    }
    
    get name() {
        return this.definition.Name;
    }
}

class customAnalyzers extends viewModelBase {
    analyzers = ko.observableArray<analyzerListItem>([]);
    
    addUrl = ko.pureComputed(() => appUrl.forEditCustomAnalyzer(this.activeDatabase()));
    
    isFirstRun = true;
    
    constructor() {
        super();
        
        aceEditorBindingHandler.install();
        this.bindToCurrentInstance("confirmRemoveAnalyzer", "editAnalyzer");
    }
    
    activate(args: any) {
        super.activate(args);
        
        return this.loadAnalyzers();
    }
    
    private loadAnalyzers() {
        return new getCustomAnalyzersCommand(this.activeDatabase())
            .execute()
            .done(analyzers => {
                this.analyzers(analyzers.map(x => new analyzerListItem(x)));
            });
    }
    
    editAnalyzer(analyzer: analyzerListItem) {
        const url = appUrl.forEditCustomAnalyzer(this.activeDatabase(), analyzer.name);
        router.navigate(url);
    }
    
    confirmRemoveAnalyzer(analyzer: analyzerListItem) {
        this.confirmationMessage("Delete Custom Analyzer",
            `You're deleting custom analyzer: <br><ul><li>${generalUtils.escapeHtml(analyzer.name)}</li></ul>`, {
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

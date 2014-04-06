import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import scriptedIndex = require("models/scriptedIndex");
import scriptedIndexMap = require("models/scriptedIndexMap");
import getScriptedIndexesCommand = require("commands/getScriptedIndexesCommand");
import saveScriptedIndexesCommand = require("commands/saveScriptedIndexesCommand");

class scriptedIndexes extends viewModelBase {

    indexNames = ko.observableArray<string>();
    selectedIndex = ko.observable<string>();

    scrIndexes = ko.observable<scriptedIndexMap>().extend({ required: true });
    scrIndex = ko.observable<scriptedIndex>();

    constructor() {
        super();

        aceEditorBindingHandler.install();
    }

    activate() {
        this.fetchAllIndexes();
    }

    fetchAllIndexes(): JQueryPromise<any> {
        return new getDatabaseStatsCommand(this.activeDatabase())
            .execute()
            .done((results: databaseStatisticsDto) => { this.performAllIndexesResult(results); });
    }

    performAllIndexesResult(results: databaseStatisticsDto) {
        this.indexNames(results.Indexes.map(i => i.PublicName));
        this.fetchAllScriptedIndexes();
    }

    fetchAllScriptedIndexes() {
        new getScriptedIndexesCommand(this.activeDatabase())
            .execute()
            .done((indexes: scriptedIndex[])=> {
                this.performAllScriptedIndexes(indexes);
                viewModelBase.dirtyFlag = new ko.DirtyFlag([this.scrIndexes]);
        });
    }

    performAllScriptedIndexes(indexes: scriptedIndex[]) {
        this.scrIndexes(new scriptedIndexMap(indexes));
        if (this.indexNames().length > 0) {
            this.setSelectedIndex(this.indexNames.first());
        }
    }

    attached() {
        this.addIndexScriptHelpPopover();
        this.addDeleteScriptHelpPopover();
    }

    addIndexScriptHelpPopover() {
        $("#indexScriptLabel").popover({
            html: true,
            trigger: 'hover',
            content: 'Index Scripts are written in JScript.<br/><br/>Example:</br><pre><span class="code-keyword">var</span> company = LoadDocument(<span class="code-keyword">this</span>.Company);<br/><span class="code-keyword">if</span>(company == null) <span class="code-keyword">return</span>;<br/>company.Orders = { Count: <span class="code-keyword">this</span>.Count, Total: <span class="code-keyword">this</span>.Total };<br/>PutDocument(<span class="code-keyword">this</span>.Company, company);</pre>',
        });
    }

    addDeleteScriptHelpPopover() {
        $("#deleteScriptLabel").popover({
            html: true,
            trigger: 'hover',
            content: 'Index Scripts are written in JScript.<br/><br/>Example:</br><pre><span class="code-keyword">var</span> company = LoadDocument(<span class="code-keyword">this</span>.Company);<br/><span class="code-keyword">if</span> (company == null) <span class="code-keyword">return</span>;<br/><span class="code-keyword">delete</span> company.Orders;<br/>PutDocument(<span class="code-keyword">this</span>.Company, company);</pre>',
        });
    }

    setSelectedIndex(indexName: string) {
        this.selectedIndex(indexName);
        this.getIndexForName(indexName);
    }

    createNewScript() {
        this.scrIndexes().addEmptyIndex(this.selectedIndex());
        this.getIndexForName(this.selectedIndex());
    }

    private getIndexForName(indexName: string) {
        var index = this.scrIndexes().getIndex(indexName);
        this.scrIndex(index);
    }

    saveChanges() {
        new saveScriptedIndexesCommand(this.scrIndexes(), this.activeDatabase())
            .execute()
            .done((result: bulkDocumentDto[])=> {
                this.updateIndexes(result);
                // Resync Changes
                viewModelBase.dirtyFlag().reset();
        });
    }

    private updateIndexes(serverIndexes: bulkDocumentDto[]) {
        this.scrIndexes().getIndexes().forEach(index => {
            var serverIndex = serverIndexes.first(k => k.Key === index.getId());
            if (serverIndex && !serverIndex.Deleted) {
                index.__metadata.etag = serverIndex.Etag;
                index.__metadata.lastModified = serverIndex.Metadata['Last-Modified'];
            }
        });
    }

    deleteScript() {
        this.scrIndexes().removeIndex(this.selectedIndex());
        this.getIndexForName(this.selectedIndex());
    }
}

export = scriptedIndexes; 
import ace = require("ace/ace");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import scriptedIndex = require("models/scriptedIndex");
import scriptedIndexMap = require("models/scriptedIndexMap");
import getScriptedIndexesCommand = require("commands/getScriptedIndexesCommand");
import saveScriptedIndexesCommand = require("commands/saveScriptedIndexesCommand");

class scriptedIndexes extends viewModelBase {

    selectedIndex = ko.observable<string>();
    isSaveEnabled: KnockoutComputed<boolean>;
    scrIndexes = ko.observable<scriptedIndexMap>();
    scrIndex = ko.observable<scriptedIndex>();
    isScriptIndexVisible: KnockoutComputed<boolean>;
    isFirstLoad: boolean = true;

    allScriptedIndexes = ko.observableArray<scriptedIndex>();
    activeScriptedIndexes = ko.observableArray<scriptedIndex>().extend({ required: true });
    indexNames = ko.observableArray<string>();
    inactiveIndexNames: KnockoutComputed<Array<string>>;

    constructor() {
        super();

        aceEditorBindingHandler.install();
        this.inactiveIndexNames = ko.computed(function() {
            var activeIndexNames = this.allScriptedIndexes()
                .filter((index: scriptedIndex)=> { return !index.isMarkedToDelete(); })
                .map((index: scriptedIndex) => index.indexName());
            return this.indexNames().filter((indexName: string) => { return activeIndexNames.indexOf(indexName) < 0; });
        }, this);
    }

    canActivate(args: any): any {
        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            this.fetchAllIndexes(db)
                .done(() => {
                    this.fetchAllScriptedIndexes(db)
                    .done(()=> {
                        deferred.resolve({ can: true });
                    });
                });
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);

        this.isScriptIndexVisible = ko.computed(() => {
            return this.scrIndex() && !this.scrIndex().isMarkedToDelete();
        }, this);
        viewModelBase.dirtyFlag = new ko.DirtyFlag([this.scrIndexes().activeScriptedIndexes]);
        this.isSaveEnabled = ko.computed(() => {
            return viewModelBase.dirtyFlag().isDirty();
        });
    }

    attached() {
        var popOverSettings = {
            html: true,
            trigger: 'hover',
            content: 'Index Scripts are written in JScript.<br/><br/>Example:</br><pre><span class="code-keyword">var</span> company = LoadDocument(<span class="code-keyword">this</span>.Company);<br/><span class="code-keyword">if</span>(company == null) <span class="code-keyword">return</span>;<br/>company.Orders = { Count: <span class="code-keyword">this</span>.Count, Total: <span class="code-keyword">this</span>.Total };<br/>PutDocument(<span class="code-keyword">this</span>.Company, company);</pre>',
            //selector: '.index-script-label'
            selector: '#indexScriptLabel',
        };
        $('body').popover(popOverSettings);
        var popOverSettings2 = {
            html: true,
            trigger: 'hover',
            content: 'Index Scripts are written in JScript.<br/><br/>Example:</br><pre><span class="code-keyword">var</span> company = LoadDocument(<span class="code-keyword">this</span>.Company);<br/><span class="code-keyword">if</span> (company == null) <span class="code-keyword">return</span>;<br/><span class="code-keyword">delete</span> company.Orders;<br/>PutDocument(<span class="code-keyword">this</span>.Company, company);</pre>',
            //selector: '.delete-script-label'
            selector: '#deleteScriptLabel',
        };
        $('#scriptedIndex').popover(popOverSettings2);
        //this.initializeScriptsTextboxes();
    }

    fetchAllIndexes(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getDatabaseStatsCommand(db)
            .execute()
            .done((results: databaseStatisticsDto) => {
                this.performAllIndexesResult(results);
                deferred.resolve({ can: true });
            });
        return deferred;
    }

    performAllIndexesResult(results: databaseStatisticsDto) {
        this.indexNames(results.Indexes.map(i => i.PublicName));
    }

    fetchAllScriptedIndexes(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getScriptedIndexesCommand(db)
            .execute()
            .done((indexes: scriptedIndex[])=> {
                this.performAllScriptedIndexes(indexes);


                this.allScriptedIndexes.pushAll(indexes);
                this.activeScriptedIndexes.pushAll(indexes);
            //this.activeScriptedIndexes.valueHasMutated();
                deferred.resolve({ can: true });
            });
        return deferred;
    }

    performAllScriptedIndexes(indexes: scriptedIndex[]) {
        this.scrIndexes(new scriptedIndexMap(indexes));
        if (this.indexNames().length > 0) {
            this.setSelectedIndex(this.indexNames.first());
        }
    }

    initializeScriptsTextboxes() {
        //this.addIndexScriptHelpPopover();
        //this.addDeleteScriptHelpPopover();
        //this.startupAceEditor();
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

        if (index && !this.isFirstLoad) {
            this.initializeScriptsTextboxes();
        } else {
            this.isFirstLoad = false;
        }
    }

    /*startupAceEditor() {
        // Startup the Ace editor
        if ($("#indexScriptEditor").length > 0) {
            ace.edit("indexScriptEditor").focus();
        }
        $("#indexScriptEditor").on('keyup', ".ace_text-input", () => {
            var value = ace.edit("indexScriptEditor").getSession().getValue();
            this.scrIndex().indexScript(value);
        });
        $("#deleteScriptEditor").on('keyup', ".ace_text-input", () => {
            var value = ace.edit("deleteScriptEditor").getSession().getValue();
            this.scrIndex().deleteScript(value);
        });
    }*/

    saveChanges() {
        new saveScriptedIndexesCommand(this.scrIndexes(), this.activeDatabase())
            .execute()
            .done((result: bulkDocumentDto[]) => {
                this.updateIndexes(result);
                
                viewModelBase.dirtyFlag().reset(); //Resync Changes
        });
    }

    private updateIndexes(serverIndexes: bulkDocumentDto[]) {
        this.scrIndexes().getIndexes().forEach(index => {
            var serverIndex = serverIndexes.first(k => k.Key === index.getId());
            if (serverIndex && !serverIndex.Deleted) {
                index.__metadata.etag = serverIndex.Etag;
                index.__metadata.lastModified = serverIndex.Metadata['Last-Modified'];
            }
            else if (serverIndex && serverIndex.Deleted) { //remove mark to deleted indexes
                this.scrIndexes().deleteMarkToDeletedIndex(serverIndex.Key);
            }
        });
    }

    deleteScriptedIndex() {
        this.scrIndexes().removeIndex(this.selectedIndex());
        this.getIndexForName(this.selectedIndex());
    }




    createScriptedIndex(indexName: string) {
        var results = this.allScriptedIndexes().filter((index: scriptedIndex) => { return index.indexName() == indexName; });
        var activeScriptedIndex: scriptedIndex = results[0];
        if (activeScriptedIndex) {
            activeScriptedIndex.cancelDeletion();

        } else {
            activeScriptedIndex = scriptedIndex.emptyForIndex(indexName);
            this.allScriptedIndexes.push(activeScriptedIndex);
            this.activeScriptedIndexes.push(activeScriptedIndex);
        }
    }

    removeScriptedIndex(scriptedIndexToDelete: scriptedIndex) {
        scriptedIndexToDelete.markToDelete();
    }
}

export = scriptedIndexes; 
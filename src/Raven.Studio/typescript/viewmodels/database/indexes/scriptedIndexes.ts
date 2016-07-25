import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import database = require("models/resources/database");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import scriptedIndex = require("models/database/index/scriptedIndex");
import getScriptedIndexesCommand = require("commands/database/index/getScriptedIndexesCommand");
import saveScriptedIndexesCommand = require("commands/database/documents/saveScriptedIndexesCommand");
import appUrl = require("common/appUrl");

class scriptedIndexes extends viewModelBase {

    allScriptedIndexes = ko.observableArray<scriptedIndex>();
    activeScriptedIndexes = ko.observableArray<scriptedIndex>().extend({ required: true });
    indexNames = ko.observableArray<string>();
    inactiveIndexNames: KnockoutComputed<Array<string>>;
    isSaveEnabled: KnockoutComputed<boolean>;
    firstIndex: KnockoutComputed<number>;
    isFirstLoad = ko.observable(true);

    constructor() {
        super();

        aceEditorBindingHandler.install();
        this.inactiveIndexNames = ko.computed(() => {
            var activeIndexNames = this.allScriptedIndexes()
                .filter((index: scriptedIndex)=> { return !index.isMarkedToDelete(); })
                .map((index: scriptedIndex) => index.indexName());
            return this.indexNames().filter((indexName: string) => { return activeIndexNames.indexOf(indexName) < 0; });
        }, this);
        this.firstIndex = ko.computed(() => {
            return !this.isFirstLoad() ? 0 : -1;
        }, this);
    }

    canActivate(args: any): any {
        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            this.fetchAllIndexes(db)
                .done(() => {
                    this.fetchAllScriptedIndexes(db)
                        .done(() => deferred.resolve({ can: true }))
                        .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseSettings(this.activeDatabase()) }));
                });
        }
        return deferred;
    }

    activate(args: any) {
        super.activate(args);

        this.dirtyFlag = new ko.DirtyFlag([this.activeScriptedIndexes]);
        this.isSaveEnabled = ko.computed(() => this.dirtyFlag().isDirty());
    }

    compositionComplete() {
        super.compositionComplete();

        this.addScriptsLabelPopover();
        this.initializeCollapsedInvalidElements();

        $('pre').each((index, currentPreElement) => {
            if (currentPreElement) {
                var editor: AceAjax.Editor = ko.utils.domData.get(currentPreElement, "aceEditor");
                var editorValue = editor.getSession().getValue();
                this.initializeAceValidity(currentPreElement, editorValue);
            }
        });
    }

    createScriptedIndex(indexName: string) {
        if (this.isFirstLoad()) {
            this.isFirstLoad(false);
        }
        var results = this.allScriptedIndexes().filter((index: scriptedIndex) => { return index.indexName() == indexName; });
        var activeScriptedIndex: scriptedIndex = results[0];
        if (activeScriptedIndex) {
            activeScriptedIndex.cancelDeletion();
        } else {
            activeScriptedIndex = scriptedIndex.emptyForIndex(indexName);
            this.allScriptedIndexes.unshift(activeScriptedIndex);
        }
        this.activeScriptedIndexes.unshift(activeScriptedIndex);

        var twoCreatedPreElements = $('.in pre');
        var editor = ko.utils.domData.get(twoCreatedPreElements[0], "aceEditor");
        editor.focus();

        twoCreatedPreElements.each((index, preElement) => {
            this.initializeAceValidity(preElement, "");
        });
        this.initializeCollapsedInvalidElements();
    }

    removeScriptedIndex(scriptedIndexToDelete: scriptedIndex) {
        this.activeScriptedIndexes.remove(scriptedIndexToDelete);
        scriptedIndexToDelete.markToDelete();
    }

    saveChanges() {
        var db = this.activeDatabase();
        new saveScriptedIndexesCommand(this.allScriptedIndexes(), db)
            .execute()
            .done((result: bulkDocumentDto[]) => {
                this.updateIndexes(result);
                this.dirtyFlag().reset(); //Resync Changes
            });
    }

    private fetchAllIndexes(db: database): JQueryPromise<any> {
        return new getDatabaseStatsCommand(db)
            .execute()
            .done((results: databaseStatisticsDto) => this.performAllIndexesResult(results));
    }

    private performAllIndexesResult(results: databaseStatisticsDto) {
        this.indexNames(results.Indexes.map(i => i.Name));
    }

    private fetchAllScriptedIndexes(db: database): JQueryPromise<any> {
        return new getScriptedIndexesCommand(db)
            .execute()
            .done((indexes: scriptedIndex[]) => {
                this.allScriptedIndexes.pushAll(indexes);
                this.activeScriptedIndexes.pushAll(indexes);
            });
    }

    private addScriptsLabelPopover() {
        var indexScriptpopOverSettings = {
            html: true,
            trigger: 'hover',
            content: 'Index Scripts are written in JavaScript.<br/><br/>Example:</br><pre><span class="code-keyword">var</span> company = LoadDocument(<span class="code-keyword">this</span>.Company);<br/><span class="code-keyword">if</span>(company == null) <span class="code-keyword">return</span>;<br/>company.Orders = { Count: <span class="code-keyword">this</span>.Count, Total: <span class="code-keyword">this</span>.Total };<br/>PutDocument(<span class="code-keyword">this</span>.Company, company);</pre>',
            selector: '.index-script-label'
        };
        $('#accordion').popover(indexScriptpopOverSettings);
        var deleteScriptPopOverSettings = {
            html: true,
            trigger: 'hover',
            content: 'Index Scripts are written in JavaScript.<br/><br/>Example:</br><pre><span class="code-keyword">var</span> company = LoadDocument(<span class="code-keyword">this</span>.Company);<br/><span class="code-keyword">if</span> (company == null) <span class="code-keyword">return</span>;<br/><span class="code-keyword">delete</span> company.Orders;<br/>PutDocument(<span class="code-keyword">this</span>.Company, company);</pre>',
            selector: '.delete-script-label'
        };
        $('#scriptedIndexesForm').popover(deleteScriptPopOverSettings);
    }

    //when pressing the save button, show all elements which are collapsed and at least one of its' fields isn't valid.
    private initializeCollapsedInvalidElements() {
        $('textarea').bind('invalid', function (e) {
            var element: any = e.target;
            if (!element.validity.valid) {
                var parentElement = $(this).parents('.panel-default');
                parentElement.children('.collapse').collapse('show');
            }
        });
    }

    private initializeAceValidity(element: Element, editorValue: string) {
        if (editorValue === "") {
            var textarea: any = $(element).find('textarea')[0];
            textarea.setCustomValidity("Please fill out this field.");
        }
    }

    private updateIndexes(serverIndexes: bulkDocumentDto[]) {
        for (var i = 0; i < this.allScriptedIndexes().length; i++) {
            var index = this.allScriptedIndexes()[i];
            var serverIndex = serverIndexes.first(k => k.Key === index.getId());
            if (serverIndex && !serverIndex.Deleted) {
                index.__metadata.etag = serverIndex.Etag;
                index.__metadata.lastModified = serverIndex.Metadata['Last-Modified'];
            }
            else if (serverIndex && serverIndex.Deleted) { //remove mark to deleted index
                this.allScriptedIndexes().splice(i--, 1);
            }
        }
    }
}

export = scriptedIndexes; 

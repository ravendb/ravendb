import database = require("models/database");
import collection = require("models/collection");
import sqlReplication = require("models/sqlReplication");
import viewModelBase = require("viewmodels/viewModelBase");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import autoCompleteBindingHandler = require("common/autoCompleteBindingHandler");
import getSqlReplicationsCommand = require("commands/getSqlReplicationsCommand");
import saveSqlReplicationsCommand = require("commands/saveSqlReplicationsCommand");
import deleteDocumentsCommand = require("commands/deleteDocumentsCommand");
import getCollectionsCommand = require("commands/getCollectionsCommand");
import appUrl = require("common/appUrl");
import ace = require("ace/ace");


class sqlReplications extends viewModelBase {

    replications = ko.observableArray<sqlReplication>();
    collections = ko.observableArray<string>();
    isFirstLoad = ko.observable(true);
    firstIndex: KnockoutComputed<number>;
    areAllSqlReplicationsValid: KnockoutComputed<boolean>;
    isSaveEnabled: KnockoutComputed<boolean>;
    loadedSqlReplications: sqlReplication[];

    constructor() {
        super();
        aceEditorBindingHandler.install();
        autoCompleteBindingHandler.install();
        this.firstIndex = ko.computed(() => {
            return !this.isFirstLoad() ? 0 : -1;
        }, this);
    }

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();

        var db = this.activeDatabase();
        if (db) {
            $.when(this.fetchSqlReplications(db), this.fetchCollections(db))
                .done(() => {
                    this.replications().forEach((replication: sqlReplication) => {
                        replication.collections = this.collections;
                    });
                    deferred.resolve({ can: true });
                })
                .fail(() => deferred.resolve({ redirect: appUrl.forSettings(this.activeDatabase()) }));
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);
        viewModelBase.dirtyFlag = new ko.DirtyFlag([this.replications]);
        this.isSaveEnabled = ko.computed(()=> {
            return viewModelBase.dirtyFlag().isDirty();
        });
    }

    compositionComplete() {
        super.compositionComplete();

        this.addScriptLabelPopover();
        this.initializeCollapsedInvalidElements();

        this.replications().forEach((replication: sqlReplication) => {
            this.subscribeToSqlReplicationName(replication);
        });

        $('pre').each((index, currentPreElement) => {
            this.initializeAceValidity(currentPreElement);
        });
    }

    addNewSqlReplication() {
        if (this.isFirstLoad()) {
            this.isFirstLoad(false);
        }

        var newSqlReplication: sqlReplication = sqlReplication.empty();
        newSqlReplication.collections = this.collections;
        this.replications.unshift(newSqlReplication);
        this.subscribeToSqlReplicationName(newSqlReplication);
        $('.in').find('input[name="name"]').focus();

        var lastPreElement = $('pre').last().get(0);
        this.initializeAceValidity(lastPreElement);
        this.initializeCollapsedInvalidElements();
    }

    removeSqlReplication(sr: sqlReplication) {
        this.replications.remove(sr);

        this.replications().forEach((replication: sqlReplication) => {
            replication.name.valueHasMutated();
        });
    }

    saveChanges() {
        var db = this.activeDatabase();
        if (db) {
            this.replications().forEach((r: sqlReplication) => r.setIdFromName());

            var onScreenReplications: sqlReplication[] = this.replications();
            var deletedReplications: sqlReplication[] = this.loadedSqlReplications.filter((currentLoaded: sqlReplication) => {
                return onScreenReplications.filter((currentOnScreen: sqlReplication) => {
                    return currentLoaded.name() == currentOnScreen.name();
                }).length == 0;
            });
            var loadedSqlReplicationsIds = this.loadedSqlReplications.map((sr: sqlReplication) => sr.getId());

            for (var i = 0; i < onScreenReplications.length; i++) {
                var replication: sqlReplication = onScreenReplications[i];
                var replicationId = replication.getId();

                //clear the etag if the name of the replication was changed
                if (loadedSqlReplicationsIds.indexOf(replicationId) == -1) {
                    delete replication.__metadata.etag;
                    delete replication.__metadata.lastModified;
                }
            }

            var saveDeferred = this.saveSqlReplications(onScreenReplications, deletedReplications, db);
            saveDeferred.done(() => {
                    this.loadedSqlReplications = this.deepCopySqlReplications(this.replications()); //make a deep copy of the sql replications
                    viewModelBase.dirtyFlag().reset(); //Resync Changes
                });
        }
    }

    itemNumber = (index) => {
        return index + 1;
    }

    private fetchSqlReplications(db: database): JQueryPromise<any> {
        return new getSqlReplicationsCommand(db)
            .execute()
            .done((results: sqlReplication[])=> {
                this.loadedSqlReplications = this.deepCopySqlReplications(results); //make a deep copy of the sql replications
                this.replications(results);
            });
    }

    private fetchCollections(db: database): JQueryPromise<any> {
        return new getCollectionsCommand(db)
            .execute()
            .done((collections: Array<collection>) => {
                this.collections(collections.map((collection: collection) => { return collection.name; }));
            });
    }

    private addScriptLabelPopover() {
        var popOverSettings = {
            html: true,
            trigger: 'hover',
            content: 'Replication scripts use JScript.<br/><br/>The script will be called once for each document in the source document collection, with <span class="code-keyword">this</span> representing the document, and the document id available as <i>documentId</i>.<br/><br/>Call <i>replicateToTableName</i> for each row you want to write to the database.<br/><br/>Example:</br><pre><span class="code-keyword">var</span> orderData = {<br/>   Id: documentId,<br/>   OrderLinesCount: <span class="code-keyword">this</span>.OrderLines.length,<br/>   TotalCost: 0<br/>};<br/><br/>replicateToOrders(\'Id\', orderData);<br/><br/>for (<span class="code-keyword">var</span> i = 0; i &lt; <span class="code-keyword">this</span>.OrderLines.length; i++) {<br/>   <span class="code-keyword">var</span> line = <span class="code-keyword">this</span>.OrderLines[i];<br/>   orderData.TotalCost += line.Cost;<br/>   replicateToOrderLines(\'OrderId\', {"<br/>      OrderId: documentId,<br/>      Qty: line.Quantity,<br/>      Product: line.Product,<br/>      Cost: line.Cost<br/>   });<br/>}</pre>',
            selector: '.script-label',
        };
        $('body').popover(popOverSettings);
        $('form :input[name="ravenEntityName"]').on("keypress", (e) => {
            return e.which != 13;
        });
    }

    //when pressing the save button, show all elements which are collapsed and at least one of its' fields isn't valid.
    private initializeCollapsedInvalidElements() {
        $('input, textarea').bind('invalid', function (e) {
            var element: any = e.target;
            if (!element.validity.valid) {
                var parentElement = $(this).parents('.panel-default');
                parentElement.children('.collapse').collapse('show');
            }
        });
    }

    private subscribeToSqlReplicationName(sqlReplicationElement: sqlReplication) {
        sqlReplicationElement.name.subscribe((previousName) => {
            //Get the previous value of 'name' here before it's set to newValue
            var nameInputArray = $('input[name="name"]').filter(function() { return this.value === previousName; });
            if (nameInputArray.length === 1) {
                var inputField: any = nameInputArray[0];
                inputField.setCustomValidity("");
            }       
        }, this, "beforeChange");
        sqlReplicationElement.name.subscribe((newName) => {
            var message = "";
            if (newName === "") {
                message = "Please fill out this field.";
            }
            else if (this.isSqlReplicationNameExists(newName)) {
                message = "SQL Replication name already exists.";
            }
            $('input[name="name"]')
                .filter(function () { return this.value === newName; })
                .each((index, element: any) => {
                    element.setCustomValidity(message);
                });
        });
    }

    private isSqlReplicationNameExists(name): boolean {
        var count = 0;
        this.replications().forEach((replication: sqlReplication) => {
            if (replication.name() === name) {
                count++;
            }
        });
        return (count > 1) ? true : false;
    }

    private initializeAceValidity(element: Element) {
        var editor: AceAjax.Editor = ko.utils.domData.get(element, "aceEditor");
        var editorValue = editor.getSession().getValue();
        if (editorValue === "") {
            var textarea: any = $(element).find('textarea')[0];
            textarea.setCustomValidity("Please fill out this field.");
        }
    }

    private saveSqlReplications(onScreenReplications, deletedReplications, db): JQueryDeferred<{}>{
        var saveDeferred = $.Deferred();
        //save the new/updated sql replications and delete the deleted ones
        if (onScreenReplications.length > 0 || deletedReplications.length > 0) {
            new saveSqlReplicationsCommand(onScreenReplications, deletedReplications, db)
                .execute()
                .done((result: bulkDocumentDto[]) => {
                    this.updateKeys(result);
                    saveDeferred.resolve();
                });
        } else {
            saveDeferred.resolve();
        }
        return saveDeferred;
    }

    private deepCopySqlReplications(replicationsArray: sqlReplication[]): sqlReplication[] {
        var copiedSqlReplications: sqlReplication[] = [];

        replicationsArray.forEach((replication: sqlReplication) => {
            var copiedSqlReplication = new sqlReplication(replication.toDto());
            copiedSqlReplications.push(copiedSqlReplication);
        });

        return copiedSqlReplications;
    }

    private updateKeys(serverKeys: bulkDocumentDto[]) {
        this.replications().forEach(key => {
            var serverKey = serverKeys.first(k => k.Key === key.getId());
            if (serverKey) {
                key.__metadata.etag = serverKey.Etag;
                key.__metadata.lastModified = serverKey.Metadata['Last-Modified'];
            }
        });
    }
}

export = sqlReplications; 
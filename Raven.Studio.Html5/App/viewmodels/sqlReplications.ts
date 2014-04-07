import sqlReplication = require("models/sqlReplication");
import viewModelBase = require("viewmodels/viewModelBase");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import getSqlReplicationsCommand = require("commands/getSqlReplicationsCommand");
import saveSqlReplicationsCommand = require("commands/saveSqlReplicationsCommand");
import deleteDocumentsCommand = require("commands/deleteDocumentsCommand");
import appUrl = require("common/appUrl");

class sqlReplications extends viewModelBase {

    replications = ko.observableArray<sqlReplication>();
    isFirstload = ko.observable(true);
    lastIndex = ko.computed(function () {
        return this.isFirstload() ? -1 : this.replications().length - 1;
    }, this);
    isSaveEnabled: KnockoutComputed<boolean>;
    loadedSqlReplications = [];

    constructor() {
        super();

        aceEditorBindingHandler.install();
    }

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            new getSqlReplicationsCommand(db)
                .execute()
                .done(results => {
                    for (var i = 0; i < results.length; i++) {
                        this.loadedSqlReplications.push(results[i].getId());
                    }
                    this.replications(results);
                    
                    deferred.resolve({ can: true });
                })
                .fail(() => deferred.resolve({ redirect: appUrl.forIndexes(this.activeDatabase()) }));
        }
        return deferred;
    }

    activate() {
        viewModelBase.dirtyFlag = new ko.DirtyFlag([this.replications]);
        this.isSaveEnabled = ko.computed(function () {
            return viewModelBase.dirtyFlag().isDirty();
        });
    }

    attached() {
        $(".script-label").popover({
            html: true,
            trigger: 'hover',
            content: 'Replication scripts use JScript.',
        });
    }

    saveChanges() {
        var db = this.activeDatabase();
        if (db) {
            this.replications().forEach(r => r.setIdFromName());
            var deletedReplications = this.loadedSqlReplications.slice(0);
            var onScreenReplications = this.replications();

            for (var i = 0; i < onScreenReplications.length; i++) {
                var replication: sqlReplication = onScreenReplications[i];
                var replicationId = replication.getId();
                deletedReplications.remove(replicationId);

                if (this.loadedSqlReplications.indexOf(replicationId) == -1) {
                    delete replication.__metadata.etag;
                    delete replication.__metadata.lastModified;
                }
            }

            var deleteDeferred = this.deleteSqlReplications(deletedReplications, db);
            deleteDeferred.done(() => {
                var saveDeferred = this.saveSqlReplications(onScreenReplications, db);
                saveDeferred.done(()=> {
                    this.updateLoadedSqlReplications();
                    // Resync Changes
                    viewModelBase.dirtyFlag().reset();
                });
            });
        }
    }

    private deleteSqlReplications(deletedReplications: Array<string>, db): JQueryDeferred<{}> {
        var deleteDeferred = $.Deferred();
        //delete from the server the deleted on screen sql replications
        if (deletedReplications.length > 0) {
            new deleteDocumentsCommand(deletedReplications, db)
                .execute()
                .done(() => {
                    deleteDeferred.resolve();
                });
        } else {
            deleteDeferred.resolve();
        }
        return deleteDeferred;
    }

    private saveSqlReplications(onScreenReplications, db): JQueryDeferred<{}>{
        var saveDeferred = $.Deferred();
        //save the new/updated sql replications
        if (onScreenReplications.length > 0) {
            new saveSqlReplicationsCommand(this.replications(), db)
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

    private updateLoadedSqlReplications() {
        this.loadedSqlReplications = [];
        var sqlReplications = this.replications();
        for (var i = 0; i < sqlReplications.length; i++) {
            this.loadedSqlReplications.push(sqlReplications[i].getId());
        }
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

    addNewSqlReplication() {
        this.isFirstload(false);
        this.replications.push(sqlReplication.empty());
    }

    removeSqlReplication(repl: sqlReplication) {
        this.replications.remove(repl);
    }

    itemNumber = function(index) {
        return index + 1;
    }
}

export = sqlReplications; 
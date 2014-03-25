import sqlReplication = require("models/sqlReplication");
import viewModelBase = require("viewmodels/viewModelBase");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import getSqlReplicationsCommand = require("commands/getSqlReplicationsCommand");
import saveSqlReplicationsCommand = require("commands/saveSqlReplicationsCommand");

class sqlReplications extends viewModelBase {

    replications = ko.observableArray<sqlReplication>();

    constructor() {
        super();

        aceEditorBindingHandler.install();
    }

    activate() {
        this.fetchSqlReplications();
    }

    attached() {
        $(".script-label").popover({
            html: true,
            trigger: 'hover',
            content: 'Replication scripts use JScript.',
        });
    }

    private fetchSqlReplications() {
        var db = this.activeDatabase();
        if (db) {
            new getSqlReplicationsCommand(db)
                .execute()
                .done(results => this.replications(results));
        }
    }

    saveChanges() {
        var db = this.activeDatabase();
        if (db) {
            this.replications().forEach(r => r.setIdFromName());
            new saveSqlReplicationsCommand(this.replications(), db)
                .execute()
                .done((result: bulkDocumentDto[]) => this.updateKeys(result));;
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
        this.replications.push(sqlReplication.empty());
    }

    removeSqlReplication(repl: sqlReplication) {
        this.replications.remove(repl);
    }
}

export = sqlReplications; 
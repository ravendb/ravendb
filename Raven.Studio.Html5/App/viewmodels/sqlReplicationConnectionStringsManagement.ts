import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/database");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import sqlReplicationConnections = require("models/sqlReplicationConnections");
import predefinedSqlConnection = require("models/predefinedSqlConnection");
import document = require("models/document");
import documentMetadata = require("models/documentMetadata");
import saveDocumentCommand = require("commands/saveDocumentCommand");
import appUrl = require('common/appUrl');

class sqlReplicationConnectionStringsManagement extends viewModelBase{
    
    htmlSelector ="#sqlReplicationConnectionsManagement";
    connections = ko.observable<sqlReplicationConnections>();
    isSaveEnabled = ko.observable<boolean>();
    
    constructor(private db:database) {
        super();
    }

    canActivate() {
        var def = $.Deferred();
        new getDocumentWithMetadataCommand("Raven/SqlReplication/Connections", this.activeDatabase())
            .execute()
            .done( (x:document) => {
                var dto:any = x.toDto(true);
                this.connections(new sqlReplicationConnections(dto));
                def.resolve({ can: true });
            })
            .fail((err) => {
                this.connections(sqlReplicationConnections.empty());
                def.resolve({ can: true });
            });

        return def;
    }

    activate() {
        super.activate("sqlReplicationConnections");
        viewModelBase.dirtyFlag = new ko.DirtyFlag([this.connections]);
        this.isSaveEnabled = ko.computed(() => {
            return viewModelBase.dirtyFlag().isDirty();
        });
    }

    save() {
        var newDoc = new document(this.connections().toDto());
        this.attachReservedMetaProperties("Raven/SqlReplication/Connections", newDoc.__metadata);

        var saveCommand = new saveDocumentCommand("Raven/SqlReplication/Connections", newDoc, this.activeDatabase());
        var saveTask = saveCommand.execute();
        saveTask.done((idAndEtag: { Key: string; ETag: string }) => {
            viewModelBase.dirtyFlag().reset(); 
        });
    }

    attachReservedMetaProperties(id: string, target: documentMetadata) {
        target.etag = '';
        target.ravenEntityName = !target.ravenEntityName ? document.getEntityNameFromId(id) : target.ravenEntityName;
        target.id = id;
    }

    getSqlReplicationConnectionsUrl() {
        return appUrl.forSqlReplicationConnections(this.activeDatabase());
    }


    addSqlReplicationConnection() {
        this.connections().predefinedConnections.push(predefinedSqlConnection.empty());
    }

    removeSqlReplicationConnection(connection) {
        this.connections().predefinedConnections.remove(connection);
    }

}

export =sqlReplicationConnectionStringsManagement;
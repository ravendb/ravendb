﻿import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/database");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import sqlReplicationConnections = require("models/sqlReplicationConnections");
import predefinedSqlConnection = require("models/predefinedSqlConnection");
import document = require("models/document");
import documentMetadata = require("models/documentMetadata");
import saveDocumentCommand = require("commands/saveDocumentCommand");
import appUrl = require('common/appUrl');
import editSqlReplication = require("viewmodels/editSqlReplication");
import testSqlConnectionCommand = require("commands/testSqlConnectionCommand");
import app = require("durandal/app");

class sqlReplicationConnectionStringsManagement extends viewModelBase{
    
    htmlSelector ="#sqlReplicationConnectionsManagement";
    connections = ko.observable<sqlReplicationConnections>();
    canActivateCalled = ko.observable<boolean>(false);
    isSaveEnabled :KnockoutComputed<boolean>;
    constructor() {
        super();
    }

    loadConnections():JQueryPromise<any> {
        return new getDocumentWithMetadataCommand("Raven/SqlReplication/Connections", this.activeDatabase())
            .execute()
            .done((x: document) => {
                var dto: any = x.toDto(true);
                this.connections(new sqlReplicationConnections(dto));
                if (this.connections().predefinedConnections().length > 0) {
                    this.connections().predefinedConnections().forEach(x=> this.subscribeToSqlReplicationConnectionName(x));
                }
            })
            .fail(() => {
                this.connections(sqlReplicationConnections.empty());
            });
    }

    canActivate() {
        var def = $.Deferred();
        this.loadConnections()
            .always(() => def.resolve({ can: true }));
        this.canActivateCalled(true);

        return def;
    }

    activate(args) {
        super.activate(args);
        this.dirtyFlag = new ko.DirtyFlag([this.connections]);
        this.isSaveEnabled = ko.computed(() => {
            return this.dirtyFlag().isDirty() === true && this.canActivateCalled() === true;
        });

        if (this.canActivateCalled() === false) {
            this.loadConnections().always(() => this.canActivateCalled(true));
        }
    }

    save() {
        var newDoc = new document(this.connections().toDto());
        this.attachReservedMetaProperties("Raven/SqlReplication/Connections", newDoc.__metadata);

        var saveCommand = new saveDocumentCommand("Raven/SqlReplication/Connections", newDoc, this.activeDatabase());
        var saveTask = saveCommand.execute();
        saveTask.done((idAndEtag: { Key: string; ETag: string }) => {
            this.dirtyFlag().reset(); 
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
        var newPredefinedConnection: predefinedSqlConnection;
        newPredefinedConnection = predefinedSqlConnection.empty();
        this.connections().predefinedConnections.push(newPredefinedConnection);
        this.subscribeToSqlReplicationConnectionName(newPredefinedConnection);
        newPredefinedConnection.name("New");
    }

    removeSqlReplicationConnection(connection) {
        this.connections().predefinedConnections.remove(connection);
    }

    subscribeToSqlReplicationConnectionName(con: predefinedSqlConnection) {
        con.name.subscribe((previousName: string) => {
                //Get the previous value of 'name' here before it's set to newValue
            var nameInputArray = $('input[name="name"]')
                .each((index, inputField: any) => {
                inputField.setCustomValidity("");
            });
            }, this, "beforeChange");
            con.name.subscribe((newName) => {
                var message = "";
                if (newName === "") {
                    message = "Please fill out this field.";
                }
                else if (this.isSqlPredefinedConnectionNameExists(newName)) {
                    message = "SQL Replication Connection name already exists.";
                }
                $('input[name="name"]')
                    .filter(function () { return this.value === newName; })
                    .each((index, element: any) => {
                        element.setCustomValidity(message);
                    });
            });
    }

    isSqlPredefinedConnectionNameExists(connectionName: string) :boolean {
        if (this.connections().predefinedConnections().count(x => x.name() == connectionName) >1) {
            return true;
        }
        return false;
    }

    providerChanged(obj, event) {
        if (event.originalEvent) {
            var curConnectionString = !!obj.connectionString() ? obj.connectionString().trim() : "";
            if (curConnectionString === "" ||
                editSqlReplication.sqlProvidersConnectionStrings.first(x => x.ConnectionString == curConnectionString)) {
                var matchingConnectionStringPair: { ProviderName: string; ConnectionString: string; } = editSqlReplication.sqlProvidersConnectionStrings.first(x => x.ProviderName == event.originalEvent.srcElement.selectedOptions[0].value);
                if (!!matchingConnectionStringPair) {
                    var matchingConnectionStringValue: string = matchingConnectionStringPair.ConnectionString;
                    obj.connectionString(
                        matchingConnectionStringValue
                    );
                }
            }
        }
    }

    testConnection(data: predefinedSqlConnection) {
        new testSqlConnectionCommand(this.activeDatabase(), data.factoryName(), data.connectionString())
            .execute()
            .done(() => {
                app.showMessage("Connection " + data.name() + " is valid", "SQL Connection test");
            })
            .fail((request, status, error) => {
                var errorText = !!request.responseJSON ? !!request.responseJSON.Exception ? request.responseJSON.Exception.Message : error : error;
                app.showMessage("Connection " + data.name() + " is not valid, error: " + errorText, "SQL Connection test");
            });
    }

}

export =sqlReplicationConnectionStringsManagement;
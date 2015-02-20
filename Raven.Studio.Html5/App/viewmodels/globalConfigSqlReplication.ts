import viewModelBase = require("viewmodels/viewModelBase");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import sqlReplicationConnections = require("models/sqlReplicationConnections");
import predefinedSqlConnection = require("models/predefinedSqlConnection");
import document = require("models/document");
import documentMetadata = require("models/documentMetadata");
import saveDocumentCommand = require("commands/saveDocumentCommand");
import appUrl = require("common/appUrl");
import messagePublisher = require("common/messagePublisher");
import deleteDocumentCommand = require("commands/deleteDocumentCommand");

class globalConfigSqlReplication extends viewModelBase{
    
    htmlSelector ="#sqlReplicationConnectionsManagement";
    connections = ko.observable<sqlReplicationConnections>();
    isSaveEnabled: KnockoutComputed<boolean>;
    
    activated = ko.observable<boolean>(false);

    constructor() {
        super();
    }

    loadConnections():JQueryPromise<any> {
        return new getDocumentWithMetadataCommand("Raven/Global/SqlReplication/Connections", appUrl.getSystemDatabase())
            .execute()
            .done((x: document) => {
                var dto: any = x.toDto(true);
                this.connections(new sqlReplicationConnections({ MergedDocument: dto }));
                if (this.connections().predefinedConnections().length > 0) {
                    this.connections().predefinedConnections().forEach(x=> this.subscribeToSqlReplicationConnectionName(x));
                }
                this.activated(true);
            })
            .fail(() => {
                this.connections(sqlReplicationConnections.empty());
            });
    }


    getActiveDatabase() {
        return this.getActiveDatabase();
    }

    canActivate() {
        var def = $.Deferred();
        this.loadConnections()
            .always(() => def.resolve({ can: true }));
        return def;
    }

    activate(args) {
        super.activate(args);
        this.dirtyFlag = new ko.DirtyFlag([this.connections]);
        this.isSaveEnabled = ko.computed(() => this.dirtyFlag().isDirty());
    }

    saveChanges() {
        this.syncChanges(false);
    }

    syncChanges(deleteConfig: boolean) {
        if (deleteConfig) {
            new deleteDocumentCommand("Raven/Global/SqlReplication/Connections", appUrl.getSystemDatabase())
                .execute()
                .done(() => messagePublisher.reportSuccess("Global Settings were successfully saved!"))
                .fail((response: JQueryXHR) => messagePublisher.reportError("Failed to save global settings!", response.responseText, response.statusText));
        } else {
            var newDoc = new document(this.connections().toDto());
            this.attachReservedMetaProperties("Raven/Global/SqlReplication/Connections", newDoc.__metadata);

            var saveCommand = new saveDocumentCommand("Raven/Global/SqlReplication/Connections", newDoc, appUrl.getSystemDatabase());
            var saveTask = saveCommand.execute();
            saveTask.done(() => this.dirtyFlag().reset());
        }
    }

    attachReservedMetaProperties(id: string, target: documentMetadata) {
        target.etag = "";
        target.ravenEntityName = !target.ravenEntityName ? document.getEntityNameFromId(id) : target.ravenEntityName;
        target.id = id;
    }

    getSqlReplicationConnectionsUrl() {
        return appUrl.forSqlReplicationConnections(appUrl.getSystemDatabase());
    }


    addSqlReplicationConnection() {
        var newPredefinedConnection = predefinedSqlConnection.empty();
        this.connections().predefinedConnections.splice(0, 0, newPredefinedConnection);
        this.subscribeToSqlReplicationConnectionName(newPredefinedConnection);
        newPredefinedConnection.name("New");
    }

    removeSqlReplicationConnection(connection) {
        this.connections().predefinedConnections.remove(connection);
    }

    subscribeToSqlReplicationConnectionName(con: predefinedSqlConnection) {
        con.name.subscribe((previousName: string) => {
                //Get the previous value of 'name' here before it's set to newValue
            var nameInputArray = $("input[name=\"name\"]")
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
            $("input[name=\"name\"]")
                .filter(function () { return this.value === newName; })
                .each((index, element: any) => {
                    element.setCustomValidity(message);
                });
        });
    }

    isSqlPredefinedConnectionNameExists(connectionName: string) :boolean {
        if (this.connections().predefinedConnections().count(x => x.name() === connectionName) >1) {
            return true;
        }
        return false;
    }

    providerChanged(obj, event) {
        if (event.originalEvent) {
            var curConnectionString = !!obj.connectionString() ? obj.connectionString().trim() : "";
            if (curConnectionString === "" ||
                sqlReplicationConnections.sqlProvidersConnectionStrings.first(x => x.ConnectionString == curConnectionString)) {
                var matchingConnectionStringPair: { ProviderName: string; ConnectionString: string; } = sqlReplicationConnections.sqlProvidersConnectionStrings.first(x => x.ProviderName == event.originalEvent.srcElement.selectedOptions[0].value);
                if (!!matchingConnectionStringPair) {
                    var matchingConnectionStringValue: string = matchingConnectionStringPair.ConnectionString;
                    obj.connectionString(
                        matchingConnectionStringValue
                    );
                }
            }
        }
    }

    activateConfig() {
        this.activated(true);
    }

    disactivateConfig() {
        this.confirmationMessage("Delete global configuration for sql replication?", "Are you sure?")
            .done(() => {
                this.connections().predefinedConnections.removeAll();
                this.activated(false);
                this.syncChanges(true);
            });
    }
}

export =globalConfigSqlReplication;
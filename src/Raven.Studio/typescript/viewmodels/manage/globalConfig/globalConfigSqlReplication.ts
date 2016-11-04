import viewModelBase = require("viewmodels/viewModelBase");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import sqlReplicationConnections = require("models/database/sqlReplication/sqlReplicationConnections");
import predefinedSqlConnection = require("models/database/sqlReplication/predefinedSqlConnection");
import document = require("models/database/documents/document");
import documentMetadata = require("models/database/documents/documentMetadata");
import saveDocumentCommand = require("commands/database/documents/saveDocumentCommand");
import appUrl = require("common/appUrl");
import messagePublisher = require("common/messagePublisher");
import deleteDocumentCommand = require("commands/database/documents/deleteDocumentCommand");
import globalConfig = require("viewmodels/manage/globalConfig/globalConfig");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");

class globalConfigSqlReplication extends viewModelBase {
    /* TODO
    developerLicense = globalConfig.developerLicense;
    canUseGlobalConfigurations = globalConfig.canUseGlobalConfigurations;
    htmlSelector ="#sqlReplicationConnectionsManagement";
    connections = ko.observable<sqlReplicationConnections>();
    isSaveEnabled: KnockoutComputed<boolean>;
    
    activated = ko.observable<boolean>(false);
    settingsAccess = new settingsAccessAuthorizer();

    loadConnections():JQueryPromise<any> {
        return new getDocumentWithMetadataCommand("Raven/Global/SqlReplication/Connections", null)
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

    canActivate() {
        var deferred = $.Deferred();

        if (settingsAccessAuthorizer.isForbidden()) {
            deferred.resolve({ can: true });
        } else {
            this.loadConnections()
                .always(() => deferred.resolve({ can: true }));
        }
       
        return deferred;
    }

    activate(args: any) {
        super.activate(args);
        this.dirtyFlag = new ko.DirtyFlag([this.connections]);
        this.isSaveEnabled = ko.computed(() => !settingsAccessAuthorizer.isReadOnly() && this.dirtyFlag().isDirty());
    }

    saveChanges() {
        this.syncChanges(false);
    }

    syncChanges(deleteConfig: boolean) {
        if (deleteConfig) {
            new deleteDocumentCommand("Raven/Global/SqlReplication/Connections", null)
                .execute()
                .done(() => messagePublisher.reportSuccess("Global Settings were successfully saved!"))
                .fail((response: JQueryXHR) => messagePublisher.reportError("Failed to save global settings!", response.responseText, response.statusText));
        } else {
            var newDoc = new document(this.connections().toDto());
            this.attachReservedMetaProperties("Raven/Global/SqlReplication/Connections", newDoc.__metadata);

            var saveCommand = new saveDocumentCommand("Raven/Global/SqlReplication/Connections", newDoc, null);
            var saveTask = saveCommand.execute();
            saveTask.done(() => this.dirtyFlag().reset());
        }
    }

    attachReservedMetaProperties(id: string, target: documentMetadata) {
        target.etag = 0;
        target.ravenEntityName = target.ravenEntityName || document.getEntityNameFromId(id);
        target.id = id;
    }

    getSqlReplicationConnectionsUrl() {
        return appUrl.forSqlReplicationConnections(null);
    }

    addSqlReplicationConnection() {
        var newPredefinedConnection = predefinedSqlConnection.empty();
        this.connections().predefinedConnections.splice(0, 0, newPredefinedConnection);
        this.subscribeToSqlReplicationConnectionName(newPredefinedConnection);
        newPredefinedConnection.name("New");
    }

    removeSqlReplicationConnection(connection: predefinedSqlConnection) {
        this.connections().predefinedConnections.remove(connection);
    }

    subscribeToSqlReplicationConnectionName(con: predefinedSqlConnection) {
        con.name.subscribe(() => {
             //Get the previous value of 'name' here before it's set to newValue
             $("input[name=\"name\"]")
                .each((index: number, inputField: any) => {
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
                .each((index: number, element: any) => {
                    element.setCustomValidity(message);
                });
        });
    }

    isSqlPredefinedConnectionNameExists(connectionName: string) :boolean {
        return this.connections().predefinedConnections().count(x => x.name() === connectionName) > 1;
    }

    providerChanged(obj: predefinedSqlConnection, event: JQueryEventObject) {
        if (event.originalEvent) {
            var curConnectionString = !!obj.connectionString() ? obj.connectionString().trim() : "";
            if (curConnectionString === "" ||
                sqlReplicationConnections.sqlProvidersConnectionStrings.first(x => x.ConnectionString === curConnectionString)) {
                var matchingConnectionStringPair: { ProviderName: string; ConnectionString: string; } =
                    sqlReplicationConnections.sqlProvidersConnectionStrings.first(x => x.ProviderName == (<any>event.originalEvent.srcElement).selectedOptions[0].value);
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
    }*/
}

export = globalConfigSqlReplication;

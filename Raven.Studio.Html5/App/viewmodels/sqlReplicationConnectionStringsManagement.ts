import viewModelBase = require("viewmodels/viewModelBase");
import sqlReplicationConnections = require("models/sqlReplicationConnections");
import predefinedSqlConnection = require("models/predefinedSqlConnection");
import document = require("models/document");
import documentMetadata = require("models/documentMetadata");
import saveDocumentCommand = require("commands/saveDocumentCommand");
import appUrl = require("common/appUrl");
import editSqlReplication = require("viewmodels/editSqlReplication");
import getEffectiveSqlReplicationConnectionStringsCommand = require("commands/getEffectiveSqlReplicationConnectionStringsCommand");
import messagePublisher = require("common/messagePublisher");
import deleteDocumentCommand = require("commands/deleteDocumentCommand");

class sqlReplicationConnectionStringsManagement extends viewModelBase{
    
    htmlSelector ="#sqlReplicationConnectionsManagement";
    connections = ko.observable<sqlReplicationConnections>();
    isSaveEnabled: KnockoutComputed<boolean>;
    
    usingGlobal = ko.observable<boolean>(false);
    hasGlobalValues = ko.observable<boolean>(false);

    constructor() {
        super();
    }

    loadConnections():JQueryPromise<any> {
        return new getEffectiveSqlReplicationConnectionStringsCommand(this.activeDatabase())
            .execute()
            .done((repSetup: configurationDocumentDto<sqlReplicationConnectionsDto>) => {
                this.usingGlobal(repSetup.GlobalExists && !repSetup.LocalExists);
                this.hasGlobalValues(repSetup.GlobalExists);

                this.connections(new sqlReplicationConnections(repSetup));
                if (this.connections().predefinedConnections().length > 0) {
                    this.connections().predefinedConnections().forEach(x=> this.subscribeToSqlReplicationConnectionName(x));
                }
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

    save() {
        if (this.usingGlobal()) {
            new deleteDocumentCommand("Raven/SqlReplication/Connections", this.activeDatabase())
                .execute()
                .done(() => {
                    messagePublisher.reportSuccess("Settings were successfully saved!");
                    this.dirtyFlag().reset();
                })
                .fail((response: JQueryXHR) => messagePublisher.reportError("Failed to save settings!", response.responseText, response.statusText));
        } else {
            var newDoc = new document(this.connections().toDto());
            this.attachReservedMetaProperties("Raven/SqlReplication/Connections", newDoc.__metadata);

            var saveCommand = new saveDocumentCommand("Raven/SqlReplication/Connections", newDoc, this.activeDatabase());
            var saveTask = saveCommand.execute();
            saveTask.done(() => this.dirtyFlag().reset());
        }
       
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

    override(value: boolean, config: predefinedSqlConnection) {
        config.hasLocal(value);
        if (!config.hasLocal()) {
            config.copyFromGlobal();
        }
    }

    useLocal() {
        this.usingGlobal(false);
    }

    useGlobal() {
        this.usingGlobal(true);
        this.connections().copyFromParent();
    }
}

export =sqlReplicationConnectionStringsManagement;
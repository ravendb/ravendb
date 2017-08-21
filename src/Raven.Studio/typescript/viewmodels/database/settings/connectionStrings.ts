import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import deleteConnectionStringConfirm = require("viewmodels/resources/deleteConnectionStringConfirm");
import connectionStringRavenEtlModel = require("models/database/settings/connectionStringRavenEtlModel");
import connectionStringSqlEtlModel = require("models/database/settings/connectionStringSqlEtlModel");
import testClusterNodeConnectionCommand = require("commands/database/cluster/testClusterNodeConnectionCommand");
import saveConnectionStringCommand = require("commands/database/settings/saveConnectionStringCommand");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import getConnectionStringInfoCommand = require("commands/database/settings/getConnectionStringInfoCommand");
import deleteConnectionStringCommand = require("commands/database/settings/deleteConnectionStringCommand");
import eventsCollector = require("common/eventsCollector");

class connectionStrings extends viewModelBase {

    ravenEtlConnectionStringsNames = ko.observableArray<string>([]);
    sqlEtlConnectionStringsNames = ko.observableArray<string>([]);

    editedRavenEtlConnectionString = ko.observable<connectionStringRavenEtlModel>(null);
    editedSqlEtlConnectionString = ko.observable<connectionStringSqlEtlModel>(null);

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    spinners = { test: ko.observable<boolean>(false) };

    constructor() {
        super();

        this.bindToCurrentInstance("onEditSqlEtl", "onEditRavenEtl", "confirmDeleteRavenEtl", "confirmDeleteSqlEtl");
        this.dirtyFlag = new ko.DirtyFlag([this.editedRavenEtlConnectionString, this.editedSqlEtlConnectionString], false); 
    }

    activate(args: any) {
        super.activate(args);
        return this.getAllConnectionStrings();
    }

    private clearTestResult() {
        this.testConnectionResult(null);
    }

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.activeDatabase())
            .execute()
            .done((result: Raven.Client.ServerWide.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                this.ravenEtlConnectionStringsNames(Object.keys(result.RavenConnectionStrings));
                this.sqlEtlConnectionStringsNames(Object.keys(result.SqlConnectionStrings));

                this.ravenEtlConnectionStringsNames(_.sortBy(this.ravenEtlConnectionStringsNames(), x => x.toUpperCase()));
                this.sqlEtlConnectionStringsNames(_.sortBy(this.sqlEtlConnectionStringsNames(), x => x.toUpperCase()));
            });
    }

    confirmDeleteRavenEtl(connectionStringName: string) {
        this.confirmDelete(connectionStringName, "Raven");
    }
   
    confirmDeleteSqlEtl(connectionStringName: string) {
        this.confirmDelete(connectionStringName, "Sql");
    }

    private confirmDelete(connectionStringName: string, type: Raven.Client.ServerWide.ConnectionStringType) {
        const confirmDeleteViewModel = new deleteConnectionStringConfirm(type, connectionStringName);
        app.showBootstrapDialog(confirmDeleteViewModel);
        confirmDeleteViewModel.result.done(result => {
            if (result.can) {
                this.deleteConnectionSring(type, connectionStringName); 
            }
        });
    }

    private deleteConnectionSring(connectionStringType: Raven.Client.ServerWide.ConnectionStringType, connectionStringName: string) {
        new deleteConnectionStringCommand(this.activeDatabase(), connectionStringType, connectionStringName)
            .execute()
            .done(() => {
                this.getAllConnectionStrings();
                this.onCloseEdit();
            });
    }
    
    onAddRavenEtl() {
        this.editedRavenEtlConnectionString(connectionStringRavenEtlModel.empty());
        this.editedRavenEtlConnectionString().url.subscribe(() => this.clearTestResult());

        this.editedSqlEtlConnectionString(null);
    }

    onAddSqlEtl() {
        this.editedSqlEtlConnectionString(connectionStringSqlEtlModel.empty());
        this.editedSqlEtlConnectionString().connectionString.subscribe(() => this.clearTestResult());

        this.editedRavenEtlConnectionString(null);
    }

    onEditRavenEtl(connectionStringName: string) {
        return getConnectionStringInfoCommand.forRavenEtl(this.activeDatabase(), connectionStringName)
            .execute()
            .done((result: Raven.Client.ServerWide.ETL.RavenConnectionString) => {
                this.editedRavenEtlConnectionString(new connectionStringRavenEtlModel(result));
                this.editedRavenEtlConnectionString().url.subscribe(() => this.clearTestResult());
                this.editedSqlEtlConnectionString(null);
            });
    }

    onEditSqlEtl(connectionStringName: string) {
        return getConnectionStringInfoCommand.forSqlEtl(this.activeDatabase(), connectionStringName)
            .execute()
            .done((result: Raven.Client.ServerWide.ETL.SqlConnectionString) => {
                this.editedSqlEtlConnectionString(new connectionStringSqlEtlModel(result));
                this.editedSqlEtlConnectionString().connectionString.subscribe(() => this.clearTestResult());
                this.editedRavenEtlConnectionString(null);
            });
    }
    
    onTestConnection() {
        if (this.editedRavenEtlConnectionString()) {
            if (this.isValid(this.editedRavenEtlConnectionString().url)) {
                eventsCollector.default.reportEvent("ravenDB-ETL-connection-string", "test-connection");

                this.spinners.test(true);

                new testClusterNodeConnectionCommand(this.editedRavenEtlConnectionString().url())
                    .execute()
                    .done(result => this.testConnectionResult(result))
                    .always(() => this.spinners.test(false));
            } 
        }

        // TODO: test connection for sql ... will be done with issue 7128
    }
    
    onCloseEdit() {
        this.editedRavenEtlConnectionString(null);
        this.editedSqlEtlConnectionString(null);
    }

    onOk() {
        let type: Raven.Client.ServerWide.ConnectionStringType;

        // 1. Validate model
        if (this.editedRavenEtlConnectionString()) {
            type = "Raven";
            if (!this.isValid(this.editedRavenEtlConnectionString().validationGroup)) { 
                return;
            }
        } else {
            type = "Sql";
            if (!this.isValid(this.editedSqlEtlConnectionString().validationGroup)) {
                return;
            }
        }

        // 2. Create/add the new connection string
        new saveConnectionStringCommand(this.activeDatabase(), type, this.editedRavenEtlConnectionString(), this.editedSqlEtlConnectionString())
            .execute()
            .done(() => {
                // 3. Refresh list view....
                this.getAllConnectionStrings();

                this.editedRavenEtlConnectionString(null);
                this.editedSqlEtlConnectionString(null);

                this.dirtyFlag().reset();
            });
    }
}

export = connectionStrings
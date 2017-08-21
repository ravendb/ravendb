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
import generalUtils = require("common/generalUtils");

class connectionStrings extends viewModelBase {

    ravenEtlConnectionStringsNames = ko.observableArray<string>([]);
    sqlEtlConnectionStringsNames = ko.observableArray<string>([]);

    editedRavenEtlConnectionString = ko.observable<connectionStringRavenEtlModel>(null);
    editedSqlEtlConnectionString = ko.observable<connectionStringSqlEtlModel>(null);

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    spinners = { test: ko.observable<boolean>(false) };
    fullErrorDetailsVisible = ko.observable<boolean>(false);

    shortErrorText: KnockoutObservable<string>;

    constructor() {
        super();

        this.initObservables();
        
        this.bindToCurrentInstance("onEditSqlEtl", "onEditRavenEtl", "confirmDeleteRavenEtl", "confirmDeleteSqlEtl");
        this.dirtyFlag = new ko.DirtyFlag([this.editedRavenEtlConnectionString, this.editedSqlEtlConnectionString], false); 
    }
    
    private initObservables() {
        this.shortErrorText = ko.pureComputed(() => {
            const result = this.testConnectionResult();
            if (!result || result.Success) {
                return "";
            }
            return generalUtils.trimMessage(result.Error);
        });
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
        this.clearTestResult();
    }

    onAddSqlEtl() {
        this.editedSqlEtlConnectionString(connectionStringSqlEtlModel.empty());
        this.editedSqlEtlConnectionString().connectionString.subscribe(() => this.clearTestResult());

        this.editedRavenEtlConnectionString(null);
        this.clearTestResult();
    }

    onEditRavenEtl(connectionStringName: string) {
        this.clearTestResult();
        
        return getConnectionStringInfoCommand.forRavenEtl(this.activeDatabase(), connectionStringName)
            .execute()
            .done((result: Raven.Client.ServerWide.ETL.RavenConnectionString) => {
                this.editedRavenEtlConnectionString(new connectionStringRavenEtlModel(result));
                this.editedRavenEtlConnectionString().url.subscribe(() => this.clearTestResult());
                this.editedSqlEtlConnectionString(null);
            });
    }

    onEditSqlEtl(connectionStringName: string) {
        this.clearTestResult();
        
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
            if (this.isValid(this.editedRavenEtlConnectionString().testConnectionValidationGroup)) {
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
        let model: connectionStringRavenEtlModel | connectionStringSqlEtlModel;

        // 1. Validate model
        if (this.editedRavenEtlConnectionString()) {
            if (!this.isValid(this.editedRavenEtlConnectionString().validationGroup)) { 
                return;
            }
            model = this.editedRavenEtlConnectionString();
        } else {
            if (!this.isValid(this.editedSqlEtlConnectionString().validationGroup)) {
                return;
            }
            model = this.editedSqlEtlConnectionString();
        }

        // 2. Create/add the new connection string
        new saveConnectionStringCommand(this.activeDatabase(), model)
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

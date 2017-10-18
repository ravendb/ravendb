import viewModelBase = require("viewmodels/viewModelBase");
import connectionStringRavenEtlModel = require("models/database/settings/connectionStringRavenEtlModel");
import connectionStringSqlEtlModel = require("models/database/settings/connectionStringSqlEtlModel");
import testClusterNodeConnectionCommand = require("commands/database/cluster/testClusterNodeConnectionCommand");
import saveConnectionStringCommand = require("commands/database/settings/saveConnectionStringCommand");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import getConnectionStringInfoCommand = require("commands/database/settings/getConnectionStringInfoCommand");
import deleteConnectionStringCommand = require("commands/database/settings/deleteConnectionStringCommand");
import testSqlConnectionStringCommand = require("commands/database/cluster/testSqlConnectionStringCommand");
import ongoingTasksCommand = require("commands/database/tasks/getOngoingTasksCommand");
import eventsCollector = require("common/eventsCollector");
import generalUtils = require("common/generalUtils");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import temp = require("rbush");

class connectionStrings extends viewModelBase {

    ravenEtlConnectionStringsNames = ko.observableArray<string>([]);
    sqlEtlConnectionStringsNames = ko.observableArray<string>([]);

    tasksStringsInfo: dictionary<string> = {};
    tasksIdsInfo: dictionary<number> = {};
    
    editedRavenEtlConnectionString = ko.observable<connectionStringRavenEtlModel>(null);
    editedSqlEtlConnectionString = ko.observable<connectionStringSqlEtlModel>(null);

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    spinners = { test: ko.observable<boolean>(false) };
    fullErrorDetailsVisible = ko.observable<boolean>(false);

    shortErrorText: KnockoutObservable<string>;

    constructor() {
        super();

        this.initObservables();
        this.bindToCurrentInstance("onEditSqlEtl", "onEditRavenEtl", "confirmDelete", "isConnectionStringInUse");
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
        return $.when<any>(this.getAllConnectionStrings(), this.fetchOngoingTasks());      
    }

    compositionComplete() {
        super.compositionComplete();
        this.setupDisableReasons();    
    }
    
    private clearTestResult() {
        this.testConnectionResult(null);
    }

    private fetchOngoingTasks(): JQueryPromise<Raven.Server.Web.System.OngoingTasksResult> {
        const db = this.activeDatabase();
        return new ongoingTasksCommand(db)
            .execute()
            .done((info) => {  
                this.processData(info);
            });
    }  
    
    private processData(result: Raven.Server.Web.System.OngoingTasksResult) {
        const tasksThatUseConnectionStrings = result.OngoingTasksList.filter((task) => task.TaskType === 'RavenEtl' || task.TaskType === 'SqlEtl');
        // todo: add external replication when issue 8667 is done 
        
        for (let i = 0; i < tasksThatUseConnectionStrings.length; i++) {
            const task = tasksThatUseConnectionStrings[i];
            
            switch (task.TaskType) {
                case 'RavenEtl':
                    this.tasksStringsInfo[task.TaskName] = (task as Raven.Client.ServerWide.Operations.OngoingTaskRavenEtlListView).ConnectionStringName;
                    break;
                case 'SqlEtl':
                    this.tasksStringsInfo[task.TaskName] = (task as Raven.Client.ServerWide.Operations.OngoingTaskSqlEtlListView).ConnectionStringName;
                    break;
            }
         
            this.tasksIdsInfo[task.TaskName] = task.TaskId;            
        }    
    }   

    isConnectionStringInUse(connectionStringName: string) :boolean { 
        for (let key in this.tasksStringsInfo) {            
             if (this.tasksStringsInfo[key] === connectionStringName) {
                 return true;                 
             }            
        } 
        
        return false;
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

    confirmDelete(connectionStringName: string, type: Raven.Client.ServerWide.ConnectionStringType) {
        const typeString = type === 'Raven' ? 'RavenDB' : 'SQL';
        this.confirmationMessage("Are you sure?", `Do you want to delete ${typeString} ETL connection string:  ${connectionStringName}`, ["Cancel", "Delete"])
            .done(result => {
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
                this.editedRavenEtlConnectionString(new connectionStringRavenEtlModel(result, false, this.getTasksThatUseThisString(connectionStringName)));
                this.editedRavenEtlConnectionString().url.subscribe(() => this.clearTestResult());
                this.editedSqlEtlConnectionString(null);
            });
    }

    onEditSqlEtl(connectionStringName: string) {
        this.clearTestResult();
        
        return getConnectionStringInfoCommand.forSqlEtl(this.activeDatabase(), connectionStringName)
            .execute()
            .done((result: Raven.Client.ServerWide.ETL.SqlConnectionString) => {
                this.editedSqlEtlConnectionString(new connectionStringSqlEtlModel(result, false, this.getTasksThatUseThisString(connectionStringName)));
                this.editedSqlEtlConnectionString().connectionString.subscribe(() => this.clearTestResult());
                this.editedRavenEtlConnectionString(null);
            });
    }
    
    private getTasksThatUseThisString(connectionStringName: string) : string[]{
        let result: string[] = [];
        
        for (let key in this.tasksStringsInfo) {            
            if (this.tasksStringsInfo[key] === connectionStringName) {
               result.push(key);
            }                     
        }
        
        return _.sortBy(result, x => x.toUpperCase())
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
        
        const sqlConnectionString = this.editedSqlEtlConnectionString();
        
        if (sqlConnectionString) {
            if (this.isValid(sqlConnectionString.testConnectionValidationGroup)) {
                eventsCollector.default.reportEvent("ravenDB-SQL-connection-string", "test-connection");

                this.spinners.test(true);

                new testSqlConnectionStringCommand(this.activeDatabase(), sqlConnectionString.connectionString())
                    .execute()
                    .done(result => this.testConnectionResult(result))
                    .always(() => this.spinners.test(false));
            }
        }
    }
    
    onCloseEdit() {
        this.editedRavenEtlConnectionString(null);
        this.editedSqlEtlConnectionString(null);
    }

    onSave() {
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

    navigateToTask(taskName: string, type: Raven.Client.ServerWide.ConnectionStringType) {     
        const taskId = this.tasksIdsInfo[taskName];
        const urls = appUrl.forCurrentDatabase();
     
        let destination = (type == 'Sql') ? urls.editSqlEtl(taskId) : urls.editRavenEtl(taskId);
        // todo: add external replication when issue 8667 is done 
        
        router.navigate(destination());          
    }
}

export = connectionStrings

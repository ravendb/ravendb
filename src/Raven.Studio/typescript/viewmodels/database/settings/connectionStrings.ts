import viewModelBase = require("viewmodels/viewModelBase");
import connectionStringRavenEtlModel = require("models/database/settings/connectionStringRavenEtlModel");
import connectionStringSqlEtlModel = require("models/database/settings/connectionStringSqlEtlModel");
import saveConnectionStringCommand = require("commands/database/settings/saveConnectionStringCommand");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import getConnectionStringInfoCommand = require("commands/database/settings/getConnectionStringInfoCommand");
import deleteConnectionStringCommand = require("commands/database/settings/deleteConnectionStringCommand");
import ongoingTasksCommand = require("commands/database/tasks/getOngoingTasksCommand");
import eventsCollector = require("common/eventsCollector");
import generalUtils = require("common/generalUtils");
import appUrl = require("common/appUrl");

class connectionStrings extends viewModelBase {

    ravenEtlConnectionStringsNames = ko.observableArray<string>([]);
    sqlEtlConnectionStringsNames = ko.observableArray<string>([]);

    // Mapping from { connection string } to { taskId, taskName, taskType }
    connectionStringsTasksInfo: dictionary<Array<{ TaskId: number, TaskName: string, TaskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType }>> = {}; 
    
    editedRavenEtlConnectionString = ko.observable<connectionStringRavenEtlModel>(null);
    editedSqlEtlConnectionString = ko.observable<connectionStringSqlEtlModel>(null);

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    testConnectionHttpSuccess: KnockoutComputed<boolean>;
    spinners = { 
        test: ko.observable<boolean>(false) 
    };
    fullErrorDetailsVisible = ko.observable<boolean>(false);

    shortErrorText: KnockoutObservable<string>;

    constructor() {
        super();

        this.initObservables();
        this.bindToCurrentInstance("onEditSqlEtl", "onEditRavenEtl", "confirmDelete", "isConnectionStringInUse", "onTestConnectionRaven");
        const currenlyEditedObjectIsDirty = ko.pureComputed(() => {
            const ravenEtl = this.editedRavenEtlConnectionString();
            if (ravenEtl) {
                return ravenEtl.dirtyFlag().isDirty();
            }
            
            const sqlEtl = this.editedSqlEtlConnectionString();
            if (sqlEtl) {
                return sqlEtl.dirtyFlag().isDirty();
            }
            
            return false;
        });
        this.dirtyFlag = new ko.DirtyFlag([currenlyEditedObjectIsDirty], false); 
    }
    
    private initObservables() {
        this.shortErrorText = ko.pureComputed(() => {
            const result = this.testConnectionResult();
            if (!result || result.Success) {
                return "";
            }
            return generalUtils.trimMessage(result.Error);
        });
        
        this.testConnectionHttpSuccess = ko.pureComputed(() => {
            const testResult = this.testConnectionResult();
            
            if (!testResult) {
                return false;
            }
            
            return testResult.HTTPSuccess || false;
        })
    }

    activate(args: any) {
        super.activate(args);        
        
        return $.when<any>(this.getAllConnectionStrings(), this.fetchOngoingTasks())
                .done(()=>{                    
                    if (args.name) {
                        if (args.type === 'sql') {                           
                            this.onEditSqlEtl(args.name);
                        } else {
                            this.onEditRavenEtl(args.name);
                        }
                    }
                });      
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
        const tasksThatUseConnectionStrings = result.OngoingTasksList.filter((task) => task.TaskType === 'RavenEtl' || 
                                                                              task.TaskType === 'SqlEtl'            || 
                                                                              task.TaskType === 'Replication');       
        for (let i = 0; i < tasksThatUseConnectionStrings.length; i++) {
            const task = tasksThatUseConnectionStrings[i];
            
            let taskData = { TaskId: task.TaskId,
                TaskName: task.TaskName,
                TaskType: task.TaskType };
            let stringName: string;
            
            switch (task.TaskType) {                
                case 'RavenEtl':                   
                    stringName = (task as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlListView).ConnectionStringName;   
                    break;
                case 'SqlEtl':
                    stringName = (task as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlListView).ConnectionStringName;
                    break;
                case 'Replication':
                    stringName = (task as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication).ConnectionStringName;
                    break;
            }

            if (this.connectionStringsTasksInfo[stringName]) {
                this.connectionStringsTasksInfo[stringName].push(taskData);
            } else {
                this.connectionStringsTasksInfo[stringName] = [taskData];
            }        
        }    
    }   

    isConnectionStringInUse(connectionStringName: string, task: string): boolean {
        return _.includes(Object.keys(this.connectionStringsTasksInfo), connectionStringName)
            && !!this.connectionStringsTasksInfo[connectionStringName].find(x => x.TaskType === task);
    }
    
    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.activeDatabase())
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                this.ravenEtlConnectionStringsNames(Object.keys(result.RavenConnectionStrings));
                this.sqlEtlConnectionStringsNames(Object.keys(result.SqlConnectionStrings));

                this.ravenEtlConnectionStringsNames(_.sortBy(this.ravenEtlConnectionStringsNames(), x => x.toUpperCase()));
                this.sqlEtlConnectionStringsNames(_.sortBy(this.sqlEtlConnectionStringsNames(), x => x.toUpperCase()));
            });
    }

    confirmDelete(connectionStringName: string, connectionStringtype: Raven.Client.Documents.Operations.ConnectionStrings.ConnectionStringType) {
        const stringType = connectionStringtype === 'Raven' ? 'RavenDB' : 'SQL';
        this.confirmationMessage("Are you sure?", `Do you want to delete ${stringType} ETL connection string:  ${generalUtils.escapeHtml(connectionStringName)}`, {
            buttons: ["Cancel", "Delete"],
            html: true
        })
            .done(result => {
                if (result.can) {
                    this.deleteConnectionSring(connectionStringtype, connectionStringName);
                }
        });
    }

    private deleteConnectionSring(connectionStringType: Raven.Client.Documents.Operations.ConnectionStrings.ConnectionStringType, connectionStringName: string) {
        new deleteConnectionStringCommand(this.activeDatabase(), connectionStringType, connectionStringName)
            .execute()
            .done(() => {
                this.getAllConnectionStrings();
                this.onCloseEdit();
            });
    }
    
    onAddRavenEtl() {
        eventsCollector.default.reportEvent("connection-strings", "add-raven-etl");
        this.editedRavenEtlConnectionString(connectionStringRavenEtlModel.empty());
        this.editedRavenEtlConnectionString().topologyDiscoveryUrls.subscribe(() => this.clearTestResult());
        this.editedRavenEtlConnectionString().inputUrl().discoveryUrlName.subscribe(() => this.testConnectionResult(null));

        this.editedSqlEtlConnectionString(null);
        this.clearTestResult();
    }

    onAddSqlEtl() {
        eventsCollector.default.reportEvent("connection-strings", "add-sql-etl");
        this.editedSqlEtlConnectionString(connectionStringSqlEtlModel.empty());
        this.editedSqlEtlConnectionString().connectionString.subscribe(() => this.clearTestResult());

        this.editedRavenEtlConnectionString(null);
        this.clearTestResult();
    }

    onEditRavenEtl(connectionStringName: string) {
        this.clearTestResult();
        
        return getConnectionStringInfoCommand.forRavenEtl(this.activeDatabase(), connectionStringName)
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                this.editedRavenEtlConnectionString(new connectionStringRavenEtlModel(result.RavenConnectionStrings[connectionStringName], false, this.getTasksThatUseThisString(connectionStringName, 'RavenEtl')));
                this.editedRavenEtlConnectionString().topologyDiscoveryUrls.subscribe(() => this.clearTestResult());
                this.editedRavenEtlConnectionString().inputUrl().discoveryUrlName.subscribe(() => this.testConnectionResult(null));
                this.editedSqlEtlConnectionString(null);
            });
    }

    onEditSqlEtl(connectionStringName: string) {
        this.clearTestResult();
        
        return getConnectionStringInfoCommand.forSqlEtl(this.activeDatabase(), connectionStringName)
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                this.editedSqlEtlConnectionString(new connectionStringSqlEtlModel(result.SqlConnectionStrings[connectionStringName], false, this.getTasksThatUseThisString(connectionStringName, 'SqlEtl')));
                this.editedSqlEtlConnectionString().connectionString.subscribe(() => this.clearTestResult());
                this.editedRavenEtlConnectionString(null);
            });
    }
    
    private getTasksThatUseThisString(connectionStringName: string, taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType): { taskName: string; taskId: number }[] {
        if (!this.connectionStringsTasksInfo[connectionStringName]) {
            return [];
        } else {
            const tasksData = this.connectionStringsTasksInfo[connectionStringName].filter(x => x.TaskType === taskType);

            return tasksData ? _.sortBy(tasksData.map((task) => { return { taskName: task.TaskName, taskId: task.TaskId }; }),
                x => x.taskName.toUpperCase()) : [];  
        }
    }

    onTestConnectionSql() {
        this.testConnectionResult(null);
        const sqlConnectionString = this.editedSqlEtlConnectionString();

        if (sqlConnectionString) {
            if (this.isValid(sqlConnectionString.testConnectionValidationGroup)) {
                eventsCollector.default.reportEvent("ravenDB-SQL-connection-string", "test-connection");

                this.spinners.test(true);
                sqlConnectionString.testConnection(this.activeDatabase())
                    .done((testResult) => this.testConnectionResult(testResult))
                    .always(() => {
                        this.spinners.test(false);
                    });           
            }
        }
    }  
    
    onTestConnectionRaven(urlToTest: string) {
        this.testConnectionResult(null);
        const ravenConnectionString = this.editedRavenEtlConnectionString();
        eventsCollector.default.reportEvent("ravenDB-ETL-connection-string", "test-connection");
        
        this.spinners.test(true);
        ravenConnectionString.selectedUrlToTest(urlToTest);

        ravenConnectionString.testConnection(urlToTest)
            .done((testResult) => this.testConnectionResult(testResult))
            .always(() => { 
                this.spinners.test(false); 
                ravenConnectionString.selectedUrlToTest(null); 
            });
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
        // TODO: change to model.testConnection() instead of calling the command directly when issue 8825 is done
        
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

    taskEditLink(taskId: number, connectionStringName: string) : string {        
        const task = _.find(this.connectionStringsTasksInfo[connectionStringName], task => task.TaskId === taskId);
        const urls = appUrl.forCurrentDatabase();

        switch (task.TaskType) {
            case 'SqlEtl':
                return urls.editSqlEtl(task.TaskId)();
            case 'RavenEtl': 
                return urls.editRavenEtl(task.TaskId)();            
            case 'Replication':
               return urls.editExternalReplication(task.TaskId)();
        }
    }
}

export = connectionStrings

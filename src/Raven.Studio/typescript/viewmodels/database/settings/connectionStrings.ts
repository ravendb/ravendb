import viewModelBase = require("viewmodels/viewModelBase");
import connectionStringRavenEtlModel = require("models/database/settings/connectionStringRavenEtlModel");
import connectionStringSqlEtlModel = require("models/database/settings/connectionStringSqlEtlModel");
import connectionStringOlapEtlModel = require("models/database/settings/connectionStringOlapEtlModel");
import saveConnectionStringCommand = require("commands/database/settings/saveConnectionStringCommand");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import getConnectionStringInfoCommand = require("commands/database/settings/getConnectionStringInfoCommand");
import deleteConnectionStringCommand = require("commands/database/settings/deleteConnectionStringCommand");
import ongoingTasksCommand = require("commands/database/tasks/getOngoingTasksCommand");
import discoveryUrl = require("models/database/settings/discoveryUrl");
import eventsCollector = require("common/eventsCollector");
import generalUtils = require("common/generalUtils");
import appUrl = require("common/appUrl");
import getPeriodicBackupConfigCommand = require("commands/database/tasks/getPeriodicBackupConfigCommand");
import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");
import testPeriodicBackupCredentialsCommand = require("commands/serverWide/testPeriodicBackupCredentialsCommand");

class connectionStrings extends viewModelBase {

    ravenEtlConnectionStringsNames = ko.observableArray<string>([]);
    sqlEtlConnectionStringsNames = ko.observableArray<string>([]);
    olapEtlConnectionStringsNames = ko.observableArray<string>([]);

    // Mapping from { connection string } to { taskId, taskName, taskType }
    connectionStringsTasksInfo: dictionary<Array<{ TaskId: number, TaskName: string, TaskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType }>> = {};
    
    editedRavenEtlConnectionString = ko.observable<connectionStringRavenEtlModel>(null);
    editedSqlEtlConnectionString = ko.observable<connectionStringSqlEtlModel>(null);
    editedOlapEtlConnectionString = ko.observable<connectionStringOlapEtlModel>(null);

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    testConnectionHttpSuccess: KnockoutComputed<boolean>;
    
    spinners = { 
        test: ko.observable<boolean>(false)
    };
    fullErrorDetailsVisible = ko.observable<boolean>(false);    


    shortErrorText: KnockoutObservable<string>;

    serverConfiguration = ko.observable<periodicBackupServerLimitsResponse>(); // needed for olap local destination

    constructor() {
        super();
        this.bindToCurrentInstance("onEditSqlEtl", "onEditRavenEtl", "onEditOlapEtl", "confirmDelete",
                                   "isConnectionStringInUse", "onTestConnectionRaven", "testCredentials");
        this.initObservables();
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
        });

        const currentlyEditedObjectIsDirty = ko.pureComputed(() => {
            const ravenEtl = this.editedRavenEtlConnectionString();
            if (ravenEtl) {
                return ravenEtl.dirtyFlag().isDirty();
            }

            const sqlEtl = this.editedSqlEtlConnectionString();
            if (sqlEtl) {
                return sqlEtl.dirtyFlag().isDirty();
            }

            const olapEtl = this.editedOlapEtlConnectionString();
            if (olapEtl) {
                return olapEtl.dirtyFlag().isDirty();
            }

            return false;
        });

        this.dirtyFlag = new ko.DirtyFlag([currentlyEditedObjectIsDirty], false);
    }

    activate(args: any) {
        super.activate(args);
        
        return $.when<any>(this.getAllConnectionStrings(), this.fetchOngoingTasks(), this.loadServerSideConfiguration())
                .done(() => {
                    if (args.name) {
                        switch (args.type) {
                            case "sql":
                                this.onEditSqlEtl(args.name);
                                break;
                            case "ravendb":
                                this.onEditRavenEtl(args.name);
                                break;
                            case "olap":
                                this.onEditOlapEtl(args.name);
                                break;
                        }
                    }
                });
    }

    compositionComplete() {
        super.compositionComplete();
        this.setupDisableReasons();
    }

    private loadServerSideConfiguration() {
        return new getPeriodicBackupConfigCommand(this.activeDatabase())
            .execute()
            .done(config => {
                this.serverConfiguration(config);
            });
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
        const tasksThatUseConnectionStrings = result.OngoingTasksList.filter((task) =>
                                                                              task.TaskType === "RavenEtl"    ||
                                                                              task.TaskType === "SqlEtl"      ||
                                                                              task.TaskType === "OlapEtl"     ||
                                                                              task.TaskType === "Replication" ||
                                                                              task.TaskType === "PullReplicationAsSink");
        for (let i = 0; i < tasksThatUseConnectionStrings.length; i++) {
            const task = tasksThatUseConnectionStrings[i];
            
            let taskData = { TaskId: task.TaskId,
                             TaskName: task.TaskName,
                             TaskType: task.TaskType };
            let stringName: string;
            
            switch (task.TaskType) {
                case "RavenEtl":
                    stringName = (task as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlListView).ConnectionStringName;
                    break;
                case "SqlEtl":
                    stringName = (task as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlListView).ConnectionStringName;
                    break;
                case "OlapEtl":
                    stringName = (task as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtlListView).ConnectionStringName;
                    break;
                case "Replication":
                    stringName = (task as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication).ConnectionStringName;
                    break;
                case "PullReplicationAsSink":
                    stringName = (task as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink).ConnectionStringName;
                    break;
            }

            if (this.connectionStringsTasksInfo[stringName]) {
                this.connectionStringsTasksInfo[stringName].push(taskData);
            } else {
                this.connectionStringsTasksInfo[stringName] = [taskData];
            }
        }
    }

    isConnectionStringInUse(connectionStringName: string, connectionStringType: Raven.Client.Documents.Operations.ConnectionStrings.ConnectionStringType): boolean {
        const possibleTasksTypes = this.getTasksTypes(connectionStringType);
        const tasksUsingConnectionString = this.connectionStringsTasksInfo[connectionStringName];
        
        const isInUse = _.includes(Object.keys(this.connectionStringsTasksInfo), connectionStringName);
        return isInUse && !!tasksUsingConnectionString.find(x => _.includes(possibleTasksTypes, x.TaskType));
    }
    
    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.activeDatabase())
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                // ravenEtl
                this.ravenEtlConnectionStringsNames(Object.keys(result.RavenConnectionStrings));
                const groupedRavenEtlNames = _.groupBy(this.ravenEtlConnectionStringsNames(), x => this.hasServerWidePrefix(x));
                const serverWideNames = _.sortBy(groupedRavenEtlNames.true, x => x.toUpperCase());
                const regularNames = _.sortBy(groupedRavenEtlNames.false, x => x.toUpperCase());
                this.ravenEtlConnectionStringsNames([...regularNames, ...serverWideNames]);
                
                // sqlEtl
                this.sqlEtlConnectionStringsNames(Object.keys(result.SqlConnectionStrings));
                this.sqlEtlConnectionStringsNames(_.sortBy(this.sqlEtlConnectionStringsNames(), x => x.toUpperCase()));

                // olapEtl
                this.olapEtlConnectionStringsNames(Object.keys(result.OlapConnectionStrings));
                this.olapEtlConnectionStringsNames(_.sortBy(this.olapEtlConnectionStringsNames(), x => x.toUpperCase()));
            });
    }

    confirmDelete(connectionStringName: string, connectionStringtype: Raven.Client.Documents.Operations.ConnectionStrings.ConnectionStringType) {
        const stringType = connectionStringtype === "Raven" ? "RavenDB" :  connectionStringtype === "Sql" ? "SQL" : "Olap";

        this.confirmationMessage("Delete connection string?",
            `You're deleting ${stringType} connection string: <br><ul><li><strong>${generalUtils.escapeHtml(connectionStringName)}</strong></li></ul>`, {
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
        this.onRavenEtl();
        this.clearTestResult();
    }
    
    onEditRavenEtl(connectionStringName: string) {
        this.clearTestResult();

        return getConnectionStringInfoCommand.forRavenEtl(this.activeDatabase(), connectionStringName)
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                this.editedRavenEtlConnectionString(new connectionStringRavenEtlModel(result.RavenConnectionStrings[connectionStringName], false, this.getTasksThatUseThisString(connectionStringName, "Raven")));
                this.onRavenEtl();
            });
    }
    
    private onRavenEtl() {
        this.editedRavenEtlConnectionString().topologyDiscoveryUrls.subscribe(() => this.clearTestResult());
        this.editedRavenEtlConnectionString().inputUrl().discoveryUrlName.subscribe(() => this.clearTestResult());

        this.editedSqlEtlConnectionString(null);
        this.editedOlapEtlConnectionString(null);
    }

    onAddSqlEtl() {
        eventsCollector.default.reportEvent("connection-strings", "add-sql-etl");
        this.editedSqlEtlConnectionString(connectionStringSqlEtlModel.empty());
        this.onSqlEtl();
        this.clearTestResult();
    }
    
    onEditSqlEtl(connectionStringName: string) {
        this.clearTestResult();

        return getConnectionStringInfoCommand.forSqlEtl(this.activeDatabase(), connectionStringName)
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                this.editedSqlEtlConnectionString(new connectionStringSqlEtlModel(result.SqlConnectionStrings[connectionStringName], false, this.getTasksThatUseThisString(connectionStringName, "Sql")));
                this.onSqlEtl();
            });
    }
    
    private onSqlEtl() {
        this.editedSqlEtlConnectionString().connectionString.subscribe(() => this.clearTestResult());

        this.editedRavenEtlConnectionString(null);
        this.editedOlapEtlConnectionString(null);
    }

    onAddOlapEtl() {
        eventsCollector.default.reportEvent("connection-strings", "add-olap-etl");
        this.editedOlapEtlConnectionString(connectionStringOlapEtlModel.empty());
        this.onOlapEtl();
        this.clearTestResult();
    }

    onEditOlapEtl(connectionStringName: string) {
        this.clearTestResult();

        return getConnectionStringInfoCommand.forOlapEtl(this.activeDatabase(), connectionStringName)
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const olapConnectionString = new connectionStringOlapEtlModel(
                    result.OlapConnectionStrings[connectionStringName],
                    false,
                    this.getTasksThatUseThisString(connectionStringName, "Olap"),
                    this.serverConfiguration().AllowedAwsRegions);
                
                this.editedOlapEtlConnectionString(olapConnectionString);
                this.onOlapEtl();
            });
    }
    
    private onOlapEtl() {
        const s3Settings = this.editedOlapEtlConnectionString().s3Settings();
        s3Settings.bucketName.subscribe(() => this.clearTestResult());
        s3Settings.useCustomS3Host.subscribe(() => this.clearTestResult());
        s3Settings.customServerUrl.subscribe(() => this.clearTestResult());
        s3Settings.accessKeyPropertyName.subscribe(() => this.clearTestResult());
        s3Settings.awsAccessKey.subscribe(() => this.clearTestResult());
        s3Settings.awsSecretKey.subscribe(() => this.clearTestResult());
        s3Settings.awsRegionName.subscribe(() => this.clearTestResult());
        s3Settings.remoteFolderName.subscribe(() => this.clearTestResult());

        const azureSettings = this.editedOlapEtlConnectionString().azureSettings();
        azureSettings.storageContainer.subscribe(() => this.clearTestResult());
        azureSettings.accountName.subscribe(() => this.clearTestResult());
        azureSettings.accountKey.subscribe(() => this.clearTestResult());
        
        const googleCloudSettings = this.editedOlapEtlConnectionString().googleCloudSettings();
        googleCloudSettings.bucket.subscribe(() => this.clearTestResult());
        googleCloudSettings.remoteFolderName.subscribe(() => this.clearTestResult());
        googleCloudSettings.googleCredentialsJson.subscribe(() => this.clearTestResult());
        
        const glacierSettings = this.editedOlapEtlConnectionString().glacierSettings();
        glacierSettings.vaultName.subscribe(() => this.clearTestResult());
        glacierSettings.remoteFolderName.subscribe(() => this.clearTestResult());
        glacierSettings.selectedAwsRegion.subscribe(() => this.clearTestResult());
        glacierSettings.awsAccessKey.subscribe(() => this.clearTestResult());
        glacierSettings.awsSecretKey.subscribe(() => this.clearTestResult());
        
        this.editedRavenEtlConnectionString(null);
        this.editedSqlEtlConnectionString(null);
    }
    
    private getTasksThatUseThisString(connectionStringName: string, connectionStringType: Raven.Client.Documents.Operations.ConnectionStrings.ConnectionStringType): { taskName: string; taskId: number }[] {
        const tasksUsingConnectionString = this.connectionStringsTasksInfo[connectionStringName];
        
        if (!tasksUsingConnectionString) {
            return [];
        } else {
            const possibleTasksTypes = this.getTasksTypes(connectionStringType);
            const tasks = tasksUsingConnectionString.filter(x => _.includes(possibleTasksTypes, x.TaskType));
            
            const tasksData = tasks.map((task) => { return { taskName: task.TaskName, taskId: task.TaskId }; });
            return tasksData ? _.sortBy(tasksData, x => x.taskName.toUpperCase()) : [];
        }
    }
    
    private getTasksTypes(connectionType: Raven.Client.Documents.Operations.ConnectionStrings.ConnectionStringType): Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType[] {
        if (connectionType === "Sql") {
            return ["SqlEtl"];
        }
        
        if (connectionType === "Olap") {
            return ["OlapEtl"];
        }
        
        return ["RavenEtl", "Replication", "PullReplicationAsSink"];
    }

    onTestConnectionSql() {
        this.clearTestResult();
        const sqlConnectionString = this.editedSqlEtlConnectionString();

        if (sqlConnectionString) {
            if (this.isValid(sqlConnectionString.testConnectionValidationGroup)) {
                eventsCollector.default.reportEvent("SQL-connection-string", "test-connection");

                this.spinners.test(true);
                sqlConnectionString.testConnection(this.activeDatabase())
                    .done((testResult) => this.testConnectionResult(testResult))
                    .always(() => {
                        this.spinners.test(false);
                    });
            }
        }
    }
    
    onTestConnectionRaven(urlToTest: discoveryUrl) {
        this.clearTestResult();
        const ravenConnectionString = this.editedRavenEtlConnectionString();
        eventsCollector.default.reportEvent("ravenDB-connection-string", "test-connection");
        
        this.spinners.test(true);
        ravenConnectionString.selectedUrlToTest(urlToTest.discoveryUrlName());

        ravenConnectionString.testConnection(urlToTest)
            .done(result => this.testConnectionResult(result))
            .always(() => {
                this.spinners.test(false);
                this.fullErrorDetailsVisible(false);
            });
    }
    
    onCloseEdit() {
        this.editedRavenEtlConnectionString(null);
        this.editedSqlEtlConnectionString(null);
        this.editedOlapEtlConnectionString(null);
    }

    onSave() {
        let model: connectionStringRavenEtlModel | connectionStringSqlEtlModel | connectionStringOlapEtlModel;
        
        const editedRavenEtl = this.editedRavenEtlConnectionString();
        const editedSqlEtl = this.editedSqlEtlConnectionString();
        const editedOlapEtl = this.editedOlapEtlConnectionString();
        
        // 1. Validate model
        if (editedRavenEtl) {
            if (!this.isValidEditedRavenEtl()) {
                return;
            }
            model = editedRavenEtl;
            
        } else if (editedSqlEtl) {
            if (!this.isValidEditedSqlEtl()) {
                return;
            }
            model = editedSqlEtl;
            
        } else if (editedOlapEtl) {
            if (!this.isValidEditedOlapEtl()) {
                return;
            }
            model = editedOlapEtl;
        }

        // 2. Create/add the new connection string
        new saveConnectionStringCommand(this.activeDatabase(), model)
            .execute()
            .done(() => {
                // 3. Refresh list view....
                this.getAllConnectionStrings();

                this.editedRavenEtlConnectionString(null);
                this.editedSqlEtlConnectionString(null);
                this.editedOlapEtlConnectionString(null);

                this.dirtyFlag().reset();
            });
    }

    isValidEditedRavenEtl() {
        const editedRavenEtl = this.editedRavenEtlConnectionString();
        
        let isValid = true;

        const discoveryUrl = editedRavenEtl.inputUrl().discoveryUrlName;
        if (discoveryUrl()) {
            if (discoveryUrl.isValid()) {
                // user probably forgot to click on 'Add Url' button 
                editedRavenEtl.addDiscoveryUrlWithBlink();
            } else {
                isValid = false;
            }
        }

        if (!this.isValid(editedRavenEtl.validationGroup)) {
            isValid = false;
        }

        return isValid;
    }

    isValidEditedSqlEtl() {
        const editedSqlEtl = this.editedSqlEtlConnectionString();
        return this.isValid(editedSqlEtl.validationGroup);
    }

    isValidEditedOlapEtl() {
        const editedOlapEtl = this.editedOlapEtlConnectionString();

        let isValid = true;

        if (!this.isValid(editedOlapEtl.validationGroup)) {
            isValid = false;
        }

        const localSettings = editedOlapEtl.localSettings();
        if (localSettings.enabled() && !this.isValid(localSettings.effectiveValidationGroup()))
            isValid = false;

        const s3Settings = editedOlapEtl.s3Settings();
        if (s3Settings.enabled() && !this.isValid(s3Settings.effectiveValidationGroup()))
            isValid = false;

        const azureSettings = editedOlapEtl.azureSettings();
        if (azureSettings.enabled() && !this.isValid(azureSettings.effectiveValidationGroup()))
            isValid = false;

        const googleCloudSettings = editedOlapEtl.googleCloudSettings();
        if (googleCloudSettings.enabled() && !this.isValid(googleCloudSettings.effectiveValidationGroup()))
            isValid = false;

        const glacierSettings = editedOlapEtl.glacierSettings();
        if (glacierSettings.enabled() && !this.isValid(glacierSettings.effectiveValidationGroup()))
            isValid = false;

        const ftpSettings = editedOlapEtl.ftpSettings();
        if (ftpSettings.enabled() && !this.isValid(ftpSettings.effectiveValidationGroup()))
            isValid = false;
        
        return isValid;
    }
    
    taskEditLink(taskId: number, connectionStringName: string) : string {
        const task = _.find(this.connectionStringsTasksInfo[connectionStringName], task => task.TaskId === taskId);
        const urls = appUrl.forCurrentDatabase();

        switch (task.TaskType) {
            case "SqlEtl":
                return urls.editSqlEtl(task.TaskId)();
            case "OlapEtl":
                return urls.editOlapEtl(task.TaskId)();
            case "RavenEtl": 
                return urls.editRavenEtl(task.TaskId)();
            case "Replication":
               return urls.editExternalReplication(task.TaskId)();
        }
    }
    
    isServerWide(name: string) {
        return ko.pureComputed(() => {
            return this.hasServerWidePrefix(name);
        })
    } 
    
    private hasServerWidePrefix(name: string) {
        return name.startsWith(connectionStringRavenEtlModel.serverWidePrefix);
    }

    testCredentials(bs: backupSettings) {
        if (!this.isValid(bs.effectiveValidationGroup())) {
            return;
        }

        bs.isTestingCredentials(true);
        bs.testConnectionResult(null);

        new testPeriodicBackupCredentialsCommand(bs.connectionType, bs.toDto())
            .execute()
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                bs.testConnectionResult(result);
            })
            .always(() => bs.isTestingCredentials(false));
    }
}

export = connectionStrings

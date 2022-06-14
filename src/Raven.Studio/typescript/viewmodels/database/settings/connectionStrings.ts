import viewModelBase = require("viewmodels/viewModelBase");
import connectionStringRavenEtlModel = require("models/database/settings/connectionStringRavenEtlModel");
import connectionStringSqlEtlModel = require("models/database/settings/connectionStringSqlEtlModel");
import connectionStringOlapEtlModel = require("models/database/settings/connectionStringOlapEtlModel");
import connectionStringElasticSearchEtlModel = require("models/database/settings/connectionStringElasticSearchEtlModel");
import connectionStringKafkaEtlModel = require("models/database/settings/connectionStringKafkaEtlModel");
import connectionStringRabbitMqEtlModel = require("models/database/settings/connectionStringRabbitMqEtlModel");
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
import ongoingTaskModel from "models/database/tasks/ongoingTaskModel";

class connectionStrings extends viewModelBase {

    view = require("views/database/settings/connectionStrings.html");
    
    connectionStringOlapView = require("views/database/settings/connectionStringOlap.html");
    connectionStringRavenView = require("views/database/settings/connectionStringRaven.html");
    connectionStringElasticView = require("views/database/settings/connectionStringElasticSearch.html");
    connectionStringSqlView = require("views/database/settings/connectionStringSql.html");
    connectionStringKafkaView = require("views/database/settings/connectionStringKafka.html");
    connectionStringRabbitMqView = require("views/database/settings/connectionStringRabbitMq.html");

    backupDestinationTestCredentialsView = require("views/partial/backupDestinationTestCredentialsResults.html");
    backupConfigurationView = require("views/partial/backupConfigurationScript.html");
    backupDestinationLocalView = require("views/partial/backupDestinationLocal.html");

    ravenEtlConnectionStringsNames = ko.observableArray<string>([]);
    sqlEtlConnectionStringsNames = ko.observableArray<string>([]);
    olapEtlConnectionStringsNames = ko.observableArray<string>([]);
    elasticSearchEtlConnectionStringsNames = ko.observableArray<string>([]);
    kafkaEtlConnectionStringsNames = ko.observableArray<string>([]);
    rabbitMqEtlConnectionStringsNames = ko.observableArray<string>([]);
    
    // Mapping from { connection string } to { taskId, taskName, taskType }
    connectionStringsTasksInfo: dictionary<Array<{ TaskId: number, TaskName: string, TaskType: StudioTaskType }>> = {};
    
    editedRavenEtlConnectionString = ko.observable<connectionStringRavenEtlModel>(null);
    editedSqlEtlConnectionString = ko.observable<connectionStringSqlEtlModel>(null);
    editedOlapEtlConnectionString = ko.observable<connectionStringOlapEtlModel>(null);
    editedElasticSearchEtlConnectionString = ko.observable<connectionStringElasticSearchEtlModel>(null);
    editedKafkaEtlConnectionString = ko.observable<connectionStringKafkaEtlModel>(null);
    editedRabbitMqEtlConnectionString = ko.observable<connectionStringRabbitMqEtlModel>(null);

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
        this.bindToCurrentInstance("onEditSqlEtl", "onEditRavenEtl", "onEditOlapEtl", "onEditElasticSearchEtl", "onEditKafkaEtl", "onEditRabbitMqEtl",
                                   "confirmDelete", "isConnectionStringInUse", 
                                   "onTestConnectionRaven", "onTestConnectionElasticSearch", "onTestConnectionKafka", "onTestConnectionRabbitMq", "testCredentials");
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

            const elasticEtl = this.editedElasticSearchEtlConnectionString();
            if (elasticEtl) {
                return elasticEtl.dirtyFlag().isDirty();
            }

            const kafkaEtl = this.editedKafkaEtlConnectionString();
            if (kafkaEtl) {
                return kafkaEtl.dirtyFlag().isDirty();
            }

            const rabbitMqEtl = this.editedRabbitMqEtlConnectionString();
            if (rabbitMqEtl) {
                return rabbitMqEtl.dirtyFlag().isDirty();
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
                            case "Sql":
                                this.onEditSqlEtl(args.name);
                                break;
                            case "Raven":
                                this.onEditRavenEtl(args.name);
                                break;
                            case "Olap":
                                this.onEditOlapEtl(args.name);
                                break;
                            case "ElasticSearch":
                                this.onEditElasticSearchEtl(args.name);
                                break;
                            case "Kafka":
                                this.onEditKafkaEtl(args.name);
                                break;
                            case "RabbitMQ":
                                this.onEditRabbitMqEtl(args.name);
                                break;
                            default: 
                                console.warn(`Invalid etl type: ` + args.type);
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
                                                                              task.TaskType === "RavenEtl"         ||
                                                                              task.TaskType === "SqlEtl"           ||
                                                                              task.TaskType === "OlapEtl"          ||
                                                                              task.TaskType === "ElasticSearchEtl" ||
                                                                              task.TaskType === "QueueEtl"         ||
                                                                              task.TaskType === "Replication"      ||
                                                                              task.TaskType === "PullReplicationAsSink");
        for (let i = 0; i < tasksThatUseConnectionStrings.length; i++) {
            const task = tasksThatUseConnectionStrings[i];
            
            const studioTaskType = ongoingTaskModel.getStudioTaskTypeFromServerType(task);
            
            let taskData = { TaskId: task.TaskId,
                             TaskName: task.TaskName,
                             TaskType: studioTaskType };
            
            let stringName: string;
            
            switch (studioTaskType) {
                case "RavenEtl":
                    stringName = (task as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlListView).ConnectionStringName;
                    break;
                case "SqlEtl":
                    stringName = (task as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlListView).ConnectionStringName;
                    break;
                case "OlapEtl":
                    stringName = (task as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtlListView).ConnectionStringName;
                    break;
                case "ElasticSearchEtl":
                    stringName = (task as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtlListView).ConnectionStringName;
                    break;
                case "KafkaQueueEtl":
                case "RabbitQueueEtl":
                    stringName = (task as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlListView).ConnectionStringName;
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

    isConnectionStringInUse(connectionStringName: string, connectionStringType: StudioEtlType): boolean {
        const possibleTasksTypes = this.getTasksTypes(connectionStringType);
        const tasksUsingConnectionString = this.connectionStringsTasksInfo[connectionStringName];
        
        const isInUse = _.includes(Object.keys(this.connectionStringsTasksInfo), connectionStringName);
        return isInUse && !!tasksUsingConnectionString.find(x => _.includes(possibleTasksTypes, x.TaskType));
    }
    
    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.activeDatabase())
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                // raven Etl
                this.ravenEtlConnectionStringsNames(Object.keys(result.RavenConnectionStrings));
                const groupedRavenEtlNames = _.groupBy(this.ravenEtlConnectionStringsNames(), x => this.hasServerWidePrefix(x));
                const serverWideNames = _.sortBy(groupedRavenEtlNames.true, x => x.toUpperCase());
                const regularNames = _.sortBy(groupedRavenEtlNames.false, x => x.toUpperCase());
                this.ravenEtlConnectionStringsNames([...regularNames, ...serverWideNames]);
                
                // sql Etl
                this.sqlEtlConnectionStringsNames(Object.keys(result.SqlConnectionStrings));
                this.sqlEtlConnectionStringsNames(_.sortBy(this.sqlEtlConnectionStringsNames(), x => x.toUpperCase()));

                // olap Etl
                this.olapEtlConnectionStringsNames(Object.keys(result.OlapConnectionStrings));
                this.olapEtlConnectionStringsNames(_.sortBy(this.olapEtlConnectionStringsNames(), x => x.toUpperCase()));

                // elasticSearch Etl
                this.elasticSearchEtlConnectionStringsNames(Object.keys(result.ElasticSearchConnectionStrings));
                this.elasticSearchEtlConnectionStringsNames(_.sortBy(this.elasticSearchEtlConnectionStringsNames(), x => x.toUpperCase()));
                
                // Kafka Etl
                const queueObjects = Object.values(result.QueueConnectionStrings);
                const kafkaObjects = queueObjects.filter(x => x.BrokerType === "Kafka");
                const kafkaNames = kafkaObjects.map(x => x.Name);
                
                this.kafkaEtlConnectionStringsNames(kafkaNames);
                this.kafkaEtlConnectionStringsNames(_.sortBy(this.kafkaEtlConnectionStringsNames(), x => x.toUpperCase()));

                // RabbitMQ Etl
                const rabbitMqObjects = queueObjects.filter(x => x.BrokerType === "RabbitMq");
                const rabbitMqNames = rabbitMqObjects.map(x => x.Name);
                
                this.rabbitMqEtlConnectionStringsNames(rabbitMqNames);
                this.rabbitMqEtlConnectionStringsNames(_.sortBy(this.rabbitMqEtlConnectionStringsNames(), x => x.toUpperCase()));
            });
    }

    confirmDelete(connectionStringName: string, connectionStringType: StudioEtlType) {
        let stringName: string;
        switch (connectionStringType) {
            case "Raven":
                stringName = "RavenDB"; 
                break;
            case "Sql":
                stringName = "SQL";
                break;
            case "Olap":
                stringName = "OLAP";
                break;
            case "ElasticSearch":
                stringName = "Elasticsearch"
                break;
            case "Kafka":
                stringName = "Kafka";
                break;
            case "RabbitMQ":
                stringName = "RabbitMQ";
                break;
            default:
                console.warn("Invalid connection string type: " + connectionStringType);
                break;
        }

        this.confirmationMessage("Delete connection string?",
            `You're deleting ${stringName} connection string: <br><ul><li><strong>${generalUtils.escapeHtml(connectionStringName)}</strong></li></ul>`, {
                buttons: ["Cancel", "Delete"],
                html: true
            })
            .done(result => {
                if (result.can) {
                    this.deleteConnectionString(connectionStringType, connectionStringName);
                }
            });
    }

    private deleteConnectionString(connectionStringType: StudioEtlType, connectionStringName: string) {
        new deleteConnectionStringCommand(this.activeDatabase(), ongoingTaskModel.getServerEtlTypeFromStudioType(connectionStringType), connectionStringName)
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
        this.editedElasticSearchEtlConnectionString(null);
        this.editedKafkaEtlConnectionString(null);
        this.editedRabbitMqEtlConnectionString(null);
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
        this.editedElasticSearchEtlConnectionString(null);
        this.editedKafkaEtlConnectionString(null);
        this.editedRabbitMqEtlConnectionString(null);
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
        this.editedElasticSearchEtlConnectionString(null);
        this.editedKafkaEtlConnectionString(null);
        this.editedRabbitMqEtlConnectionString(null);
    }

    onAddElasticSearchEtl() {
        eventsCollector.default.reportEvent("connection-strings", "add-elastic-search-etl");
        this.editedElasticSearchEtlConnectionString(connectionStringElasticSearchEtlModel.empty());
        this.onElasticSearchEtl();
        this.clearTestResult();
    }

    onEditElasticSearchEtl(connectionStringName: string) {
        this.clearTestResult();

        return getConnectionStringInfoCommand.forElasticSearchEtl(this.activeDatabase(), connectionStringName)
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const elasticConnectionString = new connectionStringElasticSearchEtlModel(
                    result.ElasticSearchConnectionStrings[connectionStringName],
                    false,
                    this.getTasksThatUseThisString(connectionStringName, "ElasticSearch"));

                this.editedElasticSearchEtlConnectionString(elasticConnectionString);
                this.onElasticSearchEtl();
            });
    }

    private onElasticSearchEtl() {
        this.editedElasticSearchEtlConnectionString().nodesUrls.subscribe(() => this.clearTestResult());
        this.editedElasticSearchEtlConnectionString().inputUrl().discoveryUrlName.subscribe(() => this.clearTestResult());

        this.editedRavenEtlConnectionString(null);
        this.editedOlapEtlConnectionString(null);
        this.editedSqlEtlConnectionString(null);
        this.editedKafkaEtlConnectionString(null);
        this.editedRabbitMqEtlConnectionString(null);
    }

    onAddKafkaEtl() {
        eventsCollector.default.reportEvent("connection-strings", "add-kafka-etl");
        this.editedKafkaEtlConnectionString(connectionStringKafkaEtlModel.empty());
        this.onKafkaEtl();
        this.clearTestResult();
    }

    onEditKafkaEtl(connectionStringName: string) {
        this.clearTestResult();

        return getConnectionStringInfoCommand.forKafkaEtl(this.activeDatabase(), connectionStringName)
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const kafkaConnectionString = new connectionStringKafkaEtlModel(
                    result.QueueConnectionStrings[connectionStringName],
                    false,
                    this.getTasksThatUseThisString(connectionStringName, "Kafka"));

                this.editedKafkaEtlConnectionString(kafkaConnectionString);
                this.onKafkaEtl();
            });
    }

    private onKafkaEtl() {
        this.editedKafkaEtlConnectionString().bootstrapServers.subscribe(() => this.clearTestResult());

        this.editedRavenEtlConnectionString(null);
        this.editedOlapEtlConnectionString(null);
        this.editedSqlEtlConnectionString(null);
        this.editedElasticSearchEtlConnectionString(null);
        this.editedRabbitMqEtlConnectionString(null);
    }

    onAddRabbitMqEtl() {
        eventsCollector.default.reportEvent("connection-strings", "add-rabbitmq-etl");
        this.editedRabbitMqEtlConnectionString(connectionStringRabbitMqEtlModel.empty());
        this.onRabbitMqEtl();
        this.clearTestResult();
    }

    onEditRabbitMqEtl(connectionStringName: string) {
        this.clearTestResult();

        return getConnectionStringInfoCommand.forRabbitMqEtl(this.activeDatabase(), connectionStringName)
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const rabbitMqConnectionString = new connectionStringRabbitMqEtlModel(
                    result.QueueConnectionStrings[connectionStringName],
                    false,
                    this.getTasksThatUseThisString(connectionStringName, "RabbitMQ"));

                this.editedRabbitMqEtlConnectionString(rabbitMqConnectionString);
                this.onRabbitMqEtl();
            });
    }

    private onRabbitMqEtl() {
        this.editedRabbitMqEtlConnectionString().rabbitMqConnectionString.subscribe(() => this.clearTestResult());

        this.editedRavenEtlConnectionString(null);
        this.editedOlapEtlConnectionString(null);
        this.editedSqlEtlConnectionString(null);
        this.editedElasticSearchEtlConnectionString(null);
        this.editedKafkaEtlConnectionString(null);
    }

    private getTasksThatUseThisString(connectionStringName: string, connectionStringType: StudioEtlType): { taskName: string; taskId: number }[] {
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
    
    private getTasksTypes(connectionType: StudioEtlType): StudioTaskType[] {
        if (connectionType === "Sql") {
            return ["SqlEtl"];
        }
        
        if (connectionType === "Olap") {
            return ["OlapEtl"];
        }

        if (connectionType === "ElasticSearch") {
            return ["ElasticSearchEtl"];
        }
        
        if (connectionType === "Kafka") {
            return ["KafkaQueueEtl"];
        }

        if (connectionType === "RabbitMQ") {
            return ["RabbitQueueEtl"];
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

    onTestConnectionElasticSearch(urlToTest: discoveryUrl) {
        this.clearTestResult();
        const elasticConnectionString = this.editedElasticSearchEtlConnectionString();
        eventsCollector.default.reportEvent("elastic-search-connection-string", "test-connection");

        this.spinners.test(true);
        elasticConnectionString.selectedUrlToTest(urlToTest.discoveryUrlName());

        elasticConnectionString.testConnection(this.activeDatabase(), urlToTest)
            .done(result => this.testConnectionResult(result))
            .always(() => {
                this.spinners.test(false);
                this.fullErrorDetailsVisible(false);
            });
    }

    onTestConnectionKafka() {
        this.clearTestResult();
        const kafkaConnectionString = this.editedKafkaEtlConnectionString();
        eventsCollector.default.reportEvent("kafka-connection-string", "test-connection");

        this.spinners.test(true);

        kafkaConnectionString.testConnection(this.activeDatabase())
            .done(result => this.testConnectionResult(result))
            .always(() => {
                this.spinners.test(false);
                this.fullErrorDetailsVisible(false);
            });
    }

    onTestConnectionRabbitMq() {
        this.clearTestResult();
        const rabbitMqConnectionString = this.editedRabbitMqEtlConnectionString();
        eventsCollector.default.reportEvent("rabbitmq-connection-string", "test-connection");

        this.spinners.test(true);

        rabbitMqConnectionString.testConnection(this.activeDatabase())
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
        this.editedElasticSearchEtlConnectionString(null);
        this.editedKafkaEtlConnectionString(null);
        this.editedRabbitMqEtlConnectionString(null);
    }

    onSave() {
        let model: connectionStringRavenEtlModel | connectionStringSqlEtlModel | connectionStringOlapEtlModel |
                   connectionStringElasticSearchEtlModel | connectionStringKafkaEtlModel | connectionStringRabbitMqEtlModel;
        
        const editedRavenEtl = this.editedRavenEtlConnectionString();
        const editedSqlEtl = this.editedSqlEtlConnectionString();
        const editedOlapEtl = this.editedOlapEtlConnectionString();
        const editedElasticSearchEtl = this.editedElasticSearchEtlConnectionString();
        const editedKafkaEtl = this.editedKafkaEtlConnectionString();
        const editedRabbitMqEtl = this.editedRabbitMqEtlConnectionString();
        
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
            
        } else if (editedElasticSearchEtl) {
            if (!this.isValidEditedElasticSearchEtl()) {
                return;
            }
            model = editedElasticSearchEtl;
            
        } else if (editedKafkaEtl) {
            if (!this.isValidEditedKafkaEtl()) {
                return;
            }
            model = editedKafkaEtl;
            
        } else if (editedRabbitMqEtl) {
            if (!this.isValidEditedRabbitMqEtl()) {
                return;
            }
            model = editedRabbitMqEtl;
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
                this.editedElasticSearchEtlConnectionString(null);
                this.editedKafkaEtlConnectionString(null);
                this.editedRabbitMqEtlConnectionString(null);

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

    isValidEditedElasticSearchEtl() {
        const editedElasticEtl = this.editedElasticSearchEtlConnectionString();
        return this.isValid(editedElasticEtl.validationGroup) && 
               this.isValid(editedElasticEtl.authentication().validationGroup);
    }

    isValidEditedKafkaEtl() {
        const editedKafkaEtl = this.editedKafkaEtlConnectionString();
        
        let validOptions = true;
        editedKafkaEtl.connectionOptions().forEach(x => {
            validOptions = this.isValid(x.validationGroup);
        })
        
        return this.isValid(editedKafkaEtl.validationGroup) && validOptions;
    }

    isValidEditedRabbitMqEtl() {
        const editedRabbitMqEtl = this.editedRabbitMqEtlConnectionString();
        return this.isValid(editedRabbitMqEtl.validationGroup);
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
            case "ElasticSearchEtl":
                return urls.editElasticSearchEtl(task.TaskId)();
            case "KafkaQueueEtl":
                return urls.editKafkaEtl(task.TaskId)();
            case "RabbitQueueEtl":
                return urls.editRabbitMqEtl(task.TaskId)();
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

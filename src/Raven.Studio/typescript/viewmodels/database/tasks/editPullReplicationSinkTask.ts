import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import savePullReplicationSinkTaskCommand = require("commands/database/tasks/savePullReplicationSinkTaskCommand");
import ongoingTaskPullReplicationSinkEditModel = require("models/database/tasks/ongoingTaskPullReplicationSinkEditModel");
import eventsCollector = require("common/eventsCollector");
import generalUtils = require("common/generalUtils");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import getPossibleMentorsCommand = require("commands/database/tasks/getPossibleMentorsCommand");
import connectionStringRavenEtlModel = require("models/database/settings/connectionStringRavenEtlModel");
import jsonUtil = require("common/jsonUtil");
import getOngoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");

class editPullReplicationSinkTask extends viewModelBase {

    editedReplication = ko.observable<ongoingTaskPullReplicationSinkEditModel>();
    isAddingNewTask = ko.observable<boolean>(true);
    private taskId: number = null;
    
    possibleMentors = ko.observableArray<string>([]);
    
    ravenEtlConnectionStringsDetails = ko.observableArray<Raven.Client.Documents.Operations.ETL.RavenConnectionString>([]);

    connectionStringsUrl = appUrl.forCurrentDatabase().connectionStrings();

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    
    spinners = { 
        test: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false) 
    };

    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;

    createNewConnectionString = ko.observable<boolean>(false);
    newConnectionString = ko.observable<connectionStringRavenEtlModel>();

    constructor() {
        super();
        this.bindToCurrentInstance("useConnectionString", "onTestConnectionRaven");
    }

    activate(args: any) { 
        super.activate(args);
        const deferred = $.Deferred<void>();

        if (args.taskId) {
            // 1. Editing an existing task
            this.isAddingNewTask(false);
            this.taskId = args.taskId;

            getOngoingTaskInfoCommand.forPullReplicationSink(this.activeDatabase(), this.taskId)
                .execute()
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink) => { 
                    this.editedReplication(new ongoingTaskPullReplicationSinkEditModel(result));
                    deferred.resolve();
                })
                .fail(() => {
                    deferred.reject();
                    
                    router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
                });
        } else {
            // 2. Creating a new task
            this.isAddingNewTask(true);
            this.editedReplication(ongoingTaskPullReplicationSinkEditModel.empty());
            deferred.resolve();
        }

        deferred.done(() => this.initObservables());
        
        return $.when<any>(this.getAllConnectionStrings(), this.loadPossibleMentors(), deferred);
    }
    
    private loadPossibleMentors() {
        return new getPossibleMentorsCommand(this.activeDatabase().name)
            .execute()
            .done(mentors => this.possibleMentors(mentors));
    }

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.activeDatabase())
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const connectionStrings = (<any>Object).values(result.RavenConnectionStrings);
                this.ravenEtlConnectionStringsDetails(_.sortBy(connectionStrings, x => x.Name.toUpperCase()));                
            });
    }

    private initObservables() {        
        this.shortErrorText = ko.pureComputed(() => {
            const result = this.testConnectionResult();
            if (!result || result.Success) {
                return "";
            }
            return generalUtils.trimMessage(result.Error);
        });
        
        const model = this.editedReplication();
        
        this.dirtyFlag = new ko.DirtyFlag([
                model.taskName,
                model.manualChooseMentor,
                model.preferredMentor,
                model.connectionStringName,
                model.hubDefinitionName,
                this.createNewConnectionString
            ], false, jsonUtil.newLineNormalizingHashFunction);

        this.newConnectionString(connectionStringRavenEtlModel.empty());

        // Open the 'Create new conn. str.' area if no connection strings are yet defined 
        this.ravenEtlConnectionStringsDetails.subscribe((value) => { this.createNewConnectionString(!value.length) }); 
        
        // Discard test connection result when needed
        this.createNewConnectionString.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().inputUrl().discoveryUrlName.subscribe(() => this.testConnectionResult(null));
    }

    compositionComplete() {
        super.compositionComplete();
        document.getElementById('taskName').focus();
        
        $('.edit-pull-replication-sink-task [data-toggle="tooltip"]').tooltip();
    }

    saveTask() {
        let hasAnyErrors = false;
        
        // 0. Save discovery URL if user forgot to hit 'add url' button
        if (this.createNewConnectionString() &&
            this.newConnectionString().inputUrl().discoveryUrlName() &&
            this.isValid(this.newConnectionString().inputUrl().validationGroup)) {
                this.newConnectionString().addDiscoveryUrlWithBlink();
        }
        
        // 1. Validate *new connection string* (if relevant..) 
        if (this.createNewConnectionString()) {
            if (!this.isValid(this.newConnectionString().validationGroup)) {
                hasAnyErrors = true;
            } else {
                // Use the new connection string
                this.editedReplication().connectionStringName(this.newConnectionString().connectionStringName());
            }
        }

        // 2. Validate *general form*
        if (!this.isValid(this.editedReplication().validationGroup)) {
            hasAnyErrors = true;
        }
       
        if (hasAnyErrors) {
            return false;
        }

        this.spinners.save(true);

        // 3. All is well, Save connection string (if relevant..) 
        let savingNewStringAction = $.Deferred<void>();
        if (this.createNewConnectionString()) {
            this.newConnectionString()
                .saveConnectionString(this.activeDatabase())
                .done(() => {
                    savingNewStringAction.resolve();
                })
                .fail(() => {
                    this.spinners.save(false);
                });
        } else {
            savingNewStringAction.resolve();
        }
        
        // 4. All is well, Save Replication task
        savingNewStringAction.done(() => {
            const dto = this.editedReplication().toDto(this.taskId);
            this.taskId = this.isAddingNewTask() ? 0 : this.taskId;

            eventsCollector.default.reportEvent("pull-replication-sink", "save");
            
            new savePullReplicationSinkTaskCommand(this.activeDatabase(), dto)
                .execute()
                .done(() => {
                    this.dirtyFlag().reset();
                    this.goToOngoingTasksView();
                })
                .always(() => this.spinners.save(false));
        });  
    }
   
    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
    }

    useConnectionString(connectionStringToUse: string) {
        this.editedReplication().connectionStringName(connectionStringToUse);
    }
    
    onTestConnectionRaven(urlToTest: string) {
        eventsCollector.default.reportEvent("pull-replication-sink", "test-connection");
        this.spinners.test(true);
        this.newConnectionString().selectedUrlToTest(urlToTest);
        this.testConnectionResult(null);

        this.newConnectionString()
            .testConnection(urlToTest)
            .done(result => this.testConnectionResult(result))
            .always(() => {
                this.spinners.test(false);
                this.newConnectionString().selectedUrlToTest(null);
            });
    }
}

export = editPullReplicationSinkTask;

import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import saveExternalReplicationTaskCommand = require("commands/database/tasks/saveExternalReplicationTaskCommand");
import ongoingTaskReplicationEditModel = require("models/database/tasks/ongoingTaskReplicationEditModel");
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import eventsCollector = require("common/eventsCollector");
import generalUtils = require("common/generalUtils");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import getPossibleMentorsCommand = require("commands/database/tasks/getPossibleMentorsCommand");
import connectionStringRavenEtlModel = require("models/database/settings/connectionStringRavenEtlModel");
import jsonUtil = require("common/jsonUtil");

class editExternalReplicationTask extends viewModelBase {

    editedExternalReplication = ko.observable<ongoingTaskReplicationEditModel>();
    isAddingNewReplicationTask = ko.observable<boolean>(true);
    private taskId: number = null;
    
    possibleMentors = ko.observableArray<string>([]);
    ravenEtlConnectionStringsNames = ko.observableArray<string>([]);
    connectionStringsUrl = appUrl.forCurrentDatabase().connectionStrings();

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    
    spinners = { 
        test: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false) 
    };

    fullErrorDetailsVisible = ko.observable<boolean>(false);

    shortErrorText: KnockoutObservable<string>;
    showError = ko.observable<boolean>(true);

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
            this.isAddingNewReplicationTask(false);
            this.taskId = args.taskId;

            ongoingTaskInfoCommand.forExternalReplication(this.activeDatabase(), this.taskId)
                .execute()
                .done((result: Raven.Client.ServerWide.Operations.OngoingTaskReplication) => { 
                    this.editedExternalReplication(new ongoingTaskReplicationEditModel(result));
                    deferred.resolve();
                })
                .fail(() => {
                    deferred.reject();
                    
                    router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
                });
        }
        else {
            // 2. Creating a new task
            this.isAddingNewReplicationTask(true);
            this.editedExternalReplication(ongoingTaskReplicationEditModel.empty());
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
            .done((result: Raven.Client.ServerWide.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const connectionStringsNames = Object.keys(result.RavenConnectionStrings);
                this.ravenEtlConnectionStringsNames(_.sortBy(connectionStringsNames, x => x.toUpperCase()));
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
        
        const model = this.editedExternalReplication();
        
        this.dirtyFlag = new ko.DirtyFlag([
                model.taskName,
                model.manualChooseMentor,
                model.preferredMentor,
                model.connectionStringName,
                this.createNewConnectionString                
            ], false, jsonUtil.newLineNormalizingHashFunction);

        this.newConnectionString(connectionStringRavenEtlModel.empty());

        // Discard test connection result when needed
        this.createNewConnectionString.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().inputUrl().discoveryUrlName.subscribe(() => this.testConnectionResult(null));
    }

    compositionComplete() {
        super.compositionComplete();
        document.getElementById('taskName').focus();
        
        $('.edit-replication-task [data-toggle="tooltip"]').tooltip();
    }

    saveExternalReplication() {
        let hasAnyErrors = false;
        this.spinners.save(true);
        
        // 1. Validate *new connection string* (if relevant..) 
        if (this.createNewConnectionString()) {
            if (!this.isValid(this.newConnectionString().validationGroup)) {
                hasAnyErrors = true;
            }
            else {
                // Use the new connection string
                this.editedExternalReplication().connectionStringName(this.newConnectionString().connectionStringName());
            }
        }

        // 2. Validate *general form*
        if (!this.isValid(this.editedExternalReplication().validationGroup)) {
            hasAnyErrors = true;
        }            
       
        if (hasAnyErrors) {
            this.spinners.save(false);
            return false;
        }

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
                    return false;
                });
        }
        else {
            savingNewStringAction.resolve();
        }
        
        // 4. All is well, Save Replication task
        savingNewStringAction.done(()=> {
            const dto = this.editedExternalReplication().toDto(this.taskId);
            this.taskId = this.isAddingNewReplicationTask() ? 0 : this.taskId;

            new saveExternalReplicationTaskCommand(this.activeDatabase(), dto)
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
        this.editedExternalReplication().connectionStringName(connectionStringToUse);
    }
    
    onTestConnectionRaven(urlToTest: string) {
        eventsCollector.default.reportEvent("external-replication", "test-connection");
        this.spinners.test(true);
        this.newConnectionString().selectedUrlToTest(urlToTest);
        this.showError(false);

        this.newConnectionString()
            .testConnection(urlToTest)
            .done(result => this.testConnectionResult(result))
            .always(() => {
                this.spinners.test(false);
                this.newConnectionString().selectedUrlToTest("");
                this.showError(true);
            });
    }
}

export = editExternalReplicationTask;

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
import popoverUtils = require("common/popoverUtils");
import tasksCommonContent = require("models/database/tasks/tasksCommonContent");
import discoveryUrl = require("models/database/settings/discoveryUrl");

class editExternalReplicationTask extends viewModelBase {

    editedExternalReplication = ko.observable<ongoingTaskReplicationEditModel>();
    isAddingNewReplicationTask = ko.observable<boolean>(true);
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
            this.isAddingNewReplicationTask(false);
            this.taskId = args.taskId;

            ongoingTaskInfoCommand.forExternalReplication(this.activeDatabase(), this.taskId)
                .execute()
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication) => { 
                    this.editedExternalReplication(new ongoingTaskReplicationEditModel(result));
                    deferred.resolve();
                })
                .fail(() => {
                    deferred.reject();
                    
                    router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
                });
        } else {
            // 2. Creating a new task
            this.isAddingNewReplicationTask(true);
            this.editedExternalReplication(ongoingTaskReplicationEditModel.empty());
            deferred.resolve();
        }

        return $.when<any>(this.getAllConnectionStrings(), this.loadPossibleMentors(), deferred)
            .done(() => this.initObservables());
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
        
        const model = this.editedExternalReplication();

        this.newConnectionString(connectionStringRavenEtlModel.empty());
        
        this.newConnectionString().setNameUniquenessValidator(name => !this.ravenEtlConnectionStringsDetails().find(x => x.Name.toLocaleLowerCase() === name.toLocaleLowerCase()));
        
        const connectionStringName = this.editedExternalReplication().connectionStringName();
        const connectionStringIsMissing = connectionStringName && !this.ravenEtlConnectionStringsDetails()
            .find(x => x.Name.toLocaleLowerCase() === connectionStringName.toLocaleLowerCase());

        if (!this.ravenEtlConnectionStringsDetails().length || connectionStringIsMissing) {
            this.createNewConnectionString(true);
        }

        if (connectionStringIsMissing) {
            // looks like user imported data w/o connection strings, prefill form with desired name
            this.newConnectionString().connectionStringName(connectionStringName);
            this.editedExternalReplication().connectionStringName(null);
        }
        
        // Discard test connection result when needed
        this.createNewConnectionString.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().topologyDiscoveryUrls.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().inputUrl().discoveryUrlName.subscribe(() => this.testConnectionResult(null));

        this.dirtyFlag = new ko.DirtyFlag([
            model.taskName,
            model.manualChooseMentor,
            model.mentorNode,
            model.connectionStringName,
            model.delayReplicationTime,
            model.showDelayReplication,
            this.createNewConnectionString,
            this.newConnectionString().dirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    compositionComplete() {
        super.compositionComplete();
        document.getElementById('taskName').focus();
        
        $('.edit-replication-task [data-toggle="tooltip"]').tooltip();

        popoverUtils.longWithHover($(".responsible-node"),
            {
                content: tasksCommonContent.responsibleNodeInfo
            });
    }

    saveExternalReplication() {
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
                this.editedExternalReplication().connectionStringName(this.newConnectionString().connectionStringName());
            }
        }

        // 2. Validate *general form*
        if (!this.isValid(this.editedExternalReplication().validationGroup)) {
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
            const dto = this.editedExternalReplication().toDto(this.taskId);
            this.taskId = this.isAddingNewReplicationTask() ? 0 : this.taskId;

            eventsCollector.default.reportEvent("external-replication", "save");
                        
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
    
    onTestConnectionRaven(urlToTest: discoveryUrl) {
        eventsCollector.default.reportEvent("external-replication", "test-connection");
        this.spinners.test(true);
        this.newConnectionString().selectedUrlToTest(urlToTest.discoveryUrlName());
        this.testConnectionResult(null);

        this.newConnectionString()
            .testConnection(urlToTest)
            .done(result => this.testConnectionResult(result))
            .always(() => {
                this.spinners.test(false);
                this.fullErrorDetailsVisible(false);
            });
    }
}

export = editExternalReplicationTask;

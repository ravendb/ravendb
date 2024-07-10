import appUrl = require("common/appUrl");
import router = require("plugins/router");
import saveExternalReplicationTaskCommand = require("commands/database/tasks/saveExternalReplicationTaskCommand");
import ongoingTaskReplicationEditModel = require("models/database/tasks/ongoingTaskReplicationEditModel");
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import eventsCollector = require("common/eventsCollector");
import generalUtils = require("common/generalUtils");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import connectionStringRavenEtlModel = require("models/database/settings/connectionStringRavenEtlModel");
import jsonUtil = require("common/jsonUtil");
import discoveryUrl = require("models/database/settings/discoveryUrl");
import shardViewModelBase from "viewmodels/shardViewModelBase";
import database from "models/resources/database";
import licenseModel from "models/auth/licenseModel";
import { EditExternalReplicationInfoHub } from "viewmodels/database/tasks/EditExternalReplicationInfoHub";
import { sortBy } from "common/typeUtils";
class editExternalReplicationTask extends shardViewModelBase {

    view = require("views/database/tasks/editExternalReplicationTask.html");
    connectionStringView = require("views/database/settings/connectionStringRaven.html");
    certificateUploadInfoForOngoingTasks = require("views/partial/certificateUploadInfoForOngoingTasks.html");
    taskResponsibleNodeSectionView = require("views/partial/taskResponsibleNodeSection.html");
    pinResponsibleNodeTextScriptView = require("views/partial/pinResponsibleNodeTextScript.html");

    editedExternalReplication = ko.observable<ongoingTaskReplicationEditModel>();
    isAddingNewReplicationTask = ko.observable<boolean>(true);
    private taskId: number = null;
    
    possibleMentors = ko.observableArray<string>([]);
    
    ravenEtlConnectionStringsDetails = ko.observableArray<Raven.Client.Documents.Operations.ETL.RavenConnectionString>([]);

    usingHttps = location.protocol === "https:";
    certificatesUrl = appUrl.forCertificates();
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
    
    hasExternalReplication = licenseModel.getStatusValue("HasExternalReplication");
    infoHubView: ReactInKnockout<typeof EditExternalReplicationInfoHub>

    constructor(db: database) {
        super(db);
        
        this.bindToCurrentInstance("useConnectionString", "onTestConnectionRaven", "setState");

        this.infoHubView = ko.pureComputed(() => ({
            component: EditExternalReplicationInfoHub
        }))
    }

    activate(args: any) { 
        super.activate(args);
        const deferred = $.Deferred<void>();

        this.loadPossibleMentors();
        
        if (args.taskId) {
            // 1. Editing an existing task
            this.isAddingNewReplicationTask(false);
            this.taskId = args.taskId;

            ongoingTaskInfoCommand.forExternalReplication(this.db, this.taskId)
                .execute()
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication) => { 
                    this.editedExternalReplication(new ongoingTaskReplicationEditModel(result));
                    deferred.resolve();
                })
                .fail(() => {
                    deferred.reject();
                    
                    router.navigate(appUrl.forOngoingTasks(this.db));
                });
        } else {
            // 2. Creating a new task
            this.isAddingNewReplicationTask(true);
            this.editedExternalReplication(ongoingTaskReplicationEditModel.empty());
            deferred.resolve();
        }

        return $.when<any>(this.getAllConnectionStrings(), deferred)
            .done(() => this.initObservables());
    }

    private loadPossibleMentors() {
        const members = this.db.nodes()
            .filter(x => x.type === "Member")
            .map(x => x.tag);

        this.possibleMentors(members);
    }

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.db)
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const connectionStrings = (<any>Object).values(result.RavenConnectionStrings);
                this.ravenEtlConnectionStringsDetails(sortBy(connectionStrings, x => x.Name.toUpperCase()));
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
            model.taskState,
            model.manualChooseMentor,
            model.pinMentorNode,
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
        if (this.hasExternalReplication) {
            document.getElementById('taskName').focus();
        }
        
        $('.edit-replication-task [data-toggle="tooltip"]').tooltip();
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
        const savingNewStringAction = $.Deferred<void>();
        if (this.createNewConnectionString()) {
            this.newConnectionString()
                .saveConnectionString(this.db)
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
                        
            new saveExternalReplicationTaskCommand(this.db, dto)
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
        router.navigate(appUrl.forOngoingTasks(this.db));
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

    setState(state: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState): void {
        this.editedExternalReplication().taskState(state);
    }
}

export = editExternalReplicationTask;

import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import serverWideExternalReplicationEditModel = require("models/database/tasks/serverWide/serverWideExternalReplicationEditModel");
import getServerWideTaskInfoCommand = require("commands/serverWide/tasks/getServerWideTaskInfoCommand");
import eventsCollector = require("common/eventsCollector");
import saveServerWideExternalReplicationCommand = require("commands/serverWide/tasks/saveServerWideExternalReplicationCommand");
import connectionStringRavenEtlModel = require("models/database/settings/connectionStringRavenEtlModel");
import generalUtils = require("common/generalUtils");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import popoverUtils = require("common/popoverUtils");
import tasksCommonContent = require("models/database/tasks/tasksCommonContent");
import discoveryUrl = require("models/database/settings/discoveryUrl");

class editServerWideExternalReplication extends viewModelBase {
    
    editedTask = ko.observable<serverWideExternalReplicationEditModel>();
    isAddingNewExternalReplicationTask = ko.observable<boolean>(true);

    connectionStringForTest = ko.observable<connectionStringRavenEtlModel>();
    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;

    possibleMentors = ko.observableArray<string>([]);

    spinners = {
        test: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false)
    };
    
    constructor() {
        super();
        this.bindToCurrentInstance("onTestConnection");
    }
    
    activate(args: any) {
        super.activate(args);
        
        const replicationLoader = () => {
            const deferred = $.Deferred<void>();

            if (args && args.taskName) {
                // 1. Editing an existing task
                this.isAddingNewExternalReplicationTask(false);

                getServerWideTaskInfoCommand.forExternalReplication(args.taskName)
                    .execute()
                    .done((result: Raven.Server.Web.System.ServerWideTasksResult<Raven.Client.ServerWide.Operations.OngoingTasks.ServerWideExternalReplication>) => {
                        if (result.Results.length) {
                            const replicationTask = result.Results[0];
                            this.editedTask(new serverWideExternalReplicationEditModel(replicationTask));
                            deferred.resolve();
                        } else {
                            deferred.reject();
                            router.navigate(appUrl.forServerWideTasks());
                        }
                    })
                    .fail(() => {
                        deferred.reject();
                        router.navigate(appUrl.forServerWideTasks());
                    });
            } else {
                // 2. Creating a new task
                this.isAddingNewExternalReplicationTask(true);
                
                this.editedTask(serverWideExternalReplicationEditModel.empty());
                deferred.resolve();
            }

            return deferred
                .then(() => {
                    this.initObservables();
                });
        };

        return replicationLoader();
    }

    compositionComplete() {
        super.compositionComplete();
        
        $('.edit-server-wide-replication [data-toggle="tooltip"]').tooltip();

        popoverUtils.longWithHover($(".responsible-node"),
            {
                content: tasksCommonContent.responsibleNodeInfo
            });
    }
   
    private initObservables() {
        this.connectionStringForTest(connectionStringRavenEtlModel.empty());

        this.possibleMentors(clusterTopologyManager.default.topology().nodes().map(x => x.tag()));

        this.editedTask().connectionString().topologyDiscoveryUrls.subscribe(() => this.testConnectionResult(null));
        this.editedTask().connectionString().inputUrl().discoveryUrlName.subscribe(() => this.testConnectionResult(null));
        
        this.shortErrorText = ko.pureComputed(() => {
            const result = this.testConnectionResult();
            if (!result || result.Success) {
                return "";
            }
            return generalUtils.trimMessage(result.Error);
        });

        this.dirtyFlag = this.editedTask().dirtyFlag;
    }
    
    saveServerWideExternalReplication() {
        if (!this.validate()) {
            return;
        }

        this.spinners.save(true);
        eventsCollector.default.reportEvent("server-wide-external-replication", "save");
        
        const dto = this.editedTask().toDto();
        new saveServerWideExternalReplicationCommand(dto)
            .execute()
            .done(() => {
                this.dirtyFlag().reset();
                this.goToServerWideTasksView();
            })
            .always(() => this.spinners.save(false));
    }
    
    cancelOperation() {
        this.goToServerWideTasksView();
    }

    private goToServerWideTasksView() {
        router.navigate(appUrl.forServerWideTasks()); 
    }

    private validate(): boolean {
        return this.isValid(this.editedTask().validationGroup);
    }

    onTestConnection(urlToTest: discoveryUrl) {
        eventsCollector.default.reportEvent("external-replication", "test-connection");
        this.spinners.test(true);
        this.connectionStringForTest().selectedUrlToTest(urlToTest.discoveryUrlName());
        this.testConnectionResult(null);

        this.connectionStringForTest()
            .testConnection(urlToTest)
            .done(result => this.testConnectionResult(result))
            .always(() => {
                this.spinners.test(false);
                this.fullErrorDetailsVisible(false);
            });
    }
}

export = editServerWideExternalReplication;

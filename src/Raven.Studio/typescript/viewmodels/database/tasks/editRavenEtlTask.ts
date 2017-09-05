import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import ongoingTaskRavenEtl = require("models/database/tasks/ongoingTaskRavenEtlModel");
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import eventsCollector = require("common/eventsCollector");
import testClusterNodeConnectionCommand = require("commands/database/cluster/testClusterNodeConnectionCommand");
import getConnectionStringInfoCommand = require("commands/database/settings/getConnectionStringInfoCommand");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import saveRavenEtlTaskCommand = require("commands/database/tasks/saveRavenEtlTaskCommand");
import generalUtils = require("common/generalUtils");

class editRavenEtlTask extends viewModelBase {

    editedRavenEtl = ko.observable<ongoingTaskRavenEtl>();
    isAddingNewRavenEtlTask = ko.observable<boolean>(true);
    ravenEtlConnectionStringsNames = ko.observableArray<string>([]);
    private taskId: number = null;

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    spinners = { test: ko.observable<boolean>(false) };
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;

    constructor() {
        super();
        this.bindToCurrentInstance("useConnectionString","testConnection");
    }

    activate(args: any) {
        super.activate(args);
        const deferred = $.Deferred<void>();

        if (args.taskId) {

            // 1. Editing an Existing task
            this.isAddingNewRavenEtlTask(false);
            this.taskId = args.taskId;
            
            new ongoingTaskInfoCommand(this.activeDatabase(), "RavenEtl", args.taskId, args.taskName)
                .execute()
                .done((result: Raven.Client.ServerWide.Operations.OngoingTaskRavenEtl) => {
                    this.editedRavenEtl(new ongoingTaskRavenEtl(result, false));
                    deferred.resolve();
                })
                .fail(() => router.navigate(appUrl.forOngoingTasks(this.activeDatabase())));
        }
        else {
            // 2. Creating a New task
            this.isAddingNewRavenEtlTask(true);
            this.editedRavenEtl(ongoingTaskRavenEtl.empty());
            deferred.resolve();
        }

        deferred.always(() => this.initObservables());
        return $.when<any>(this.getAllConnectionStrings(), deferred); 
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
        // Discard test connection result when connection string has changed
        this.editedRavenEtl().connectionStringName.subscribe(() => this.testConnectionResult(null));

        this.shortErrorText = ko.pureComputed(() => {
            const result = this.testConnectionResult();
            if (!result || result.Success) {
                return "";
            }
            return generalUtils.trimMessage(result.Error);
        });
    }

    useConnectionString(connectionStringToUse: string) {
        this.editedRavenEtl().connectionStringName(connectionStringToUse);
      
        return getConnectionStringInfoCommand.forRavenEtl(this.activeDatabase(), connectionStringToUse)
            .execute()
            .done((result: Raven.Client.ServerWide.ETL.RavenConnectionString) => {
                this.editedRavenEtl().destinationURL(result.Url);
                this.editedRavenEtl().destinationDB(result.Database);
            });
    }

    testConnection() {
        if (this.editedRavenEtl().connectionStringName) {
            if (this.isValid(this.editedRavenEtl().connectionStringName)) {
                eventsCollector.default.reportEvent("ravenDB-ETL-connection-string", "test-connection"); // TODO: do we really need this ?

                this.spinners.test(true);

                new testClusterNodeConnectionCommand(this.editedRavenEtl().destinationURL())
                    .execute()
                    .done(result => this.testConnectionResult(result))
                    .always(() => this.spinners.test(false));
            }
        }
    }

    saveRavenEtl() {
        // 1. Validate model
        if (!this.validate()) {
            return;
        }

        // 2. Create/add the new raven-etl task
        const dto = this.editedRavenEtl().toDto();
        
        new saveRavenEtlTaskCommand(this.activeDatabase(), this.taskId, dto)
            .execute()
            .done(() => {
                this.goToOngoingTasksView();
            });
    }

    private validate(): boolean {
        let valid = true;

        if (!this.isValid(this.editedRavenEtl().validationGroup))
            valid = false;

        return valid;
    }

    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
    }
}

export = editRavenEtlTask;

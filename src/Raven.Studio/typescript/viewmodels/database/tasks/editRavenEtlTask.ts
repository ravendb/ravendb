import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import ongoingTaskRavenEtlEditModel = require("models/database/tasks/ongoingTaskRavenEtlEditModel");
import getOngoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import eventsCollector = require("common/eventsCollector");
import testClusterNodeConnectionCommand = require("commands/database/cluster/testClusterNodeConnectionCommand");
import getConnectionStringInfoCommand = require("commands/database/settings/getConnectionStringInfoCommand");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import saveRavenEtlTaskCommand = require("commands/database/tasks/saveRavenEtlTaskCommand");
import generalUtils = require("common/generalUtils");
import ongoingTaskEtlTransformationModel = require("models/database/tasks/ongoingTaskEtlTransformationModel");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import deleteTransformationScriptConfirm = require("viewmodels/database/tasks/deleteTransformationScriptConfirm");
import transformationScriptSyntax = require("viewmodels/database/tasks/transformationScriptSyntax");
import getPossibleMentorsCommand = require("commands/database/tasks/getPossibleMentorsCommand");

class editRavenEtlTask extends viewModelBase {

    editedRavenEtl = ko.observable<ongoingTaskRavenEtlEditModel>();
    isAddingNewRavenEtlTask = ko.observable<boolean>(true);
    ravenEtlConnectionStringsNames = ko.observableArray<string>([]);
    private taskId: number = null;

    possibleMentors = ko.observableArray<string>([]);

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    spinners = { test: ko.observable<boolean>(false) };
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;
    
    connectionStringsUrl = appUrl.forCurrentDatabase().connectionStrings();

    collections = collectionsTracker.default.collections;

    constructor() {
        super();
        this.bindToCurrentInstance("useConnectionString", "testConnection", "confirmRemoveTransformationScript", "cancelEditedTransformation", "saveEditedTransformation", "syntaxHelp");
    }

    activate(args: any) {
        super.activate(args);
        const deferred = $.Deferred<void>();

        if (args.taskId) {
            // 1. Editing an Existing task
            this.isAddingNewRavenEtlTask(false);
            this.taskId = args.taskId;
            
            getOngoingTaskInfoCommand.forRavenEtl(this.activeDatabase(), args.taskId)
                .execute()
                .done((result: Raven.Client.ServerWide.Operations.OngoingTaskRavenEtlDetails) => {
                    this.editedRavenEtl(new ongoingTaskRavenEtlEditModel(result));
                    deferred.resolve();
                })
                .fail(() => { 
                    deferred.reject();
                    router.navigate(appUrl.forOngoingTasks(this.activeDatabase())); 
                });
        }
        else {
            // 2. Creating a New task
            this.isAddingNewRavenEtlTask(true);
            this.editedRavenEtl(ongoingTaskRavenEtlEditModel.empty());
            deferred.resolve();
        }

        deferred.always(() => {
            this.initObservables();
        });

        return $.when<any>(this.getAllConnectionStrings(), this.loadPossibleMentors(), deferred); 
    }

    private loadPossibleMentors() {
        return new getPossibleMentorsCommand(this.activeDatabase().name)
            .execute()
            .done(mentors => this.possibleMentors(mentors));
    }
    
    compositionComplete() {
        super.compositionComplete();

        $('.edit-raven-etl-task [data-toggle="tooltip"]').tooltip();
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

        this.dirtyFlag = new ko.DirtyFlag([this.editedRavenEtl().isDirtyEditedScript().isDirty, this.editedRavenEtl().transformationScripts]);
    }

    useConnectionString(connectionStringToUse: string) {
        this.editedRavenEtl().connectionStringName(connectionStringToUse);
    }

    testConnection() {
        if (this.editedRavenEtl().connectionStringName) {
            // 1. Input connection string name is pre-defined
            eventsCollector.default.reportEvent("ravenDB-ETL-connection-string", "test-connection");
            this.spinners.test(true);

            getConnectionStringInfoCommand.forRavenEtl(this.activeDatabase(), this.editedRavenEtl().connectionStringName())
                .execute()
                .done((result: Raven.Client.ServerWide.ETL.RavenConnectionString) => {
                    new testClusterNodeConnectionCommand(result.Url)
                        .execute()
                        .done(result => this.testConnectionResult(result))
                        .always(() => this.spinners.test(false));
                }
            );
        }
    }

    trySaveRavenEtl() {
        if (!this.editedRavenEtl().isDirtyEditedScript().isDirty()) {
            this.saveRavenEtl();
            return true;
        }

        const deferredResult = $.Deferred<boolean>();
        this.discardStayResult().done((result: confirmDialogResult) => {
            if (!result.can) {
                // Keep changes & stay
                return;
            } else {
                // Discard changes & add save the Raven Etl model
                this.saveRavenEtl();
            }

            deferredResult.resolve(result.can);
        });

        return deferredResult;
    }

    saveRavenEtl() {
        // 1. Validate model
        if (!this.isValid(this.editedRavenEtl().validationGroup)) {
            return;
        }

        // 2. Create/add the new raven-etl task
        const dto = this.editedRavenEtl().toDto(this.taskId);
        
        new saveRavenEtlTaskCommand(this.activeDatabase(), this.taskId, dto)
            .execute()
            .done(() => {
                this.editedRavenEtl().isDirtyEditedScript().reset();
                this.dirtyFlag().reset();
                this.goToOngoingTasksView();
            });
    }

    tryAddNewTransformation() {
        if (!this.editedRavenEtl().isDirtyEditedScript().isDirty()) {
            this.addNewTransformation();
            return true;
        }

        const deferredResult = $.Deferred<boolean>();
        this.discardStayResult().done((result: confirmDialogResult) => {
            if (!result.can) {
                // Keep changes & stay
                return; 
            } else {
                // Discard changes & add new transformation
                this.addNewTransformation();
            }

            deferredResult.resolve(result.can);
        });

        return deferredResult;
    }

    private addNewTransformation() {
        this.editedRavenEtl().showEditTransformationArea(false);
        this.editedRavenEtl().editedTransformationScript().update(ongoingTaskEtlTransformationModel.empty().toDto(), true);

        this.editedRavenEtl().showEditTransformationArea(true);
        this.editedRavenEtl().isDirtyEditedScript().reset();    
    }

    cancelEditedTransformation() {
        this.editedRavenEtl().showEditTransformationArea(false);
        this.editedRavenEtl().isDirtyEditedScript().reset();
    }

    saveEditedTransformation(transformation: ongoingTaskEtlTransformationModel) {
        // 1. Validate
        if (!this.isValid(this.editedRavenEtl().editedTransformationScript().validationGroup)) {
            return;
        }

        // 2. Save
        if (transformation.isNew()) {
            let newTransformationItem = new ongoingTaskEtlTransformationModel({
                ApplyToAllDocuments: transformation.applyScriptForAllCollections(),
                Collections: transformation.transformScriptCollections(),
                Disabled: false,
                HasLoadAttachment: false,
                Name: transformation.name(),
                Script: transformation.script()
            }, true);

            this.editedRavenEtl().transformationScripts.push(newTransformationItem); 
        }
        else {
            let item = this.editedRavenEtl().transformationScripts().find(x => x.name() === transformation.name());
            item.applyScriptForAllCollections(transformation.applyScriptForAllCollections());
            item.transformScriptCollections(transformation.transformScriptCollections());
            item.script(transformation.script());
        }

        // 3. Sort
        this.editedRavenEtl().transformationScripts.sort((a, b) => a.name().toLowerCase().localeCompare(b.name().toLowerCase()));

        // 4. Clear
        this.editedRavenEtl().showEditTransformationArea(false);
        this.editedRavenEtl().isDirtyEditedScript().reset();
    }

    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
    }

    createCollectionNameAutocompleter(usedCollections: KnockoutObservableArray<string>, collectionText: KnockoutObservable<string>) {
        return ko.pureComputed(() => {
            const key = collectionText();

            const options = this.collections().filter(x => !x.isAllDocuments).map(x => x.name);

            const usedOptions = usedCollections().filter(k => k !== key);

            const filteredOptions = _.difference(options, usedOptions);

            if (key) {
                return filteredOptions.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                return filteredOptions;
            }
        });
    }

    confirmRemoveTransformationScript(model: ongoingTaskEtlTransformationModel) {
        const db = this.activeDatabase();

        const confirmDeleteViewModel = new deleteTransformationScriptConfirm(db, model.name()); 
        app.showBootstrapDialog(confirmDeleteViewModel);
        confirmDeleteViewModel.result.done(result => {
            if (result.can) {
                this.editedRavenEtl().deleteTransformationScript(model);
            }
        });
    }

    syntaxHelp() {
        const viewmodel = new transformationScriptSyntax();
        app.showBootstrapDialog(viewmodel);
    }
}

export = editRavenEtlTask;

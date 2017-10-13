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
import saveEtlTaskCommand = require("commands/database/tasks/saveEtlTaskCommand");
import generalUtils = require("common/generalUtils");
import ongoingTaskRavenEtlTransformationModel = require("models/database/tasks/ongoingTaskRavenEtlTransformationModel");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import transformationScriptSyntax = require("viewmodels/database/tasks/transformationScriptSyntax");
import getPossibleMentorsCommand = require("commands/database/tasks/getPossibleMentorsCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

class editRavenEtlTask extends viewModelBase {

    editedRavenEtl = ko.observable<ongoingTaskRavenEtlEditModel>();
    isAddingNewRavenEtlTask = ko.observable<boolean>(true);
    ravenEtlConnectionStringsNames = ko.observableArray<string>([]);

    possibleMentors = ko.observableArray<string>([]);

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    spinners = { 
        test: ko.observable<boolean>(false) 
    };
    
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;
    
    connectionStringsUrl = appUrl.forCurrentDatabase().connectionStrings();

    collections = collectionsTracker.default.collections;

    constructor() {
        super();
        
        aceEditorBindingHandler.install();
        this.bindToCurrentInstance("useConnectionString", "testConnection", "removeTransformationScript", "cancelEditedTransformation", "saveEditedTransformation", "syntaxHelp");
    }

    activate(args: any) {
        super.activate(args);
        const deferred = $.Deferred<void>();

        if (args.taskId) {
            // 1. Editing an Existing task
            this.isAddingNewRavenEtlTask(false);            
            
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
            this.editedRavenEtl().showEditTransformationArea(true);
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

    trySaveRavenEtl() {
        const editedEtl = this.editedRavenEtl();

        // show validations errors if any - ignore returned value
        this.isValid(this.editedRavenEtl().validationGroup);
        
        if (editedEtl.showEditTransformationArea()) {
            if (!this.isValid(editedEtl.editedTransformationScript().validationGroup)) {
                return true;
            }

            this.saveEditedTransformation(editedEtl.editedTransformationScript());
        }
        
        this.saveRavenEtl();
    }

    saveRavenEtl() {
        // 1. Validate model
        if (!this.isValid(this.editedRavenEtl().validationGroup)) {
            return;
        }

        // 2. Create/add the new raven-etl task
        const dto = this.editedRavenEtl().toDto();
        saveEtlTaskCommand.forRavenEtl(this.activeDatabase(), dto)
            .execute()
            .done(() => {
                this.editedRavenEtl().isDirtyEditedScript().reset();
                this.dirtyFlag().reset();
                this.goToOngoingTasksView();
            });
    }

    addNewTransformation() {
        this.editedRavenEtl().showEditTransformationArea(false);
        this.editedRavenEtl().editedTransformationScript().update(ongoingTaskRavenEtlTransformationModel.empty().toDto(), true);

        this.editedRavenEtl().showEditTransformationArea(true);
        this.editedRavenEtl().isDirtyEditedScript().reset();    
    }

    cancelEditedTransformation() {
        this.editedRavenEtl().showEditTransformationArea(false);
        this.editedRavenEtl().isDirtyEditedScript().reset();
    }

    saveEditedTransformation(transformation: ongoingTaskRavenEtlTransformationModel) {
        // 1. Validate
        if (!this.isValid(transformation.validationGroup)) {
            return;
        }

        // 2. Save
        if (transformation.isNew()) {
            let newTransformationItem = new ongoingTaskRavenEtlTransformationModel({
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

    removeTransformationScript(model: ongoingTaskRavenEtlTransformationModel) {
        this.editedRavenEtl().deleteTransformationScript(model);
    }

    syntaxHelp() {
        const viewmodel = new transformationScriptSyntax("Raven");
        app.showBootstrapDialog(viewmodel);
    }
}

export = editRavenEtlTask;

import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import getOngoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import eventsCollector = require("common/eventsCollector");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import saveEtlTaskCommand = require("commands/database/tasks/saveEtlTaskCommand");
import generalUtils = require("common/generalUtils");
import ongoingTaskRavenEtlEditModel = require("models/database/tasks/ongoingTaskRavenEtlEditModel");
import ongoingTaskRavenEtlTransformationModel = require("models/database/tasks/ongoingTaskRavenEtlTransformationModel");
import connectionStringRavenEtlModel = require("models/database/settings/connectionStringRavenEtlModel");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import transformationScriptSyntax = require("viewmodels/database/tasks/transformationScriptSyntax");
import getPossibleMentorsCommand = require("commands/database/tasks/getPossibleMentorsCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import jsonUtil = require("common/jsonUtil");
import {RelativeTimeSpec} from "../../../../wwwroot/lib/moment/moment";

class editRavenEtlTask extends viewModelBase {
    
    static readonly scriptNamePrefix = "Script #";

    editedRavenEtl = ko.observable<ongoingTaskRavenEtlEditModel>();
    isAddingNewRavenEtlTask = ko.observable<boolean>(true);
    ravenEtlConnectionStringsNames = ko.observableArray<string>([]);

    possibleMentors = ko.observableArray<string>([]);

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    
    spinners = {
        test: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false)
    };
    
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;
    showError = ko.observable<boolean>(true);
    
    connectionStringsUrl = appUrl.forCurrentDatabase().connectionStrings();
    createNewConnectionString = ko.observable<boolean>(false);
    newConnectionString = ko.observable<connectionStringRavenEtlModel>();

    collections = collectionsTracker.default.collections;

    constructor() {
        super();
        
        aceEditorBindingHandler.install();
        this.bindToCurrentInstance("useConnectionString", "onTestConnectionRaven", "removeTransformationScript", "cancelEditedTransformation", "saveEditedTransformation", "syntaxHelp"); //testConnection
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
        } else {
            // 2. Creating a New task
            this.isAddingNewRavenEtlTask(true);
            this.editedRavenEtl(ongoingTaskRavenEtlEditModel.empty());
            this.editedRavenEtl().editedTransformationScriptSandbox(ongoingTaskRavenEtlTransformationModel.empty());
            deferred.resolve();
        }

        deferred.done(() => {
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
        this.shortErrorText = ko.pureComputed(() => {
            const result = this.testConnectionResult();
            if (!result || result.Success) {
                return "";
            }
            return generalUtils.trimMessage(result.Error);
        });

        this.dirtyFlag = new ko.DirtyFlag([           
            this.createNewConnectionString,
            this.editedRavenEtl().dirtyFlag
        ], false, jsonUtil.newLineNormalizingHashFunction);
        
        this.newConnectionString(connectionStringRavenEtlModel.empty());
        
        // Discard test connection result when needed
        this.createNewConnectionString.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().inputUrl().discoveryUrlName.subscribe(() => this.testConnectionResult(null));        
    }

    useConnectionString(connectionStringToUse: string) {
        this.editedRavenEtl().connectionStringName(connectionStringToUse);
    }

    onTestConnectionRaven(urlToTest: string) {
        eventsCollector.default.reportEvent("ravenDB-ETL-connection-string", "test-connection");
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

    saveRavenEtl() {
        let hasAnyErrors = false;
        this.spinners.save(true);        
        let editedEtl = this.editedRavenEtl();

        // 1. Validate *edited transformation script*
        if (editedEtl.showEditTransformationArea()) {
            if (!this.isValid(editedEtl.editedTransformationScriptSandbox().validationGroup)) {
                hasAnyErrors = true;
            } else {
                this.saveEditedTransformation();
            }
        }
        
        // 2. Validate *new connection string* (if relevant..)       
        if (this.createNewConnectionString()) {
            if (!this.isValid(this.newConnectionString().validationGroup)) {
                hasAnyErrors = true;
            }
            else {
                // Use the new connection string
                editedEtl.connectionStringName(this.newConnectionString().connectionStringName());               
            }
        }      

        // 3. Validate *general form*               
        if (!this.isValid(editedEtl.validationGroup)) {
            hasAnyErrors = true;
        }

        if (hasAnyErrors) {
            this.spinners.save(false);
            return false;
        }

        // 5. All is well, Save connection string (if relevant..) 
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

        // 6. All is well, Save Raven Etl task
        const dto = editedEtl.toDto();
        saveEtlTaskCommand.forRavenEtl(this.activeDatabase(), dto)
            .execute()
            .done(() => {
                editedEtl.dirtyFlag().reset();
                this.goToOngoingTasksView();
            })
            .always(() => this.spinners.save(false));
    }

    addNewTransformation() {
        this.editedRavenEtl().transformationScriptSelectedForEdit(null);
        this.editedRavenEtl().editedTransformationScriptSandbox(ongoingTaskRavenEtlTransformationModel.empty());
    }

    cancelEditedTransformation() {
        this.editedRavenEtl().editedTransformationScriptSandbox(null);
        this.editedRavenEtl().transformationScriptSelectedForEdit(null);
    }

    saveEditedTransformation() {
        const transformation = this.editedRavenEtl().editedTransformationScriptSandbox();
        if (!this.isValid(transformation.validationGroup)) {
            return;
        }
        
        if (transformation.isNew()) {
            const newTransformationItem = new ongoingTaskRavenEtlTransformationModel(transformation.toDto(), false);
            newTransformationItem.name(this.findNameForNewTransformation());
            newTransformationItem.dirtyFlag().forceDirty();
            this.editedRavenEtl().transformationScripts.push(newTransformationItem);
        } else {
            const oldItem = this.editedRavenEtl().transformationScriptSelectedForEdit();
            const newItem = new ongoingTaskRavenEtlTransformationModel(transformation.toDto(), false);
            
            if (oldItem.dirtyFlag().isDirty() || newItem.hasUpdates(oldItem)) {
                newItem.dirtyFlag().forceDirty();
            }
            
            this.editedRavenEtl().transformationScripts.replace(oldItem, newItem);
        }

        this.editedRavenEtl().transformationScripts.sort((a, b) => a.name().toLowerCase().localeCompare(b.name().toLowerCase()));
        this.editedRavenEtl().editedTransformationScriptSandbox(null);
        this.editedRavenEtl().transformationScriptSelectedForEdit(null);
    }
    
    private findNameForNewTransformation() {
        const scriptsWithPrefix = this.editedRavenEtl().transformationScripts().filter(script => {
            return script.name().startsWith(editRavenEtlTask.scriptNamePrefix);
        });
        
        const maxNumber =  _.max(scriptsWithPrefix
            .map(x => x.name().substr(editRavenEtlTask.scriptNamePrefix.length))
            .map(x => _.toInteger(x))) || 0;
        
        return editRavenEtlTask.scriptNamePrefix + (maxNumber + 1);
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

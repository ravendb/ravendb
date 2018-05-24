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
import ongoingTaskEtlTransformationModel = require("models/database/tasks/ongoingTaskRavenEtlTransformationModel");

class editRavenEtlTask extends viewModelBase {
    
    static readonly scriptNamePrefix = "Script #";
    static isApplyToAll = ongoingTaskRavenEtlTransformationModel.isApplyToAll;

    editedRavenEtl = ko.observable<ongoingTaskRavenEtlEditModel>();
    isAddingNewRavenEtlTask = ko.observable<boolean>(true);
    
    ravenEtlConnectionStringsDetails = ko.observableArray<Raven.Client.Documents.Operations.ETL.RavenConnectionString>([]);

    possibleMentors = ko.observableArray<string>([]);

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    
    spinners = {
        test: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false)
    };
    
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;
    
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
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlDetails) => {
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

        this.dirtyFlag = new ko.DirtyFlag([           
            this.createNewConnectionString,
            this.editedRavenEtl().dirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
        
        this.newConnectionString(connectionStringRavenEtlModel.empty());
        
        // Open the 'Create new conn. str.' area if no connection strings are yet defined 
        this.ravenEtlConnectionStringsDetails.subscribe((value) => { this.createNewConnectionString(!value.length) });

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
        this.testConnectionResult(null);

        this.newConnectionString()
            .testConnection(urlToTest)
            .done(result => this.testConnectionResult(result))
            .always(() => {
                this.spinners.test(false);
                this.newConnectionString().selectedUrlToTest(null);                
            });
    }

    saveRavenEtl() {
        let hasAnyErrors = false;
        this.spinners.save(true);        
        let editedEtl = this.editedRavenEtl();

        // 0. Save discovery URL if user forgot to hit 'add url' button
        if (this.createNewConnectionString() && 
            this.newConnectionString().inputUrl().discoveryUrlName() &&
            this.isValid(this.newConnectionString().inputUrl().validationGroup)) {
                this.newConnectionString().addDiscoveryUrlWithBlink();
        }
        
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

        // 4. All is well, Save connection string (if relevant..) 
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
        }
        else {
            savingNewStringAction.resolve();
        }

        // 5. All is well, Save Raven Etl task
        savingNewStringAction.done(()=> {
            const scriptsToReset = editedEtl.transformationScripts().filter(x => x.resetScript()).map(x => x.name());

            const dto = editedEtl.toDto();
            saveEtlTaskCommand.forRavenEtl(this.activeDatabase(), dto, scriptsToReset)
                .execute()
                .done(() => {
                    this.dirtyFlag().reset();
                    this.goToOngoingTasksView();
                })
                .always(() => this.spinners.save(false));
        });
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
            const newTransformationItem = new ongoingTaskRavenEtlTransformationModel(transformation.toDto(), true, false); 
            newTransformationItem.name(this.findNameForNewTransformation());
            newTransformationItem.dirtyFlag().forceDirty();
            this.editedRavenEtl().transformationScripts.push(newTransformationItem);
        } else {
            const oldItem = this.editedRavenEtl().transformationScriptSelectedForEdit();
            const newItem = new ongoingTaskRavenEtlTransformationModel(transformation.toDto(), false, transformation.resetScript());
            
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
            let result;            
            const key = collectionText();

            const options = this.collections().filter(x => !x.isAllDocuments).map(x => x.name);

            const usedOptions = usedCollections().filter(k => k !== key);

            const filteredOptions = _.difference(options, usedOptions);

            if (key) {
                result = filteredOptions.filter(x => x.toLowerCase().includes(key.toLowerCase()));               
            } else {
                result = filteredOptions;
            }
            
            if (!_.includes(this.editedRavenEtl().editedTransformationScriptSandbox().transformScriptCollections(), ongoingTaskEtlTransformationModel.applyToAllCollectionsText)) {
                result.unshift(ongoingTaskEtlTransformationModel.applyToAllCollectionsText);
            }
            
            return result;
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

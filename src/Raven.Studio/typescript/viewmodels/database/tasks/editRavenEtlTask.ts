import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import database = require("models/resources/database");
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
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import jsonUtil = require("common/jsonUtil");
import ongoingTaskEtlTransformationModel = require("models/database/tasks/ongoingTaskRavenEtlTransformationModel");
import viewHelpers = require("common/helpers/view/viewHelpers");
import documentMetadata = require("models/database/documents/documentMetadata");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import document = require("models/database/documents/document");
import testRavenEtlCommand = require("commands/database/tasks/testRavenEtlCommand");

type resultItem = {
    header: string;
    payload: string;
}

class ravenTaskTestMode {
    documentId = ko.observable<string>();
    testDelete = ko.observable<boolean>(false);
    docsIdsAutocompleteResults = ko.observableArray<string>([]);
    db: KnockoutObservable<database>;
    configurationProvider: () => Raven.Client.Documents.Operations.ETL.RavenEtlConfiguration;

    validationGroup: KnockoutValidationGroup;
    validateParent: () => boolean;

    testAlreadyExecuted = ko.observable<boolean>(false);

    spinners = {
        preview: ko.observable<boolean>(false),
        test: ko.observable<boolean>(false)
    };

    loadedDocument = ko.observable<string>();
    loadedDocumentId = ko.observable<string>();

    testResults = ko.observableArray<resultItem>([]);
    debugOutput = ko.observableArray<string>([]);

    // all kinds of alerts:
    transformationErrors = ko.observableArray<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>([]);

    warningsCount = ko.pureComputed(() => {
        return this.transformationErrors().length;
    });

    constructor(db: KnockoutObservable<database>, validateParent: () => boolean, configurationProvider: () => Raven.Client.Documents.Operations.ETL.RavenEtlConfiguration) {
        this.db = db;
        this.validateParent = validateParent;
        this.configurationProvider = configurationProvider;

        _.bindAll(this, "onAutocompleteOptionSelected");
    }

    initObservables() {
        this.documentId.extend({
            required: true
        });

        this.documentId.throttle(250).subscribe(item => {
            if (!item) {
                return;
            }

            new getDocumentsMetadataByIDPrefixCommand(item, 10, this.db())
                .execute()
                .done(results => {
                    this.docsIdsAutocompleteResults(results.map(x => x["@metadata"]["@id"]));
                });
        });

        this.validationGroup = ko.validatedObservable({
            documentId : this.documentId
        });
    }

    onAutocompleteOptionSelected(option: string) {
        this.documentId(option);
        this.previewDocument();
    }
    
    previewDocument() {
        const spinner = this.spinners.preview;
        const documentId: KnockoutObservable<string> = this.documentId;
        const documentIdValidationGroup = this.validationGroup;
        const db = this.db;

        spinner(true);

        viewHelpers.asyncValidationCompleted(this.validationGroup)
            .then(() => {
                if (viewHelpers.isValid(this.validationGroup)) {
                    new getDocumentWithMetadataCommand(documentId(), this.db())
                        .execute()
                        .done((doc: document) => {
                            const docDto = doc.toDto(true);
                            const metaDto = docDto["@metadata"];
                            documentMetadata.filterMetadata(metaDto);
                            const text = JSON.stringify(docDto, null, 4);
                            this.loadedDocument(Prism.highlight(text, (Prism.languages as any).javascript));
                            this.loadedDocumentId(doc.getId());

                            $('.test-container a[href="#documentPreview"]').tab('show');
                        }).always(() => spinner(false));
                } else {
                    spinner(false);
                }
            });
    }
    
    
    runTest() {
        const testValid = viewHelpers.isValid(this.validationGroup, true);
        const parentValid = this.validateParent();

        if (testValid && parentValid) {
            this.spinners.test(true);

            const dto = {
                DocumentId: this.documentId(),
                IsDelete: this.testDelete(),
                Configuration: this.configurationProvider()
            } as Raven.Server.Documents.ETL.Providers.Raven.Test.TestRavenEtlScript;

            new testRavenEtlCommand(this.db(), dto)
                .execute()
                .done(simulationResult => {
                    this.testResults(simulationResult.Commands.map((command: Raven.Client.Documents.Commands.Batches.ICommandData) => {

                        const json = JSON.stringify(command, null, 4);
                        const html = Prism.highlight(json, (Prism.languages as any).javascript);
                        
                        return {
                            header: command.Type + " " + command.Id,
                            payload: html
                        } as resultItem;
                    }));
                    this.debugOutput(simulationResult.DebugOutput);
                    this.transformationErrors(simulationResult.TransformationErrors);

                    if (this.warningsCount()) {
                        $('.test-container a[href="#warnings"]').tab('show');
                    } else {
                        $('.test-container a[href="#testResults"]').tab('show');
                    }

                    this.testAlreadyExecuted(true);
                })
                .always(() => this.spinners.test(false));
        }
    }
}

class editRavenEtlTask extends viewModelBase {
    
    static readonly scriptNamePrefix = "Script #";
    static isApplyToAll = ongoingTaskRavenEtlTransformationModel.isApplyToAll;

    enableTestArea = ko.observable<boolean>(false);

    test: ravenTaskTestMode;
    
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
        this.bindToCurrentInstance("useConnectionString", "onTestConnectionRaven", "removeTransformationScript", "cancelEditedTransformation", "saveEditedTransformation", "syntaxHelp", "toggleTestArea");
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
        this.newConnectionString().setNameUniquenessValidator(name => !this.ravenEtlConnectionStringsDetails().find(x => x.Name.toLocaleLowerCase() === name.toLocaleLowerCase()));
        
        // Open the 'Create new conn. str.' area if no connection strings are yet defined 
        this.ravenEtlConnectionStringsDetails.subscribe((value) => { this.createNewConnectionString(!value.length) });

        // Discard test connection result when needed
        this.createNewConnectionString.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().inputUrl().discoveryUrlName.subscribe(() => this.testConnectionResult(null));

        this.enableTestArea.subscribe(testMode => {
            $("body").toggleClass('show-test', testMode);
        });

        const dtoProvider = () => {
            const dto = this.editedRavenEtl().toDto();

            // override transforms - use only current transformation
            const transformationScriptDto = this.editedRavenEtl().editedTransformationScriptSandbox().toDto();
            transformationScriptDto.Name = "Script #1"; // assign fake name
            dto.Transforms = [transformationScriptDto];

            if (!dto.Name) {
                dto.Name = "Test Raven ETL Task"; // assign fake name
            }
            return dto;
        };
        
        this.test = new ravenTaskTestMode(this.activeDatabase, () => {
            return this.isValid(this.editedRavenEtl().editedTransformationScriptSandbox().validationGroup);
        }, dtoProvider);

        this.test.initObservables();
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
            } else {
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
        } else {
            savingNewStringAction.resolve();
        }

        // 5. All is well, Save Raven Etl task
        savingNewStringAction.done(()=> {
            eventsCollector.default.reportEvent("raven-etl", "save");
            
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
        this.enableTestArea(false);
    }

    saveEditedTransformation() {
        this.enableTestArea(false);
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

    toggleTestArea() {
        if (!this.enableTestArea()) {             
            this.enableTestArea(true);
        } else {
            this.enableTestArea(false);
        }
    }
}

export = editRavenEtlTask;

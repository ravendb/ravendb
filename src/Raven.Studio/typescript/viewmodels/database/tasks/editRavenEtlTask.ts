import app = require("durandal/app");
import appUrl = require("common/appUrl");
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
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import jsonUtil = require("common/jsonUtil");
import viewHelpers = require("common/helpers/view/viewHelpers");
import documentMetadata = require("models/database/documents/documentMetadata");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import document = require("models/database/documents/document");
import testRavenEtlCommand = require("commands/database/tasks/testRavenEtlCommand");
import discoveryUrl = require("models/database/settings/discoveryUrl");
import { highlight, languages } from "prismjs";
import shardViewModelBase from "viewmodels/shardViewModelBase";
import licenseModel from "models/auth/licenseModel";
import { EditRavenEtlInfoHub } from "viewmodels/database/tasks/EditRavenEtlInfoHub";
import { sortBy } from "common/typeUtils";

type resultItem = {
    header: string;
    payload: string;
}

class ravenTaskTestMode {
    documentId = ko.observable<string>();
    testDelete = ko.observable<boolean>(false);
    docsIdsAutocompleteResults = ko.observableArray<string>([]);
    db: database;
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

    constructor(db: database,
                validateParent: () => boolean,
                configurationProvider: () => Raven.Client.Documents.Operations.ETL.RavenEtlConfiguration) {
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

            new getDocumentsMetadataByIDPrefixCommand(item, 10, this.db)
                .execute()
                .done(results => {
                    this.docsIdsAutocompleteResults(results.map(x => x["@metadata"]["@id"]));
                });
        });

        this.validationGroup = ko.validatedObservable({
            documentId: this.documentId
        });
    }

    onAutocompleteOptionSelected(option: string) {
        this.documentId(option);
        this.previewDocument();
    }
    
    previewDocument() {
        const spinner = this.spinners.preview;
        const documentId: KnockoutObservable<string> = this.documentId;

        spinner(true);

        viewHelpers.asyncValidationCompleted(this.validationGroup)
            .then(() => {
                if (viewHelpers.isValid(this.validationGroup)) {
                    new getDocumentWithMetadataCommand(documentId(), this.db)
                        .execute()
                        .done((doc: document) => {
                            const docDto = doc.toDto(true);
                            const metaDto = docDto["@metadata"];
                            documentMetadata.filterMetadata(metaDto);
                            const text = JSON.stringify(docDto, null, 4);
                            this.loadedDocument(highlight(text, languages.javascript, "js"));
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

            const dto: Raven.Server.Documents.ETL.Providers.Raven.Test.TestRavenEtlScript = {
                DocumentId: this.documentId(),
                IsDelete: this.testDelete(),
                Configuration: this.configurationProvider()
            };

            eventsCollector.default.reportEvent("ravendb-etl", "test-script");

            new testRavenEtlCommand(this.db, dto)
                .execute()
                .done(simulationResult => {
                    this.testResults(simulationResult.Commands.map((command: Raven.Client.Documents.Commands.Batches.ICommandData): resultItem => {

                        const json = JSON.stringify(command, null, 4);
                        const html = highlight(json, languages.javascript, "js");
                        
                        return {
                            header: command.Type + " " + command.Id,
                            payload: html
                        };
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

class editRavenEtlTask extends shardViewModelBase {

    view = require("views/database/tasks/editRavenEtlTask.html");
    connectionStringView = require("views/database/settings/connectionStringRaven.html")
    certificateUploadInfoForOngoingTasks = require("views/partial/certificateUploadInfoForOngoingTasks.html");
    taskResponsibleNodeSectionView = require("views/partial/taskResponsibleNodeSection.html");
    pinResponsibleNodeTextScriptView = require("views/partial/pinResponsibleNodeTextScript.html");
    
    static readonly scriptNamePrefix = "Script_";
    static isApplyToAll = ongoingTaskRavenEtlTransformationModel.isApplyToAll;

    enableTestArea = ko.observable<boolean>(false);
    showAdvancedOptions = ko.observable<boolean>(false);

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

    usingHttps = location.protocol === "https:";
    certificatesUrl = appUrl.forCertificates();

    hasRavenEtl = licenseModel.getStatusValue("HasRavenEtl");
    infoHubView: ReactInKnockout<typeof EditRavenEtlInfoHub>;

    constructor(db: database) {
        super(db);
        
        aceEditorBindingHandler.install();
        this.bindToCurrentInstance("useConnectionString", "onTestConnectionRaven", "removeTransformationScript",
                                   "cancelEditedTransformation", "saveEditedTransformation", "syntaxHelp",
                                   "toggleTestArea", "toggleAdvancedArea", "setState");
        this.infoHubView = ko.pureComputed(() => ({
            component: EditRavenEtlInfoHub
        }))
    }

    activate(args: any) {
        super.activate(args);
        const deferred = $.Deferred<void>();
        
        this.loadPossibleMentors();

        if (args.taskId) {
            // 1. Editing an Existing task
            this.isAddingNewRavenEtlTask(false);
            
            getOngoingTaskInfoCommand.forRavenEtl(this.db, args.taskId)
                .execute()
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtl) => {
                    this.editedRavenEtl(new ongoingTaskRavenEtlEditModel(result));
                    deferred.resolve();
                })
                .fail(() => { 
                    deferred.reject();
                    router.navigate(appUrl.forOngoingTasks(this.db)); 
                });
        } else {
            // 2. Creating a New task
            this.isAddingNewRavenEtlTask(true);
            this.editedRavenEtl(ongoingTaskRavenEtlEditModel.empty());
            this.editedRavenEtl().editedTransformationScriptSandbox(ongoingTaskRavenEtlTransformationModel.empty(this.findNameForNewTransformation()));
            deferred.resolve();
        }

        return $.when<any>(this.getAllConnectionStrings(), deferred)
            .done(() => {
                this.initObservables();
            })
    }

    private loadPossibleMentors() {
        const members = this.db.nodes()
            .filter(x => x.type === "Member")
            .map(x => x.tag);

        this.possibleMentors(members);
    }
    
    compositionComplete() {
        super.compositionComplete();

        $('.edit-raven-etl-task [data-toggle="tooltip"]').tooltip();
    }

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.db)
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const connectionStrings = Object.values(result.RavenConnectionStrings);
                this.ravenEtlConnectionStringsDetails(sortBy(connectionStrings, x => x.Name.toUpperCase()));
            });
    }

    private initObservables() {
        const model = this.editedRavenEtl();
        
        this.showAdvancedOptions(model.hasAdvancedOptionsDefined());
        
        this.shortErrorText = ko.pureComputed(() => {
            const result = this.testConnectionResult();
            if (!result || result.Success) {
                return "";
            }
            return generalUtils.trimMessage(result.Error);
        });
        
        this.newConnectionString(connectionStringRavenEtlModel.empty());
        this.newConnectionString().setNameUniquenessValidator(name => !this.ravenEtlConnectionStringsDetails().find(x => x.Name.toLocaleLowerCase() === name.toLocaleLowerCase()));
        
        const connectionStringName = model.connectionStringName();
        const connectionStringIsMissing = connectionStringName && !this.ravenEtlConnectionStringsDetails()
            .find(x => x.Name.toLocaleLowerCase() === connectionStringName.toLocaleLowerCase());

        if (!this.ravenEtlConnectionStringsDetails().length || connectionStringIsMissing) {
            this.createNewConnectionString(true);
        }

        if (connectionStringIsMissing) {
            // looks like user imported data w/o connection strings, prefill form with desired name
            this.newConnectionString().connectionStringName(connectionStringName);
            model.connectionStringName(null);
        }
        
        // Discard test connection result when needed
        this.createNewConnectionString.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().topologyDiscoveryUrls.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().inputUrl().discoveryUrlName.subscribe(() => this.testConnectionResult(null));

        this.enableTestArea.subscribe(testMode => {
            $("body").toggleClass('show-test', testMode);
        });

        const dtoProvider = () => {
            const dto = model.toDto();

            // override transforms - use only current transformation
            const transformationScriptDto = model.editedTransformationScriptSandbox().toDto();
            transformationScriptDto.Name = "Script_1"; // assign fake name
            dto.Transforms = [transformationScriptDto];

            if (!dto.Name) {
                dto.Name = "Test Raven ETL Task"; // assign fake name
            }
            return dto;
        };
        
        this.test = new ravenTaskTestMode(this.db, () => {
            return this.isValid(model.editedTransformationScriptSandbox().validationGroup);

        }, dtoProvider);

        this.dirtyFlag = new ko.DirtyFlag([
            this.createNewConnectionString,
            this.newConnectionString().dirtyFlag().isDirty,
            model.dirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
        
        this.test.initObservables();
    }

    useConnectionString(connectionStringToUse: string) {
        this.editedRavenEtl().connectionStringName(connectionStringToUse);
    }

    onTestConnectionRaven(urlToTest: discoveryUrl) {
        eventsCollector.default.reportEvent("ravenDB-ETL-connection-string", "test-connection");
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

    saveRavenEtl() {
        let hasAnyErrors = false;
        this.spinners.save(true);
        const editedEtl = this.editedRavenEtl();

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

        // 5. All is well, Save Raven Etl task
        savingNewStringAction.done(()=> {
            eventsCollector.default.reportEvent("raven-etl", "save");
            
            const scriptsToReset = editedEtl.transformationScripts().filter(x => x.resetScript()).map(x => x.name());

            const dto = editedEtl.toDto();
            saveEtlTaskCommand.forRavenEtl(this.db, dto, scriptsToReset)
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
        this.editedRavenEtl().editedTransformationScriptSandbox(ongoingTaskRavenEtlTransformationModel.empty(this.findNameForNewTransformation()));
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
            newTransformationItem.name(transformation.name());
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
        
        const maxNumber = _.max(scriptsWithPrefix
            .map(x => x.name().substr(editRavenEtlTask.scriptNamePrefix.length))
            .map(x => _.toInteger(x))) || 0;
        
        return editRavenEtlTask.scriptNamePrefix + (maxNumber + 1);
    }

    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.db));
    }

    createCollectionNameAutoCompleter(usedCollections: KnockoutObservableArray<string>, collectionText: KnockoutObservable<string>) {
        return ko.pureComputed(() => {
            let result;
            const key = collectionText();

            const options = this.collections().filter(x => !x.isAllDocuments).map(x => x.name);

            const usedOptions = usedCollections().filter(k => k !== key);

            const filteredOptions = options.filter(x => !usedOptions.includes(x));

            if (key) {
                result = filteredOptions.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                result = filteredOptions;
            }
            
            if (!_.includes(this.editedRavenEtl().editedTransformationScriptSandbox().transformScriptCollections(), ongoingTaskRavenEtlTransformationModel.applyToAllCollectionsText)) {
                result.unshift(ongoingTaskRavenEtlTransformationModel.applyToAllCollectionsText);
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
    
    toggleAdvancedArea() {
        this.showAdvancedOptions.toggle();
    }

    setState(state: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState): void {
        this.editedRavenEtl().taskState(state);
    }
}

export = editRavenEtlTask;

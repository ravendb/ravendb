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
import ongoingTaskQueueEtlTransformationModel = require("models/database/tasks/ongoingTaskQueueEtlTransformationModel");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import transformationScriptSyntax = require("viewmodels/database/tasks/transformationScriptSyntax");
import getConnectionStringInfoCommand = require("commands/database/settings/getConnectionStringInfoCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import jsonUtil = require("common/jsonUtil");
import viewHelpers = require("common/helpers/view/viewHelpers");
import documentMetadata = require("models/database/documents/documentMetadata");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import testQueueEtlCommand = require("commands/database/tasks/testQueueEtlCommand");
import document = require("models/database/documents/document");
import { highlight, languages } from "prismjs";
import licenseModel from "models/auth/licenseModel";
import { EditAzureQueueStorageEtlInfoHub } from "viewmodels/database/tasks/EditAzureQueueStorageEtlInfoHub";
import ongoingTaskAzureQueueStorageEtlEditModel from "models/database/tasks/ongoingTaskAzureQueueStorageEtlEditModel";
import connectionStringAzureQueueStorageModel from "models/database/settings/connectionStringAzureQueueStorageModel";

class azureQueueStorageTaskTestMode {
    documentId = ko.observable<string>();
    testDelete = ko.observable<boolean>(false);
    docsIdsAutocompleteResults = ko.observableArray<string>([]);
    db: KnockoutObservable<database>;
    configurationProvider: () => Raven.Client.Documents.Operations.ETL.Queue.QueueEtlConfiguration;

    validationGroup: KnockoutValidationGroup;
    validateParent: () => boolean;

    testAlreadyExecuted = ko.observable<boolean>(false);

    spinners = {
        preview: ko.observable<boolean>(false),
        test: ko.observable<boolean>(false)
    };

    loadedDocument = ko.observable<string>();
    loadedDocumentId = ko.observable<string>();

    testResults = ko.observableArray<Raven.Server.Documents.ETL.Providers.Queue.Test.QueueSummary>([]);
    debugOutput = ko.observableArray<string>([]);

    // all kinds of alerts:
    transformationErrors = ko.observableArray<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>([]);

    warningsCount = ko.pureComputed(() => {
        return this.transformationErrors().length;
    });

    constructor(db: KnockoutObservable<database>,
                validateParent: () => boolean,
                configurationProvider: () => Raven.Client.Documents.Operations.ETL.Queue.QueueEtlConfiguration) {
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
                    new getDocumentWithMetadataCommand(documentId(), this.db())
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

            const dto: Raven.Server.Documents.ETL.Providers.Queue.Test.TestQueueEtlScript = {
                DocumentId: this.documentId(),
                IsDelete: this.testDelete(),
                Configuration: this.configurationProvider()
            };

            eventsCollector.default.reportEvent("azure-queue-storage-etl", "test-script");

            new testQueueEtlCommand(this.db(), dto, "AzureQueueStorage")
                .execute()
                .done(simulationResult => {
                    this.testResults(simulationResult.Summary);
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

class editAzureQueueStorageEtlTask extends viewModelBase {

    view = require("views/database/tasks/editAzureQueueStorageEtlTask.html");
    optionsPerQueueEtlView = require("views/database/tasks/optionsPerQueueEtl.html");
    connectionStringView = require("views/database/settings/connectionStringAzureQueueStorage.html");
    taskResponsibleNodeSectionView = require("views/partial/taskResponsibleNodeSection.html");
    pinResponsibleNodeTextScriptView = require("views/partial/pinResponsibleNodeTextScript.html");

    static readonly scriptNamePrefix = "Script_";
    static isApplyToAll = ongoingTaskQueueEtlTransformationModel.isApplyToAll;

    enableTestArea = ko.observable<boolean>(false);
    showAdvancedOptions = ko.observable<boolean>(false);

    test: azureQueueStorageTaskTestMode;

    editedEtl = ko.observable<ongoingTaskAzureQueueStorageEtlEditModel>();
    isAddingNewEtlTask = ko.observable<boolean>(true);

    etlConnectionStringsDetails = ko.observableArray<Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString>([]);

    possibleMentors = ko.observableArray<string>([]);

    spinners = {
        test: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false)
    };

    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;

    createNewConnectionString = ko.observable<boolean>(false);
    newConnectionString = ko.observable<connectionStringAzureQueueStorageModel>();

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();

    collections = collectionsTracker.default.collections;

    isSharded = ko.pureComputed(() => {
        const db = this.activeDatabase();
        return db ? db.isSharded() : false;
    });

    hasQueueEtl = licenseModel.getStatusValue("HasQueueEtl");
    infoHubView: ReactInKnockout<typeof EditAzureQueueStorageEtlInfoHub>;

    constructor() {
        super();

        aceEditorBindingHandler.install();
        this.bindToCurrentInstance("useConnectionString", "onTestConnection", "removeTransformationScript",
            "cancelEditedTransformation", "saveEditedTransformation", "syntaxHelp",
            "toggleTestArea", "toggleAdvancedArea", "setState");
        this.infoHubView = ko.pureComputed(() => ({
            component: EditAzureQueueStorageEtlInfoHub
        }))
    }

    activate(args: any) {
        super.activate(args);
        const deferred = $.Deferred<void>();

        this.loadPossibleMentors();

        if (args.taskId) {
            // 1. Editing an Existing task
            this.isAddingNewEtlTask(false);

            getOngoingTaskInfoCommand.forQueueEtl(this.activeDatabase(), args.taskId)
                .execute()
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtl) => {
                    this.editedEtl(new ongoingTaskAzureQueueStorageEtlEditModel(result));
                    this.showAdvancedOptions(this.editedEtl().hasAdvancedOptionsDefined());
                    deferred.resolve();
                })
                .fail(() => {
                    deferred.reject();
                    router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
                });
        } else {
            // 2. Creating a New task
            this.isAddingNewEtlTask(true);
            this.editedEtl(ongoingTaskAzureQueueStorageEtlEditModel.empty());
            this.editedEtl().editedTransformationScriptSandbox(ongoingTaskQueueEtlTransformationModel.empty(this.findNameForNewTransformation()));
            deferred.resolve();
        }

        return $.when<any>(this.getAllConnectionStrings(), deferred)
            .done(() => {
                this.initObservables();
            })
    }

    private loadPossibleMentors() {
        const db = this.activeDatabase();
        const members = db.nodes()
            .filter(x => x.type === "Member")
            .map(x => x.tag);

        this.possibleMentors(members);
    }

    compositionComplete() {
        super.compositionComplete();

        $('.edit-azure-queue-storage-etl-task [data-toggle="tooltip"]').tooltip();
    }

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.activeDatabase())
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const queueConnectionStrings = Object.values(result.QueueConnectionStrings);
                const azureQueueStorageStrings = queueConnectionStrings.filter(x => x.BrokerType === "AzureQueueStorage");
                this.etlConnectionStringsDetails(_.sortBy(azureQueueStorageStrings, x => x.Name.toUpperCase()));
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

        this.newConnectionString(connectionStringAzureQueueStorageModel.empty());
        this.newConnectionString().setNameUniquenessValidator(name => !this.etlConnectionStringsDetails().find(x => x.Name.toLocaleLowerCase() === name.toLocaleLowerCase()));

        const connectionStringName = this.editedEtl().connectionStringName();
        const connectionStringIsMissing = connectionStringName && !this.etlConnectionStringsDetails()
            .find(x => x.Name.toLocaleLowerCase() === connectionStringName.toLocaleLowerCase());

        if (!this.etlConnectionStringsDetails().length || connectionStringIsMissing) {
            this.createNewConnectionString(true);
        }

        if (connectionStringIsMissing) {
            // looks like user imported data w/o connection strings, prefill form with desired name
            this.newConnectionString().connectionStringName(connectionStringName);
            this.editedEtl().connectionStringName(null);
        }

        // Discard test connection result when needed
        this.createNewConnectionString.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().onChange(() => this.testConnectionResult());

        this.enableTestArea.subscribe(testMode => {
            $("body").toggleClass('show-test', testMode);
        });

        const dtoProvider = () => {
            const dto = this.editedEtl().toDto();

            // override transforms - use only current transformation
            const transformationScriptDto = this.editedEtl().editedTransformationScriptSandbox().toDto();
            transformationScriptDto.Name = "Script_1"; // assign fake name
            dto.Transforms = [transformationScriptDto];

            if (!dto.Name) {
                dto.Name = "Test Azure Queue Storage ETL Task"; // assign fake name
            }
            return dto;
        };

        this.test = new azureQueueStorageTaskTestMode(this.activeDatabase, () => {
            return this.isValid(this.editedEtl().editedTransformationScriptSandbox().validationGroup);
        }, dtoProvider);

        this.test.initObservables();

        this.dirtyFlag = new ko.DirtyFlag([
            this.createNewConnectionString,
            this.newConnectionString().dirtyFlag().isDirty,
            this.editedEtl().dirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    useConnectionString(connectionStringToUse: string) {
        this.editedEtl().connectionStringName(connectionStringToUse);
    }

    onTestConnection() {
        eventsCollector.default.reportEvent("azure-queue-storage-connection-string", "test-connection");
        this.spinners.test(true);
        this.testConnectionResult(null);

        // New connection string
        if (this.createNewConnectionString()) {
            this.newConnectionString()
                .testConnection(this.activeDatabase())
                .done(result => this.testConnectionResult(result))
                .always(() => {
                    this.spinners.test(false);
                    this.fullErrorDetailsVisible(false);
                });
        } else {
            // Existing connection string
            
            getConnectionStringInfoCommand.forAzureQueueStorageEtl(this.activeDatabase(), this.editedEtl().connectionStringName())
                .execute()
                .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                    new connectionStringAzureQueueStorageModel(result.QueueConnectionStrings[this.editedEtl().connectionStringName()], true, [])
                        .testConnection(this.activeDatabase())
                        .done((testResult) => this.testConnectionResult(testResult))
                        .always(() => {
                            this.spinners.test(false);
                        });
                });
        }
    }

    saveEtl() {
        let hasAnyErrors = false;
        this.spinners.save(true);
        const editedEtl = this.editedEtl();

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

            // todo test the string box if valid...

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

        let validOptions = true;
        editedEtl.optionsPerQueue().forEach(x => {
            validOptions = this.isValid(x.validationGroup);
        })

        if (hasAnyErrors || !validOptions) {
            this.spinners.save(false);
            return false;
        }

        // 4. All is well, Save connection string (if relevant..) 
        const savingNewStringAction = $.Deferred<void>();
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

        // 5. All is well, Save Etl task
        savingNewStringAction.done(()=> {
            eventsCollector.default.reportEvent("azure-queue-storage-etl", "save");

            const scriptsToReset = editedEtl.transformationScripts().filter(x => x.resetScript()).map(x => x.name());

            const dto = editedEtl.toDto();
            saveEtlTaskCommand.forQueueEtl(this.activeDatabase(), dto, scriptsToReset)
                .execute()
                .done(() => {
                    this.dirtyFlag().reset();
                    this.goToOngoingTasksView();
                })
                .always(() => this.spinners.save(false));
        });
    }

    addNewTransformation() {
        this.editedEtl().transformationScriptSelectedForEdit(null);
        this.editedEtl().editedTransformationScriptSandbox(ongoingTaskQueueEtlTransformationModel.empty(this.findNameForNewTransformation()));
    }

    cancelEditedTransformation() {
        this.editedEtl().editedTransformationScriptSandbox(null);
        this.editedEtl().transformationScriptSelectedForEdit(null);
        this.enableTestArea(false);
    }

    saveEditedTransformation() {
        this.enableTestArea(false);
        const transformation = this.editedEtl().editedTransformationScriptSandbox();
        if (!this.isValid(transformation.validationGroup)) {
            return;
        }

        if (transformation.isNew()) {
            const newTransformationItem = new ongoingTaskQueueEtlTransformationModel(transformation.toDto(), true, false);
            newTransformationItem.name(transformation.name());
            newTransformationItem.dirtyFlag().forceDirty();
            this.editedEtl().transformationScripts.push(newTransformationItem);
        } else {
            const oldItem = this.editedEtl().transformationScriptSelectedForEdit();
            const newItem = new ongoingTaskQueueEtlTransformationModel(transformation.toDto(), false, transformation.resetScript());

            if (oldItem.dirtyFlag().isDirty() || newItem.hasUpdates(oldItem)) {
                newItem.dirtyFlag().forceDirty();
            }

            this.editedEtl().transformationScripts.replace(oldItem, newItem);
        }

        this.editedEtl().transformationScripts.sort((a, b) => a.name().toLowerCase().localeCompare(b.name().toLowerCase()));
        this.editedEtl().editedTransformationScriptSandbox(null);
        this.editedEtl().transformationScriptSelectedForEdit(null);
    }

    private findNameForNewTransformation() {
        const scriptsWithPrefix = this.editedEtl().transformationScripts().filter(script => {
            return script.name().startsWith(editAzureQueueStorageEtlTask.scriptNamePrefix);
        });

        const maxNumber = _.max(scriptsWithPrefix
            .map(x => x.name().substr(editAzureQueueStorageEtlTask.scriptNamePrefix.length))
            .map(x => _.toInteger(x))) || 0;

        return editAzureQueueStorageEtlTask.scriptNamePrefix + (maxNumber + 1);
    }

    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
    }

    createCollectionNameAutoCompleter(usedCollections: KnockoutObservableArray<string>, collectionText: KnockoutObservable<string>) {
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

            if (!_.includes(this.editedEtl().editedTransformationScriptSandbox().transformScriptCollections(), ongoingTaskQueueEtlTransformationModel.applyToAllCollectionsText)) {
                result.unshift(ongoingTaskQueueEtlTransformationModel.applyToAllCollectionsText);
            }

            return result;
        });
    }

    removeTransformationScript(model: ongoingTaskQueueEtlTransformationModel) {
        this.editedEtl().deleteTransformationScript(model);
    }

    syntaxHelp() {
        const viewmodel = new transformationScriptSyntax("AzureQueueStorage");
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
        this.editedEtl().taskState(state);
    }
}

export = editAzureQueueStorageEtlTask;

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
import ongoingTaskRabbitMqEtlEditModel = require("models/database/tasks/ongoingTaskRabbitMqEtlEditModel");
import ongoingTaskQueueEtlTransformationModel = require("models/database/tasks/ongoingTaskQueueEtlTransformationModel");
import connectionStringRabbitMqEtlModel = require("models/database/settings/connectionStringRabbitMqModel");
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
import popoverUtils = require("common/popoverUtils");
import { highlight, languages } from "prismjs";
import licenseModel from "models/auth/licenseModel";
import { EditRabbitMqEtlInfoHub } from "viewmodels/database/tasks/EditRabbitMqEtlInfoHub";
import { sortBy } from "common/typeUtils";

class rabbitMqTaskTestMode {
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

            eventsCollector.default.reportEvent("rabbitmq-etl", "test-script");

            new testQueueEtlCommand(this.db(), dto, "RabbitMq")
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

class editRabbitMqEtlTask extends viewModelBase {

    view = require("views/database/tasks/editRabbitMqEtlTask.html");
    optionsPerQueueEtlView = require("views/database/tasks/optionsPerQueueEtl.html");
    connectionStringView = require("views/database/settings/connectionStringRabbitMq.html");
    taskResponsibleNodeSectionView = require("views/partial/taskResponsibleNodeSection.html");
    pinResponsibleNodeTextScriptView = require("views/partial/pinResponsibleNodeTextScript.html");
    
    static readonly scriptNamePrefix = "Script_";
    static isApplyToAll = ongoingTaskQueueEtlTransformationModel.isApplyToAll;

    enableTestArea = ko.observable<boolean>(false);
    showAdvancedOptions = ko.observable<boolean>(false);

    test: rabbitMqTaskTestMode;
    
    editedRabbitMqEtl = ko.observable<ongoingTaskRabbitMqEtlEditModel>();
    isAddingNewRabbitMqEtlTask = ko.observable<boolean>(true);
    
    rabbitMqEtlConnectionStringsDetails = ko.observableArray<Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString>([]);

    possibleMentors = ko.observableArray<string>([]);
    
    spinners = {
        test: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false)
    };
    
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;
    
    createNewConnectionString = ko.observable<boolean>(false);
    newConnectionString = ko.observable<connectionStringRabbitMqEtlModel>();

    connectionStringDefined: KnockoutComputed<boolean>;
    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    
    collections = collectionsTracker.default.collections;

    isSharded = ko.pureComputed(() => {
        const db = this.activeDatabase();
        return db ? db.isSharded() : false;
    });
    
    hasQueueEtl = licenseModel.getStatusValue("HasQueueEtl");
    infoHubView: ReactInKnockout<typeof EditRabbitMqEtlInfoHub>;
    
    constructor() {
        super();
        
        aceEditorBindingHandler.install();
        this.bindToCurrentInstance("useConnectionString", "onTestConnectionRabbitMq", "removeTransformationScript",
                                   "cancelEditedTransformation", "saveEditedTransformation", "syntaxHelp",
                                   "toggleTestArea", "toggleAdvancedArea", "setState");
        this.infoHubView = ko.pureComputed(() => ({
            component: EditRabbitMqEtlInfoHub
        }))
    }

    activate(args: any) {
        super.activate(args);
        const deferred = $.Deferred<void>();
        
        this.loadPossibleMentors();

        if (args.taskId) {
            // 1. Editing an Existing task
            this.isAddingNewRabbitMqEtlTask(false);
            
            getOngoingTaskInfoCommand.forQueueEtl(this.activeDatabase(), args.taskId)
                .execute()
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtl) => {
                    this.editedRabbitMqEtl(new ongoingTaskRabbitMqEtlEditModel(result));
                    this.showAdvancedOptions(this.editedRabbitMqEtl().hasAdvancedOptionsDefined());
                    deferred.resolve();
                })
                .fail(() => { 
                    deferred.reject();
                    router.navigate(appUrl.forOngoingTasks(this.activeDatabase())); 
                });
        } else {
            // 2. Creating a New task
            this.isAddingNewRabbitMqEtlTask(true);
            this.editedRabbitMqEtl(ongoingTaskRabbitMqEtlEditModel.empty());
            this.editedRabbitMqEtl().editedTransformationScriptSandbox(ongoingTaskQueueEtlTransformationModel.empty(this.findNameForNewTransformation()));
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

        $('.edit-rabbitmq-etl-task [data-toggle="tooltip"]').tooltip();

        popoverUtils.longWithHover($(".skip-automatic-declaration"),
            {
                content: `<small class="margin-top-xs no-padding-left">
                              Use this option when manually defining the Exchanges, Queues & Bindings.
                          </small>`
            });
    }

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.activeDatabase())
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const queueConnectionStrings = Object.values(result.QueueConnectionStrings);
                const rabbitMqStrings = queueConnectionStrings.filter(x => x.BrokerType === "RabbitMq");
                this.rabbitMqEtlConnectionStringsDetails(sortBy(rabbitMqStrings, x => x.Name.toUpperCase()));
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
        
        this.newConnectionString(connectionStringRabbitMqEtlModel.empty());
        this.newConnectionString().setNameUniquenessValidator(name => !this.rabbitMqEtlConnectionStringsDetails().find(x => x.Name.toLocaleLowerCase() === name.toLocaleLowerCase()));
        
        const connectionStringName = this.editedRabbitMqEtl().connectionStringName();
        const connectionStringIsMissing = connectionStringName && !this.rabbitMqEtlConnectionStringsDetails()
            .find(x => x.Name.toLocaleLowerCase() === connectionStringName.toLocaleLowerCase());

        if (!this.rabbitMqEtlConnectionStringsDetails().length || connectionStringIsMissing) {
            this.createNewConnectionString(true);
        }

        if (connectionStringIsMissing) {
            // looks like user imported data w/o connection strings, prefill form with desired name
            this.newConnectionString().connectionStringName(connectionStringName);
            this.editedRabbitMqEtl().connectionStringName(null);
        }
        
        // Discard test connection result when needed
        this.createNewConnectionString.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().rabbitMqConnectionString.subscribe(() => this.testConnectionResult(null));

        this.connectionStringDefined = ko.pureComputed(() => {
            const editedEtl = this.editedRabbitMqEtl();
            if (this.createNewConnectionString()) {
                return !!this.newConnectionString().rabbitMqConnectionString();
            } else {
                return !!editedEtl.connectionStringName();
            }
        });
        
        this.enableTestArea.subscribe(testMode => {
            $("body").toggleClass('show-test', testMode);
        });

        const dtoProvider = () => {
            const dto = this.editedRabbitMqEtl().toDto();

            // override transforms - use only current transformation
            const transformationScriptDto = this.editedRabbitMqEtl().editedTransformationScriptSandbox().toDto();
            transformationScriptDto.Name = "Script_1"; // assign fake name
            dto.Transforms = [transformationScriptDto];

            if (!dto.Name) {
                dto.Name = "Test RabbitMQ ETL Task"; // assign fake name
            }
            return dto;
        };

        this.test = new rabbitMqTaskTestMode(this.activeDatabase, () => {
            return this.isValid(this.editedRabbitMqEtl().editedTransformationScriptSandbox().validationGroup);
        }, dtoProvider);

        this.test.initObservables();

        this.dirtyFlag = new ko.DirtyFlag([
            this.createNewConnectionString,
            this.newConnectionString().dirtyFlag().isDirty,
            this.editedRabbitMqEtl().dirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    useConnectionString(connectionStringToUse: string) {
        this.editedRabbitMqEtl().connectionStringName(connectionStringToUse);
    }

    onTestConnectionRabbitMq() {
        eventsCollector.default.reportEvent("rabbitmq-connection-string", "test-connection");
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
            getConnectionStringInfoCommand.forKafkaEtl(this.activeDatabase(), this.editedRabbitMqEtl().connectionStringName())
                .execute()
                .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                    new connectionStringRabbitMqEtlModel(result.QueueConnectionStrings[this.editedRabbitMqEtl().connectionStringName()], true, [])
                        .testConnection(this.activeDatabase())
                        .done((testResult) => this.testConnectionResult(testResult))
                        .always(() => {
                            this.spinners.test(false);
                        });
                });
        }
    }
    
    saveRabbitMqEtl() {
        let hasAnyErrors = false;
        this.spinners.save(true);
        const editedEtl = this.editedRabbitMqEtl();
        
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

        // 5. All is well, Save RabbitMQ Etl task
        savingNewStringAction.done(()=> {
            eventsCollector.default.reportEvent("rabbitMQ-etl", "save");
            
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
        this.editedRabbitMqEtl().transformationScriptSelectedForEdit(null);
        this.editedRabbitMqEtl().editedTransformationScriptSandbox(ongoingTaskQueueEtlTransformationModel.empty(this.findNameForNewTransformation()));
    }

    cancelEditedTransformation() {
        this.editedRabbitMqEtl().editedTransformationScriptSandbox(null);
        this.editedRabbitMqEtl().transformationScriptSelectedForEdit(null);
        this.enableTestArea(false);
    }

    saveEditedTransformation() {
        this.enableTestArea(false);
        const transformation = this.editedRabbitMqEtl().editedTransformationScriptSandbox();
        if (!this.isValid(transformation.validationGroup)) {
            return;
        }
        
        if (transformation.isNew()) {
            const newTransformationItem = new ongoingTaskQueueEtlTransformationModel(transformation.toDto(), true, false); 
            newTransformationItem.name(transformation.name());
            newTransformationItem.dirtyFlag().forceDirty();
            this.editedRabbitMqEtl().transformationScripts.push(newTransformationItem);
        } else {
            const oldItem = this.editedRabbitMqEtl().transformationScriptSelectedForEdit();
            const newItem = new ongoingTaskQueueEtlTransformationModel(transformation.toDto(), false, transformation.resetScript());
            
            if (oldItem.dirtyFlag().isDirty() || newItem.hasUpdates(oldItem)) {
                newItem.dirtyFlag().forceDirty();
            }
            
            this.editedRabbitMqEtl().transformationScripts.replace(oldItem, newItem);
        }

        this.editedRabbitMqEtl().transformationScripts.sort((a, b) => a.name().toLowerCase().localeCompare(b.name().toLowerCase()));
        this.editedRabbitMqEtl().editedTransformationScriptSandbox(null);
        this.editedRabbitMqEtl().transformationScriptSelectedForEdit(null);
    }
    
    private findNameForNewTransformation() {
        const scriptsWithPrefix = this.editedRabbitMqEtl().transformationScripts().filter(script => {
            return script.name().startsWith(editRabbitMqEtlTask.scriptNamePrefix);
        });
        
        const maxNumber = _.max(scriptsWithPrefix
            .map(x => x.name().substr(editRabbitMqEtlTask.scriptNamePrefix.length))
            .map(x => _.toInteger(x))) || 0;
        
        return editRabbitMqEtlTask.scriptNamePrefix + (maxNumber + 1);
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

            const filteredOptions = options.filter(x => !usedOptions.includes(x));

            if (key) {
                result = filteredOptions.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                result = filteredOptions;
            }
            
            if (!_.includes(this.editedRabbitMqEtl().editedTransformationScriptSandbox().transformScriptCollections(), ongoingTaskQueueEtlTransformationModel.applyToAllCollectionsText)) {
                result.unshift(ongoingTaskQueueEtlTransformationModel.applyToAllCollectionsText);
            }
            
            return result;
        });
    }

    removeTransformationScript(model: ongoingTaskQueueEtlTransformationModel) {
        this.editedRabbitMqEtl().deleteTransformationScript(model);
    }

    syntaxHelp() {
        const viewmodel = new transformationScriptSyntax("RabbitMQ");
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
        this.editedRabbitMqEtl().taskState(state);
    }
}

export = editRabbitMqEtlTask;

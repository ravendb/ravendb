import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import eventsCollector = require("common/eventsCollector");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import generalUtils = require("common/generalUtils");
import connectionStringAzureServiceBusModel = require("models/database/settings/connectionStringAzureServiceBusModel");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import getConnectionStringInfoCommand = require("commands/database/settings/getConnectionStringInfoCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import jsonUtil = require("common/jsonUtil");
import ongoingTaskQueueSinkScriptModel = require("models/database/tasks/ongoingTaskQueueSinkScriptModel");
import saveQueueSinkCommand = require("commands/database/tasks/saveQueueSinkCommand");
import ongoingTaskAzureServiceBusSinkEditModel = require("models/database/tasks/ongoingTaskAzureServiceBusSinkEditModel");
import viewHelpers = require("common/helpers/view/viewHelpers");
import database = require("models/resources/database");
import testQueueSinkCommand = require("commands/database/tasks/testQueueSinkCommand");
import getOngoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import queueSinkSyntax = require("viewmodels/database/tasks/queueSinkSyntax");
import patchDebugActions = require("viewmodels/database/patch/patchDebugActions");
import licenseModel = require("models/auth/licenseModel");
import EditAzureServiceBusSinkTaskInfoHub = require("./EditAzureServiceBusSinkTaskInfoHub");
import typeUtils = require("common/typeUtils");

class azureServiceBusTaskTestMode {
    db: KnockoutObservable<database>;
    configurationProvider: () => Raven.Client.Documents.Operations.QueueSink.QueueSinkConfiguration;

    messageText = ko.observable("{}");

    validationGroup: KnockoutValidationGroup;
    validateParent: () => boolean;

    testAlreadyExecuted = ko.observable<boolean>(false);

    spinners = {
        test: ko.observable<boolean>(false)
    };

    actions = new patchDebugActions();
    debugOutput = ko.observableArray<string>([]);

    constructor(db: KnockoutObservable<database>,
                validateParent: () => boolean,
                configurationProvider: () => Raven.Client.Documents.Operations.QueueSink.QueueSinkConfiguration) {
        this.db = db;
        this.validateParent = validateParent;
        this.configurationProvider = configurationProvider;

        // on edit Azure Service Bus view we want to show documents by default
        this.actions.showDocumentsInModified(true);
    }

    initObservables() {
        this.messageText.extend({
            required: true,
            aceValidation: true
        });

        this.validationGroup = ko.validatedObservable({
            messageText: this.messageText
        });
    }

    runTest() {
        const testValid = viewHelpers.isValid(this.validationGroup, true);
        const parentValid = this.validateParent();

        if (testValid && parentValid) {
            this.spinners.test(true);

            const dto: Raven.Server.Documents.QueueSink.Test.TestQueueSinkScript = {
                Configuration: this.configurationProvider(),
                Message: this.messageText()
            };

            eventsCollector.default.reportEvent("azure-service-bus-sink", "test-script");

            new testQueueSinkCommand(this.db(), dto, "AzureServiceBus")
                .execute()
                .done(simulationResult => {
                    this.actions.fill(simulationResult.Actions);
                    this.debugOutput(simulationResult.DebugOutput);

                    this.testAlreadyExecuted(true);
                })
                .fail(() => {
                    this.actions.reset();
                })
                .always(() => this.spinners.test(false));
        }
    }
}

class editAzureServiceBusSinkTask extends viewModelBase {

    view = require("views/database/tasks/editAzureServiceBusSinkTask.html");
    connectionStringView = require("views/database/settings/connectionStringAzureServiceBus.html");
    taskResponsibleNodeSectionView = require("views/partial/taskResponsibleNodeSection.html");
    pinResponsibleNodeTextScriptView = require("views/partial/pinResponsibleNodeTextScript.html");

    patchDebugActionsLoadedView = require("views/database/patch/patchDebugActionsLoaded.html");
    patchDebugActionsModifiedView = require("views/database/patch/patchDebugActionsModified.html");
    patchDebugActionsDeletedView = require("views/database/patch/patchDebugActionsDeleted.html");

    hasQueueSink = licenseModel.getStatusValue("HasQueueSink");

    static readonly scriptNamePrefix = "Script_";

    enableTestArea = ko.observable<boolean>(false);
    test: azureServiceBusTaskTestMode;

    infoHubView: ReactInKnockout<typeof EditAzureServiceBusSinkTaskInfoHub.EditAzureServiceBusSinkTaskInfoHub>;

    editedAzureServiceBusSink = ko.observable<ongoingTaskAzureServiceBusSinkEditModel>();

    isAddingNewAzureServiceBusSinkTask = ko.observable<boolean>(true);

    azureServiceBusConnectionStringsDetails = ko.observableArray<Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString>([]);

    possibleMentors = ko.observableArray<string>([]);

    spinners = {
        test: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false)
    };

    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;

    createNewConnectionString = ko.observable<boolean>(false);
    newConnectionString = ko.observable<connectionStringAzureServiceBusModel>();

    connectionStringDefined: KnockoutComputed<boolean>;
    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();

    collections = collectionsTracker.default.collections;

    isSharded = ko.pureComputed(() => {
        const db = this.activeDatabase();
        return db ? db.isSharded() : false;
    });

    constructor() {
        super();

        aceEditorBindingHandler.install();
        this.bindToCurrentInstance("useConnectionString", "removeScript",
            "cancelEditedScript", "saveEditedScript", "syntaxHelp", "onTestConnectionAzureServiceBus", "toggleTestArea",
            "setState");

        this.infoHubView = ko.pureComputed(() => ({
            component: EditAzureServiceBusSinkTaskInfoHub.EditAzureServiceBusSinkTaskInfoHub
        }));
    }

    activate(args: any) {
        super.activate(args);
        const deferred = $.Deferred<void>();

        this.loadPossibleMentors();

        if (args.taskId) {
            // 1. Editing an Existing task
            this.isAddingNewAzureServiceBusSinkTask(false);

            getOngoingTaskInfoCommand.forQueueSink(this.activeDatabase(), args.taskId)
                .execute()
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueSink) => {
                    this.editedAzureServiceBusSink(new ongoingTaskAzureServiceBusSinkEditModel(result));
                    deferred.resolve();
                })
                .fail(() => {
                    deferred.reject();
                    router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
                });
        } else {
            // 2. Creating a New task
            this.isAddingNewAzureServiceBusSinkTask(true);
            this.editedAzureServiceBusSink(ongoingTaskAzureServiceBusSinkEditModel.empty());
            this.editedAzureServiceBusSink().editedScriptSandbox(ongoingTaskQueueSinkScriptModel.empty(this.findNameForNewScript()));
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

        $('.edit-azure-service-bus-sink-task [data-toggle="tooltip"]').tooltip();
    }

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.activeDatabase())
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const queueConnectionStrings = Object.values(result.QueueConnectionStrings);
                const azureServiceBusStrings = queueConnectionStrings.filter(x => x.BrokerType === "AzureServiceBus");
                this.azureServiceBusConnectionStringsDetails(typeUtils.sortBy(azureServiceBusStrings, x => x.Name.toUpperCase()));
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

        this.newConnectionString(connectionStringAzureServiceBusModel.empty());
        this.newConnectionString().setNameUniquenessValidator(name => !this.azureServiceBusConnectionStringsDetails().find(x => x.Name.toLocaleLowerCase() === name.toLocaleLowerCase()));

        const connectionStringName = this.editedAzureServiceBusSink().connectionStringName();
        const connectionStringIsMissing = connectionStringName && !this.azureServiceBusConnectionStringsDetails()
            .find(x => x.Name.toLocaleLowerCase() === connectionStringName.toLocaleLowerCase());

        if (!this.azureServiceBusConnectionStringsDetails().length || connectionStringIsMissing) {
            this.createNewConnectionString(true);
        }

        if (connectionStringIsMissing) {
            // looks like user imported data w/o connection strings, prefill form with desired name
            this.newConnectionString().connectionStringName(connectionStringName);
            this.editedAzureServiceBusSink().connectionStringName(null);
        }

        // Discard test connection result when needed
        this.createNewConnectionString.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().azureServiceBusConnectionString.subscribe(() => this.testConnectionResult(null));

        this.connectionStringDefined = ko.pureComputed(() => {
            const editedEtl = this.editedAzureServiceBusSink();
            if (this.createNewConnectionString()) {
                return !!this.newConnectionString().azureServiceBusConnectionString();
            } else {
                return !!editedEtl.connectionStringName();
            }
        });

        this.enableTestArea.subscribe(testMode => {
            $("body").toggleClass('show-test', testMode);
        });

        const dtoProvider = () => {
            const dto = this.editedAzureServiceBusSink().toDto();

            // override transforms - use only current transformation
            const scriptDto = this.editedAzureServiceBusSink().editedScriptSandbox().toDto();
            scriptDto.Name = "Script_1"; // assign fake name
            dto.Scripts = [scriptDto];

            if (!dto.Name) {
                dto.Name = "Test Azure Service Bus Sink Task"; // assign fake name
            }
            return dto;
        };


        this.test = new azureServiceBusTaskTestMode(this.activeDatabase, () => {
            return this.isValid(this.editedAzureServiceBusSink().editedScriptSandbox().testValidationGroup);
        }, dtoProvider);

        this.test.initObservables();

        this.dirtyFlag = new ko.DirtyFlag([
            this.createNewConnectionString,
            this.newConnectionString().dirtyFlag().isDirty,
            this.editedAzureServiceBusSink().dirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    useConnectionString(connectionStringToUse: string) {
        this.editedAzureServiceBusSink().connectionStringName(connectionStringToUse);
    }

    onTestConnectionAzureServiceBus() {
        eventsCollector.default.reportEvent("azure-service-bus-connection-string", "test-connection");
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
            getConnectionStringInfoCommand.forAzureServiceBusEtl(this.activeDatabase(), this.editedAzureServiceBusSink().connectionStringName())
                .execute()
                .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                    new connectionStringAzureServiceBusModel(result.QueueConnectionStrings[this.editedAzureServiceBusSink().connectionStringName()], true, [])
                        .testConnection(this.activeDatabase())
                        .done((testResult) => this.testConnectionResult(testResult))
                        .always(() => {
                            this.spinners.test(false);
                            this.fullErrorDetailsVisible(false);
                        });
                });
        }
    }

    saveAzureServiceBusEtl() {
        let hasAnyErrors = false;
        this.spinners.save(true);
        const editedSink = this.editedAzureServiceBusSink();

        // 1. Validate *edited script*
        if (editedSink.showEditScriptArea()) {
            if (!this.isValid(editedSink.editedScriptSandbox().validationGroup)) {
                hasAnyErrors = true;
            } else {
                this.saveEditedScript();
            }
        }

        // 2. Validate *new connection string* (if relevant..)
        if (this.createNewConnectionString()) {
            if (!this.isValid(this.newConnectionString().validationGroup)) {
                hasAnyErrors = true;
            } else {
                // Use the new connection string
                editedSink.connectionStringName(this.newConnectionString().connectionStringName());
            }
        }

        // 3. Validate *general form*
        if (!this.isValid(editedSink.validationGroup)) {
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

        // 5. All is well, Save Azure Service Bus Sink task
        savingNewStringAction.done(()=> {
            eventsCollector.default.reportEvent("azure-service-bus-sink", "save");

            const dto = editedSink.toDto();
            new saveQueueSinkCommand(this.activeDatabase(), dto)
                .execute()
                .done(() => {
                    this.dirtyFlag().reset();
                    this.goToOngoingTasksView();
                })
                .always(() => this.spinners.save(false));
        });
    }

    addNewScript() {
        this.editedAzureServiceBusSink().scriptSelectedForEdit(null);
        this.editedAzureServiceBusSink().editedScriptSandbox(ongoingTaskQueueSinkScriptModel.empty(this.findNameForNewScript()));
    }

    cancelEditedScript() {
        this.editedAzureServiceBusSink().editedScriptSandbox(null);
        this.editedAzureServiceBusSink().scriptSelectedForEdit(null);
        this.enableTestArea(false);
    }

    saveEditedScript() {
        this.enableTestArea(false);
        const script = this.editedAzureServiceBusSink().editedScriptSandbox();
        if (!this.isValid(script.validationGroup)) {
            return;
        }

        if (script.isNew()) {
            const newScriptItem = new ongoingTaskQueueSinkScriptModel(script.toDto(), true);
            newScriptItem.name(script.name());
            newScriptItem.dirtyFlag().forceDirty();
            this.editedAzureServiceBusSink().scripts.push(newScriptItem);
        } else {
            const oldItem = this.editedAzureServiceBusSink().scriptSelectedForEdit();
            const newItem = new ongoingTaskQueueSinkScriptModel(script.toDto(), false);

            if (oldItem.dirtyFlag().isDirty() || newItem.hasUpdates(oldItem)) {
                newItem.dirtyFlag().forceDirty();
            }

            this.editedAzureServiceBusSink().scripts.replace(oldItem, newItem);
        }

        this.editedAzureServiceBusSink().scripts.sort((a, b) => a.name().toLowerCase().localeCompare(b.name().toLowerCase()));
        this.editedAzureServiceBusSink().editedScriptSandbox(null);
        this.editedAzureServiceBusSink().scriptSelectedForEdit(null);
    }

    private findNameForNewScript() {
        const scriptsWithPrefix = this.editedAzureServiceBusSink().scripts().filter(script => {
            return script.name().startsWith(editAzureServiceBusSinkTask.scriptNamePrefix);
        });

        const maxNumber = _.max(scriptsWithPrefix
            .map(x => x.name().substring(editAzureServiceBusSinkTask.scriptNamePrefix.length))
            .map(x => _.toInteger(x))) || 0;

        return editAzureServiceBusSinkTask.scriptNamePrefix + (maxNumber + 1);
    }

    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
    }

    removeScript(model: ongoingTaskQueueSinkScriptModel) {
        this.editedAzureServiceBusSink().deleteScript(model);
    }

    syntaxHelp() {
        const viewmodel = new queueSinkSyntax();
        app.showBootstrapDialog(viewmodel);
    }

    toggleTestArea() {
        if (!this.enableTestArea()) {
            this.enableTestArea(true);
        } else {
            this.enableTestArea(false);
        }
    }

    setState(state: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState): void {
        this.editedAzureServiceBusSink().taskState(state);
    }
}

export = editAzureServiceBusSinkTask;

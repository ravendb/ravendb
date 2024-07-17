import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import eventsCollector = require("common/eventsCollector");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import generalUtils = require("common/generalUtils");
import connectionStringRabbitMqModel = require("models/database/settings/connectionStringRabbitMqModel");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import getConnectionStringInfoCommand = require("commands/database/settings/getConnectionStringInfoCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import jsonUtil = require("common/jsonUtil");
import ongoingTaskQueueSinkScriptModel from "models/database/tasks/ongoingTaskQueueSinkScriptModel";
import saveQueueSinkCommand from "commands/database/tasks/saveQueueSinkCommand";
import ongoingTaskRabbitMqSinkEditModel from "models/database/tasks/ongoingTaskRabbitMqSinkEditModel";
import viewHelpers from "common/helpers/view/viewHelpers";
import database from "models/resources/database";
import testQueueSinkCommand from "commands/database/tasks/testQueueSinkCommand";
import getOngoingTaskInfoCommand from "commands/database/tasks/getOngoingTaskInfoCommand";
import queueSinkSyntax from "viewmodels/database/tasks/queueSinkSyntax";
import patchDebugActions from "viewmodels/database/patch/patchDebugActions";
import licenseModel from "models/auth/licenseModel";
import { EditRabbitMqSinkTaskInfoHub } from "./EditRabbitMqSinkTaskInfoHub";
import { sortBy } from "common/typeUtils";

class rabbitMqTaskTestMode {
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

        // on edit RabbitMQ view we want to show documents by default
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

            eventsCollector.default.reportEvent("rabbitmq-sink", "test-script");

            new testQueueSinkCommand(this.db(), dto, "RabbitMq")
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

class editRabbitMqSinkTask extends viewModelBase {

    view = require("views/database/tasks/editRabbitMqSinkTask.html");
    connectionStringView = require("views/database/settings/connectionStringRabbitMq.html");
    taskResponsibleNodeSectionView = require("views/partial/taskResponsibleNodeSection.html");
    pinResponsibleNodeTextScriptView = require("views/partial/pinResponsibleNodeTextScript.html");

    patchDebugActionsLoadedView = require("views/database/patch/patchDebugActionsLoaded.html");
    patchDebugActionsModifiedView = require("views/database/patch/patchDebugActionsModified.html");
    patchDebugActionsDeletedView = require("views/database/patch/patchDebugActionsDeleted.html");

    hasQueueSink = licenseModel.getStatusValue("HasQueueSink");

    static readonly scriptNamePrefix = "Script_";

    enableTestArea = ko.observable<boolean>(false);
    test: rabbitMqTaskTestMode;
    
    infoHubView: ReactInKnockout<typeof EditRabbitMqSinkTaskInfoHub>;
    
    editedRabbitMqSink = ko.observable<ongoingTaskRabbitMqSinkEditModel>();

    isAddingNewRabbitMqSinkTask = ko.observable<boolean>(true);
    
    rabbitMqConnectionStringsDetails = ko.observableArray<Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString>([]);

    possibleMentors = ko.observableArray<string>([]);
    
    spinners = {
        test: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false)
    };
    
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;
    
    createNewConnectionString = ko.observable<boolean>(false);
    newConnectionString = ko.observable<connectionStringRabbitMqModel>();

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
            "cancelEditedScript", "saveEditedScript", "syntaxHelp", "onTestConnectionRabbitMq", "toggleTestArea",
            "setState");

        this.infoHubView = ko.pureComputed(() => ({
            component: EditRabbitMqSinkTaskInfoHub
        }));
    }

    activate(args: any) {
        super.activate(args);
        const deferred = $.Deferred<void>();
        
        this.loadPossibleMentors();

        if (args.taskId) {
            // 1. Editing an Existing task
            this.isAddingNewRabbitMqSinkTask(false);
            
            getOngoingTaskInfoCommand.forQueueSink(this.activeDatabase(), args.taskId)
                .execute()
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueSink) => {
                    this.editedRabbitMqSink(new ongoingTaskRabbitMqSinkEditModel(result));
                    deferred.resolve();
                })
                .fail(() => { 
                    deferred.reject();
                    router.navigate(appUrl.forOngoingTasks(this.activeDatabase())); 
                });
        } else {
            // 2. Creating a New task
            this.isAddingNewRabbitMqSinkTask(true);
            this.editedRabbitMqSink(ongoingTaskRabbitMqSinkEditModel.empty());
            this.editedRabbitMqSink().editedScriptSandbox(ongoingTaskQueueSinkScriptModel.empty(this.findNameForNewScript()));
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

        $('.edit-rabbitmq-sink-etl-task [data-toggle="tooltip"]').tooltip();
    }

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.activeDatabase())
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const queueConnectionStrings = Object.values(result.QueueConnectionStrings);
                const rabbitMqStrings = queueConnectionStrings.filter(x => x.BrokerType === "RabbitMq");
                this.rabbitMqConnectionStringsDetails(sortBy(rabbitMqStrings, x => x.Name.toUpperCase()));
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
        
        this.newConnectionString(connectionStringRabbitMqModel.empty());
        this.newConnectionString().setNameUniquenessValidator(name => !this.rabbitMqConnectionStringsDetails().find(x => x.Name.toLocaleLowerCase() === name.toLocaleLowerCase()));
        
        const connectionStringName = this.editedRabbitMqSink().connectionStringName();
        const connectionStringIsMissing = connectionStringName && !this.rabbitMqConnectionStringsDetails()
            .find(x => x.Name.toLocaleLowerCase() === connectionStringName.toLocaleLowerCase());

        if (!this.rabbitMqConnectionStringsDetails().length || connectionStringIsMissing) {
            this.createNewConnectionString(true);
        }

        if (connectionStringIsMissing) {
            // looks like user imported data w/o connection strings, prefill form with desired name
            this.newConnectionString().connectionStringName(connectionStringName);
            this.editedRabbitMqSink().connectionStringName(null);
        }
        
        // Discard test connection result when needed
        this.createNewConnectionString.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().rabbitMqConnectionString.subscribe(() => this.testConnectionResult(null));

        this.connectionStringDefined = ko.pureComputed(() => {
            const editedEtl = this.editedRabbitMqSink();
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
            const dto = this.editedRabbitMqSink().toDto();

            // override transforms - use only current transformation
            const scriptDto = this.editedRabbitMqSink().editedScriptSandbox().toDto();
            scriptDto.Name = "Script_1"; // assign fake name
            dto.Scripts = [scriptDto];

            if (!dto.Name) {
                dto.Name = "Test RabbitMq Sink Task"; // assign fake name
            }
            return dto;
        };
        
        
        this.test = new rabbitMqTaskTestMode(this.activeDatabase, () => {
            return this.isValid(this.editedRabbitMqSink().editedScriptSandbox().testValidationGroup);
        }, dtoProvider);

        this.test.initObservables();

        this.dirtyFlag = new ko.DirtyFlag([
            this.createNewConnectionString,
            this.newConnectionString().dirtyFlag().isDirty,
            this.editedRabbitMqSink().dirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    useConnectionString(connectionStringToUse: string) {
        this.editedRabbitMqSink().connectionStringName(connectionStringToUse);
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
            getConnectionStringInfoCommand.forRabbitMqEtl(this.activeDatabase(), this.editedRabbitMqSink().connectionStringName())
                .execute()
                .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                    new connectionStringRabbitMqModel(result.QueueConnectionStrings[this.editedRabbitMqSink().connectionStringName()], true, [])
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
        const editedSink = this.editedRabbitMqSink();
        
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

        // 5. All is well, Save RabbitMq Etl task
        savingNewStringAction.done(()=> {
            eventsCollector.default.reportEvent("rabbitmq-sink", "save");
            
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
        this.editedRabbitMqSink().scriptSelectedForEdit(null);
        this.editedRabbitMqSink().editedScriptSandbox(ongoingTaskQueueSinkScriptModel.empty(this.findNameForNewScript()));
    }

    cancelEditedScript() {
        this.editedRabbitMqSink().editedScriptSandbox(null);
        this.editedRabbitMqSink().scriptSelectedForEdit(null);
        this.enableTestArea(false);
    }

    saveEditedScript() {
        this.enableTestArea(false);
        const script = this.editedRabbitMqSink().editedScriptSandbox();
        if (!this.isValid(script.validationGroup)) {
            return;
        }
        
        if (script.isNew()) {
            const newScriptItem = new ongoingTaskQueueSinkScriptModel(script.toDto(), true);
            newScriptItem.name(script.name());
            newScriptItem.dirtyFlag().forceDirty();
            this.editedRabbitMqSink().scripts.push(newScriptItem);
        } else {
            const oldItem = this.editedRabbitMqSink().scriptSelectedForEdit();
            const newItem = new ongoingTaskQueueSinkScriptModel(script.toDto(), false);
            
            if (oldItem.dirtyFlag().isDirty() || newItem.hasUpdates(oldItem)) {
                newItem.dirtyFlag().forceDirty();
            }
            
            this.editedRabbitMqSink().scripts.replace(oldItem, newItem);
        }

        this.editedRabbitMqSink().scripts.sort((a, b) => a.name().toLowerCase().localeCompare(b.name().toLowerCase()));
        this.editedRabbitMqSink().editedScriptSandbox(null);
        this.editedRabbitMqSink().scriptSelectedForEdit(null);
    }
    
    private findNameForNewScript() {
        const scriptsWithPrefix = this.editedRabbitMqSink().scripts().filter(script => {
            return script.name().startsWith(editRabbitMqSinkTask.scriptNamePrefix);
        });
        
        const maxNumber = _.max(scriptsWithPrefix
            .map(x => x.name().substring(editRabbitMqSinkTask.scriptNamePrefix.length))
            .map(x => _.toInteger(x))) || 0;
        
        return editRabbitMqSinkTask.scriptNamePrefix + (maxNumber + 1);
    }

    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
    }

    removeScript(model: ongoingTaskQueueSinkScriptModel) {
        this.editedRabbitMqSink().deleteScript(model);
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
        this.editedRabbitMqSink().taskState(state);
    }
}

export = editRabbitMqSinkTask;

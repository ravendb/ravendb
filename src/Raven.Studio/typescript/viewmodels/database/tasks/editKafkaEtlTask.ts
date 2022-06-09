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
import ongoingTaskKafkaEtlEditModel = require("models/database/tasks/ongoingTaskKafkaEtlEditModel");
import ongoingTaskQueueEtlTransformationModel = require("models/database/tasks/ongoingTaskQueueEtlTransformationModel");
import connectionStringKafkaEtlModel = require("models/database/settings/connectionStringKafkaEtlModel");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import transformationScriptSyntax = require("viewmodels/database/tasks/transformationScriptSyntax");
import getPossibleMentorsCommand = require("commands/database/tasks/getPossibleMentorsCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import jsonUtil = require("common/jsonUtil");
import viewHelpers = require("common/helpers/view/viewHelpers");
import documentMetadata = require("models/database/documents/documentMetadata");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import document = require("models/database/documents/document");
import popoverUtils = require("common/popoverUtils");
import tasksCommonContent = require("models/database/tasks/tasksCommonContent");
import { highlight, languages } from "prismjs";

type resultItem = {
    header: string;
    payload: string;
}

class editKafkaEtlTask extends viewModelBase {

    view = require("views/database/tasks/editKafkaEtlTask.html");
    optionsPerQueueEtlView = require("views/database/tasks/optionsPerQueueEtl.html");
    connectionStringView = require("views/database/settings/connectionStringKafka.html")
    certificateUploadInfoForOngoingTasks = require("views/partial/certificateUploadInfoForOngoingTasks.html");
    
    static readonly scriptNamePrefix = "Script_";
    static isApplyToAll = ongoingTaskQueueEtlTransformationModel.isApplyToAll;

    enableTestArea = ko.observable<boolean>(false);
    showAdvancedOptions = ko.observable<boolean>(false);

    //test: kafkaTaskTestMode; // TODO
    
    editedKafkaEtl = ko.observable<ongoingTaskKafkaEtlEditModel>();
    isAddingNewKafkaEtlTask = ko.observable<boolean>(true);
    
    kafkaEtlConnectionStringsDetails = ko.observableArray<Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString>([]);

    possibleMentors = ko.observableArray<string>([]);

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    
    spinners = {
        test: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false)
    };
    
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;
    
    createNewConnectionString = ko.observable<boolean>(false);
    newConnectionString = ko.observable<connectionStringKafkaEtlModel>();

    collections = collectionsTracker.default.collections;

    usingHttps = location.protocol === "https:";
    certificatesUrl = appUrl.forCertificates();

    constructor() {
        super();
        
        aceEditorBindingHandler.install();
        this.bindToCurrentInstance("useConnectionString", "removeTransformationScript",
                                   "cancelEditedTransformation", "saveEditedTransformation", "syntaxHelp",
                                   "toggleTestArea", "toggleAdvancedArea", "setState");
    }

    activate(args: any) {
        super.activate(args);
        const deferred = $.Deferred<void>();

        if (args.taskId) {
            // 1. Editing an Existing task
            this.isAddingNewKafkaEtlTask(false);
            
            getOngoingTaskInfoCommand.forQueueEtl(this.activeDatabase(), args.taskId)
                .execute()
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlDetails) => {
                    this.editedKafkaEtl(new ongoingTaskKafkaEtlEditModel(result));
                    this.showAdvancedOptions(this.editedKafkaEtl().hasOptions());
                    deferred.resolve();
                })
                .fail(() => { 
                    deferred.reject();
                    router.navigate(appUrl.forOngoingTasks(this.activeDatabase())); 
                });
        } else {
            // 2. Creating a New task
            this.isAddingNewKafkaEtlTask(true);
            this.editedKafkaEtl(ongoingTaskKafkaEtlEditModel.empty());
            this.editedKafkaEtl().editedTransformationScriptSandbox(ongoingTaskQueueEtlTransformationModel.empty(this.findNameForNewTransformation()));
            deferred.resolve();
        }

        return $.when<any>(this.getAllConnectionStrings(), this.loadPossibleMentors(), deferred)
            .done(() => {
                this.initObservables();
            })
    }

    private loadPossibleMentors() {
        return new getPossibleMentorsCommand(this.activeDatabase().name)
            .execute()
            .done(mentors => this.possibleMentors(mentors));
    }
    
    compositionComplete() {
        super.compositionComplete();

        $('.edit-kafka-etl-task [data-toggle="tooltip"]').tooltip();

        popoverUtils.longWithHover($(".responsible-node"),
            {
                content: tasksCommonContent.responsibleNodeInfo
            });
    }

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.activeDatabase())
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const queueConnectionStrings = Object.values(result.QueueConnectionStrings);
                const kafkaStrings = queueConnectionStrings.filter(x => x.BrokerType === "Kafka");
                this.kafkaEtlConnectionStringsDetails(_.sortBy(kafkaStrings, x => x.Name.toUpperCase()));
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
        
        this.newConnectionString(connectionStringKafkaEtlModel.empty());
        this.newConnectionString().setNameUniquenessValidator(name => !this.kafkaEtlConnectionStringsDetails().find(x => x.Name.toLocaleLowerCase() === name.toLocaleLowerCase()));
        
        const connectionStringName = this.editedKafkaEtl().connectionStringName();
        const connectionStringIsMissing = connectionStringName && !this.kafkaEtlConnectionStringsDetails()
            .find(x => x.Name.toLocaleLowerCase() === connectionStringName.toLocaleLowerCase());

        if (!this.kafkaEtlConnectionStringsDetails().length || connectionStringIsMissing) {
            this.createNewConnectionString(true);
        }

        if (connectionStringIsMissing) {
            // looks like user imported data w/o connection strings, prefill form with desired name
            this.newConnectionString().connectionStringName(connectionStringName);
            this.editedKafkaEtl().connectionStringName(null);
        }
        
        // Discard test connection result when needed
        this.createNewConnectionString.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().kafkaServerUrl.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().useRavenCertificate.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().connectionOptions.subscribe(() => this.testConnectionResult(null));

        this.enableTestArea.subscribe(testMode => {
            $("body").toggleClass('show-test', testMode);
        });

        const dtoProvider = () => {
            const dto = this.editedKafkaEtl().toDto();

            // override transforms - use only current transformation
            const transformationScriptDto = this.editedKafkaEtl().editedTransformationScriptSandbox().toDto();
            transformationScriptDto.Name = "Script_1"; // assign fake name
            dto.Transforms = [transformationScriptDto];

            if (!dto.Name) {
                dto.Name = "Test Kafka ETL Task"; // assign fake name
            }
            return dto;
        };

        this.dirtyFlag = new ko.DirtyFlag([
            this.createNewConnectionString,
            this.newConnectionString().dirtyFlag().isDirty,
            this.editedKafkaEtl().dirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    useConnectionString(connectionStringToUse: string) {
        this.editedKafkaEtl().connectionStringName(connectionStringToUse);
    }

    saveKafkaEtl() {
        let hasAnyErrors = false;
        this.spinners.save(true);
        let editedEtl = this.editedKafkaEtl();
        
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

            let validOptions = true;
            this.newConnectionString().connectionOptions().forEach(x => {
                validOptions = this.isValid(x.validationGroup);
            })
            
            if (!this.isValid(this.newConnectionString().validationGroup) || !validOptions) {
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

        // 5. All is well, Save Kafka Etl task
        savingNewStringAction.done(()=> {
            eventsCollector.default.reportEvent("kafka-etl", "save");
            
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
        this.editedKafkaEtl().transformationScriptSelectedForEdit(null);
        this.editedKafkaEtl().editedTransformationScriptSandbox(ongoingTaskQueueEtlTransformationModel.empty(this.findNameForNewTransformation()));
    }

    cancelEditedTransformation() {
        this.editedKafkaEtl().editedTransformationScriptSandbox(null);
        this.editedKafkaEtl().transformationScriptSelectedForEdit(null);
        this.enableTestArea(false);
    }

    saveEditedTransformation() {
        this.enableTestArea(false);
        const transformation = this.editedKafkaEtl().editedTransformationScriptSandbox();
        if (!this.isValid(transformation.validationGroup)) {
            return;
        }
        
        if (transformation.isNew()) {
            const newTransformationItem = new ongoingTaskQueueEtlTransformationModel(transformation.toDto(), true, false); 
            newTransformationItem.name(transformation.name());
            newTransformationItem.dirtyFlag().forceDirty();
            this.editedKafkaEtl().transformationScripts.push(newTransformationItem);
        } else {
            const oldItem = this.editedKafkaEtl().transformationScriptSelectedForEdit();
            const newItem = new ongoingTaskQueueEtlTransformationModel(transformation.toDto(), false, transformation.resetScript());
            
            if (oldItem.dirtyFlag().isDirty() || newItem.hasUpdates(oldItem)) {
                newItem.dirtyFlag().forceDirty();
            }
            
            this.editedKafkaEtl().transformationScripts.replace(oldItem, newItem);
        }

        this.editedKafkaEtl().transformationScripts.sort((a, b) => a.name().toLowerCase().localeCompare(b.name().toLowerCase()));
        this.editedKafkaEtl().editedTransformationScriptSandbox(null);
        this.editedKafkaEtl().transformationScriptSelectedForEdit(null);
    }
    
    private findNameForNewTransformation() {
        const scriptsWithPrefix = this.editedKafkaEtl().transformationScripts().filter(script => {
            return script.name().startsWith(editKafkaEtlTask.scriptNamePrefix);
        });
        
        const maxNumber = _.max(scriptsWithPrefix
            .map(x => x.name().substr(editKafkaEtlTask.scriptNamePrefix.length))
            .map(x => _.toInteger(x))) || 0;
        
        return editKafkaEtlTask.scriptNamePrefix + (maxNumber + 1);
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
            
            if (!_.includes(this.editedKafkaEtl().editedTransformationScriptSandbox().transformScriptCollections(), ongoingTaskQueueEtlTransformationModel.applyToAllCollectionsText)) {
                result.unshift(ongoingTaskQueueEtlTransformationModel.applyToAllCollectionsText);
            }
            
            return result;
        });
    }

    removeTransformationScript(model: ongoingTaskQueueEtlTransformationModel) {
        this.editedKafkaEtl().deleteTransformationScript(model);
    }

    syntaxHelp() {
        const viewmodel = new transformationScriptSyntax("Queue");
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
        this.editedKafkaEtl().taskState(state);
    }
}

export = editKafkaEtlTask;

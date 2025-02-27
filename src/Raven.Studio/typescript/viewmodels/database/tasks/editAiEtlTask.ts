import app = require("durandal/app");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import database = require("models/resources/database");
import getOngoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import eventsCollector = require("common/eventsCollector");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import saveEtlTaskCommand = require("commands/database/tasks/saveEtlTaskCommand");
import ongoingTaskAiTransformationModel = require("models/database/tasks/ongoingTaskAiTransformationModel");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import transformationScriptSyntax = require("viewmodels/database/tasks/transformationScriptSyntax");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import jsonUtil = require("common/jsonUtil");
import document = require("models/database/documents/document");
import viewHelpers = require("common/helpers/view/viewHelpers");
import documentMetadata = require("models/database/documents/documentMetadata");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import testAiCommand = require("commands/database/tasks/testAiCommand");
import prismjs = require("prismjs");
import shardViewModelBase = require("viewmodels/shardViewModelBase");
import licenseModel = require("models/auth/licenseModel");
import EditAiEtlInfoHub = require("viewmodels/database/tasks/EditAiEtlInfoHub");
import typeUtils = require("common/typeUtils");
import ongoingTaskAiEtlEditModel = require("models/database/tasks/ongoingTaskAiEtlEditModel");
import EditConnectionStrings = require("components/pages/database/settings/connectionStrings/EditConnectionStrings");
import connectionStringsSlice = require("components/pages/database/settings/connectionStrings/store/connectionStringsSlice");
import storeCompat = require("components/storeCompat");

class aiEtlTask extends shardViewModelBase {
    
    view = require("views/database/tasks/editAiEtlTask.html");
    taskResponsibleNodeSectionView = require("views/partial/taskResponsibleNodeSection.html");
    pinResponsibleNodeTextScriptView = require("views/partial/pinResponsibleNodeTextScript.html");

    static readonly scriptNamePrefix = "Script_";
    static isApplyToAll = ongoingTaskAiTransformationModel.isApplyToAll;
    
    enableTestArea = ko.observable<boolean>(false);
    test: aiTaskTestMode;    

    editedAiEtl = ko.observable<ongoingTaskAiEtlEditModel>();
    isAddingNewEtlTask = ko.observable<boolean>(true);

    transformationScriptSelectedForEdit = ko.observable<ongoingTaskAiTransformationModel>();
    editedTransformationScriptSandbox = ko.observable<ongoingTaskAiTransformationModel>();

    possibleMentors = ko.observableArray<string>([]);
    connectionStringsNames = ko.observableArray<string>([]);
    aiConnectionStrings = ko.observableArray<Raven.Client.Documents.Operations.AI.AiConnectionString>([]);

    spinners = {
        test: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false)
    };

    collections = collectionsTracker.default.collections;
    
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;
    
    collectionNames: KnockoutComputed<string[]>;

    showEditTransformationArea: KnockoutComputed<boolean>;
   
    hasAiIntegration = licenseModel.getStatusValue("HasAiIntegration");
    infoHubView: ReactInKnockout<typeof EditAiEtlInfoHub.EditAiEtlInfoHub>;

    isNewConnectionStringOpen = ko.observable<boolean>(false);
    newConnectionStringView: ReactInKnockout<typeof EditConnectionStrings.default>;

    sourceView = ko.observable<EditAiTaskSourceView>();

    constructor(db: database) {
        super(db);
        this.bindToCurrentInstance("useConnectionString",
            "saveEditedTransformation",
            "syntaxHelp",
            "toggleTestArea",
            "toggleIsNewConnectionStringOpen",
            "setState");

        
        aceEditorBindingHandler.install();

        this.infoHubView = ko.pureComputed(() => ({
            component: EditAiEtlInfoHub.EditAiEtlInfoHub
        }));

        this.newConnectionStringView = ko.pureComputed(() => ({
            component: EditConnectionStrings.default,
            props: {
                initialConnection: {
                    type: "Ai"
                },
                afterSave: async (name: string) => {
                    await this.getAllConnectionStrings();
                    this.editedAiEtl().connectionStringName(name)
                    this.toggleIsNewConnectionStringOpen();
                },
                afterClose: () => {
                    this.toggleIsNewConnectionStringOpen();
                }
            }
        }))
    }

    activate(args: { taskId?: number, sourceView: EditAiTaskSourceView }) {
        super.activate(args);
        const deferred = $.Deferred<void>();

        storeCompat.globalDispatch(connectionStringsSlice.connectionStringsActions.viewContextSet("ai"));
        this.sourceView(args.sourceView);
        
        this.loadPossibleMentors();

        if (args.taskId) {
            // 1. Editing an Existing task
            this.isAddingNewEtlTask(false);

            getOngoingTaskInfoCommand.forAiIntegration(this.db, args.taskId)
                .execute()
                .done((result) => {
                    this.editedAiEtl(new ongoingTaskAiEtlEditModel(result, this.aiConnectionStrings));

                    this.editTransformationScript(new ongoingTaskAiTransformationModel(
                        result.Configuration.Transforms[0],
                        false,
                        true,
                        result.Configuration.EmbeddingsPathConfigurations?.length > 0 ? "paths" : "script",
                        result.Configuration.EmbeddingsPathConfigurations,
                        this.editedAiEtl().maxTokensPerChunkDefaultValue
                    ));

                    deferred.resolve();
                })
                .fail(() => {
                    deferred.reject();
                    router.navigate(appUrl.forOngoingTasks(this.db));
                });
        } else {
            // 2. Creating a New task
            this.isAddingNewEtlTask(true);
            this.editedAiEtl(ongoingTaskAiEtlEditModel.empty(this.aiConnectionStrings));

            this.editedTransformationScriptSandbox(ongoingTaskAiTransformationModel.empty(this.editedAiEtl().maxTokensPerChunkDefaultValue, this.findNameForNewTransformation()));

            deferred.resolve();
        }
        
        return $.when<any>(this.getAllConnectionStrings(), deferred)
            .done(() => {
                this.initObservables();
            });
    }

    private loadPossibleMentors() {
        const members = this.db.nodes()
            .filter(x => x.type === "Member")
            .map(x => x.tag);

        this.possibleMentors(members);
    }
    
    compositionComplete() {
        super.compositionComplete();

        $('.edit-ai-task [data-toggle="tooltip"]').tooltip();
    }

    toggleIsNewConnectionStringOpen() {
        this.isNewConnectionStringOpen(!this.isNewConnectionStringOpen())
    }

    /**************************************************************/
    /*** General AI Model / Page Actions Region ***/
    /**************************************************************/

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.db)
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                this.aiConnectionStrings(Object.values(result.AiConnectionStrings));

                const connectionStringsNames = Object.keys(result.AiConnectionStrings);
                this.connectionStringsNames(typeUtils.sortBy(connectionStringsNames, x => x.toUpperCase()));
            });
    }

    private initObservables() {

        this.collectionNames = ko.pureComputed(() => {
            return collectionsTracker.default.getCollectionNames();
        });

        this.showEditTransformationArea = ko.pureComputed(() => !!this.editedTransformationScriptSandbox());

        const connectionStringName = this.editedAiEtl().connectionStringName();
        const connectionStringIsMissing = connectionStringName && !this.connectionStringsNames()
            .find(x => x.toLocaleLowerCase() === connectionStringName.toLocaleLowerCase());

        if (connectionStringIsMissing) {
            // looks like user imported data w/o connection strings, prefill form with desired name
            this.editedAiEtl().connectionStringName(null);
        }

        this.enableTestArea.subscribe(testMode => {
            $("body").toggleClass('show-test', testMode);
        });

        const dtoProvider = () => {
            const dto = this.editedAiEtl().toDto();

            // override transforms - use only current transformation
            const transformationScriptDto = this.editedTransformationScriptSandbox().toDto();
            transformationScriptDto.Name = "Configuration"; // assign fake name
            dto.Transforms = [transformationScriptDto];

            if (!dto.Name) {
                dto.Name = "Test AI Task"; // assign fake name
            }
            return dto;
        };

        this.test = new aiTaskTestMode(this.db, () => {
            return this.isValid(this.editedTransformationScriptSandbox().validationGroup);
        }, dtoProvider);
                
        this.test.initObservables();

        this.initDirtyFlag();
    }
    
    private initDirtyFlag() {
        const innerDirtyFlag = ko.pureComputed(() => this.editedAiEtl().dirtyFlag().isDirty());
        const editedScriptFlag = ko.pureComputed(() => !!this.editedTransformationScriptSandbox() && this.editedTransformationScriptSandbox().dirtyFlag().isDirty());

        const scriptsCount = ko.pureComputed(() => this.editedAiEtl().transformationScripts().length);
        
        const hasAnyDirtyTransformationScript = ko.pureComputed(() => {
            let anyDirty = false;
            this.editedAiEtl().transformationScripts().forEach(script => {
                if (script.dirtyFlag().isDirty()) {
                    anyDirty = true;
                    // don't break here - we want to track all dependencies
                }
            });
            return anyDirty;
        });
        

        this.dirtyFlag = new ko.DirtyFlag([
            innerDirtyFlag,
            editedScriptFlag,
            scriptsCount,
            hasAnyDirtyTransformationScript
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    useConnectionString(connectionStringToUse: string) {
        this.editedAiEtl().connectionStringName(connectionStringToUse);
    }

    // onTestConnectionElasticSearch(urlToTest: discoveryUrl) {
    //     eventsCollector.default.reportEvent("ai-connection-string", "test-connection");
    //     this.spinners.test(true);
    //     this.testConnectionResult(null);
    //     this.newConnectionString().selectedUrlToTest(urlToTest.discoveryUrlName());

    //     this.newConnectionString()
    //         .testConnection(this.activeDatabase(), urlToTest)
    //         .done(result => this.testConnectionResult(result))
    //         .always(() => {
    //             this.spinners.test(false);
    //             this.fullErrorDetailsVisible(false);
    //         });
    // }

    saveEtl() {
        let hasAnyErrors = false;
        this.spinners.save(true);
        
        // 2. Validate *edited transformation script*
        if (this.showEditTransformationArea()) {
            if (!this.isValid(this.editedTransformationScriptSandbox().validationGroup)) {
                hasAnyErrors = true;
            } else {
                this.saveEditedTransformation();
            }
        }

        // 4. Validate *general form*
        if (!this.isValid(this.editedAiEtl().validationGroup)) {
            hasAnyErrors = true;
        }
        
        if (hasAnyErrors) {
            this.spinners.save(false);
            return false;
        }
        
        const scriptsToReset = this.editedAiEtl()
                .transformationScripts()
                .filter(x => x.resetScript())
                .map(x => x.name());
            
        const dto = this.editedAiEtl().toDto();
        saveEtlTaskCommand.forAiIntegration(this.db, dto, scriptsToReset)
            .execute()
            .done(() => {
                this.dirtyFlag().reset();
                this.goToOngoingTasksView();
                })
            .always(() => this.spinners.save(false));
    }

    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        if (this.sourceView() === "AiTasks") {
            router.navigate(appUrl.forAiTasks(this.db));
        } else {
            router.navigate(appUrl.forOngoingTasks(this.db));
        }
    }

    syntaxHelp() {
        const viewmodel = new transformationScriptSyntax("Ai");
        app.showBootstrapDialog(viewmodel);
    }
    
    toggleTestArea() {
        if (!this.enableTestArea()) {
            // let hasErrors = false;

            // validate global form - but only 'enterTestModeValidationGroup'
            // if (!this.isValid(this.editedAiEtl().enterTestModeValidationGroup)) {
            //     hasErrors = true;
            // }
            
            // if (!hasErrors) {
                this.enableTestArea(true);
            // }
        } else {
            this.enableTestArea(false);
        }
    }

    setState(state: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState): void {
        this.editedAiEtl().taskState(state);
    }

    /********************************************/
    /*** Transformation Script Actions Region ***/
    /********************************************/

    addNewTransformation() {
        this.transformationScriptSelectedForEdit(null);
        this.editedTransformationScriptSandbox(ongoingTaskAiTransformationModel.empty(this.editedAiEtl().maxTokensPerChunkDefaultValue, this.findNameForNewTransformation()));
    }
    
    saveEditedTransformation() {
        this.enableTestArea(false);
        const transformation = this.editedTransformationScriptSandbox();

        if (!this.isValid(transformation.validationGroup)) {
            return;
        }

        if (transformation.isNew()) {
            const newTransformationItem = new ongoingTaskAiTransformationModel(transformation.toDto(), false, false, transformation.embeddingsSource(), transformation.embeddingPathConfigurations(), this.editedAiEtl().maxTokensPerChunkDefaultValue);
            newTransformationItem.name(transformation.name());
            newTransformationItem.dirtyFlag().forceDirty();
            this.editedAiEtl().transformationScripts.push(newTransformationItem);
        } else {
            const oldItem = this.editedAiEtl().transformationScripts().find(x => x.name() === transformation.name());
            const newItem = new ongoingTaskAiTransformationModel(transformation.toDto(), false, transformation.resetScript(), transformation.embeddingsSource(), transformation.embeddingPathConfigurations(), this.editedAiEtl().maxTokensPerChunkDefaultValue);

            if (oldItem.dirtyFlag().isDirty() || newItem.hasUpdates(oldItem)) {
                newItem.dirtyFlag().forceDirty();
            }

            this.editedAiEtl().transformationScripts.replace(oldItem, newItem);
        }

        this.editedAiEtl().transformationScripts.sort((a, b) => a.name().toLowerCase().localeCompare(b.name().toLowerCase()));
    }

    private findNameForNewTransformation() {
        const scriptsWithPrefix = this.editedAiEtl().transformationScripts().filter(script => {
            return script.name().startsWith(aiEtlTask.scriptNamePrefix);
        });

        const maxNumber = _.max(scriptsWithPrefix
            .map(x => x.name().substr(aiEtlTask.scriptNamePrefix.length))
            .map(x => _.toInteger(x))) || 0;

        return aiEtlTask.scriptNamePrefix + (maxNumber + 1);
    }

    editTransformationScript(model: ongoingTaskAiTransformationModel) {
        this.makeSureSandboxIsVisible();
        this.transformationScriptSelectedForEdit(model);
        this.editedTransformationScriptSandbox(new ongoingTaskAiTransformationModel(model.toDto(), false, model.resetScript(), model.embeddingsSource(), model.embeddingPathConfigurations(), this.editedAiEtl().maxTokensPerChunkDefaultValue));

        $('.edit-ai-task .js-test-area [data-toggle="tooltip"]').tooltip();
    }
    
    private makeSureSandboxIsVisible() {
        const $editArea = $(".edit-ai-task");
        if ($editArea.scrollTop() > 300) {
            $editArea.scrollTop(0);
        }
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

            return result;
        });
    }
}

export = aiEtlTask;

class aiTaskTestMode {

    documentId = ko.observable<string>();
    testDelete = ko.observable<boolean>(false);
    docsIdsAutocompleteResults = ko.observableArray<string>([]);
    db: database;
    configurationProvider: () => Raven.Client.Documents.Operations.AI.EmbeddingsGenerationConfiguration;

    validationGroup: KnockoutValidationGroup;
    validateParent: () => boolean;

    testAlreadyExecuted = ko.observable<boolean>(false);

    spinners = {
        preview: ko.observable<boolean>(false),
        test: ko.observable<boolean>(false)
    };

    loadedDocument = ko.observable<string>();
    loadedDocumentId = ko.observable<string>();

    debugOutput = ko.observableArray<string>([]);
    testResults = ko.observableArray<Raven.Server.Documents.ETL.Providers.AI.Embeddings.Test.EmbeddingsGenerationTestScriptResult>([]);

    // all kinds of alerts:
    transformationErrors = ko.observableArray<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>([]);

    warningsCount = ko.pureComputed(() => {
        return this.transformationErrors().length;
    });

    constructor(db: database,
                validateParent: () => boolean,
                configurationProvider: () => Raven.Client.Documents.Operations.AI.EmbeddingsGenerationConfiguration) {
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
                            this.loadedDocument(prismjs.highlight(text, prismjs.languages.javascript, "js"));
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

            const dto: Raven.Server.Documents.ETL.Providers.ElasticSearch.Test.TestElasticSearchEtlScript = {
                DocumentId: this.documentId(),
                IsDelete: this.testDelete(),
                Configuration: this.configurationProvider()
            };

            eventsCollector.default.reportEvent("ai-etl", "test-script");

            new testAiCommand(this.db, dto)
                .execute()
                .done(simulationResult => {
                    console.log('kalczur ', simulationResult);
                    // TODO kalczur
                    // const summaryFormatted =  simulationResult.Summary.map(x => ({
                    //     Commands: x.Commands.map((cmd: string) => cmd.replace(/\r\n/g, "\n")),
                    //     IndexName: x.IndexName
                    // }));
                    
                    // this.testResults(summaryFormatted);
                    
                    // this.debugOutput(simulationResult.DebugOutput);
                    // this.transformationErrors(simulationResult.TransformationErrors);

                    // if (this.warningsCount()) {
                    //     $('.test-container a[href="#warnings"]').tab('show');
                    // } else {
                    //     $('.test-container a[href="#testResults"]').tab('show');
                    // }

                    this.testAlreadyExecuted(true);
                })
                .always(() => this.spinners.test(false));
        }
    }
}

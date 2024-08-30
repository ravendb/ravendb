import app = require("durandal/app");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import database = require("models/resources/database");
import ongoingTaskElasticSearchEtlEditModel = require("models/database/tasks/ongoingTaskElasticSearchEtlEditModel");
import getOngoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import eventsCollector = require("common/eventsCollector");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import saveEtlTaskCommand = require("commands/database/tasks/saveEtlTaskCommand");
import generalUtils = require("common/generalUtils");
import ongoingTaskElasticSearchEtlTransformationModel = require("models/database/tasks/ongoingTaskElasticSearchEtlTransformationModel");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import transformationScriptSyntax = require("viewmodels/database/tasks/transformationScriptSyntax");
import ongoingTaskElasticSearchEtlIndexModel = require("models/database/tasks/ongoingTaskElasticSearchEtlIndexModel");
import connectionStringElasticSearchEtlModel = require("models/database/settings/connectionStringElasticSearchEtlModel");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import jsonUtil = require("common/jsonUtil");
import document = require("models/database/documents/document");
import viewHelpers = require("common/helpers/view/viewHelpers");
import documentMetadata = require("models/database/documents/documentMetadata");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import testElasticSearchEtlCommand = require("commands/database/tasks/testElasticSearchEtlCommand");
import ongoingTaskElasticSearchTransformationModel = require("models/database/tasks/ongoingTaskElasticSearchEtlTransformationModel");
import discoveryUrl = require("models/database/settings/discoveryUrl");
import { highlight, languages } from "prismjs";
import shardViewModelBase from "viewmodels/shardViewModelBase";
import licenseModel from "models/auth/licenseModel";
import { EditElasticSearchEtlInfoHub } from "viewmodels/database/tasks/EditElasticSearchEtlInfoHub";
import { sortBy } from "common/typeUtils";
class elasticSearchTaskTestMode {

    documentId = ko.observable<string>();
    testDelete = ko.observable<boolean>(false);
    docsIdsAutocompleteResults = ko.observableArray<string>([]);
    db: database;
    configurationProvider: () => Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchEtlConfiguration;

    validationGroup: KnockoutValidationGroup;
    validateParent: () => boolean;

    testAlreadyExecuted = ko.observable<boolean>(false);

    spinners = {
        preview: ko.observable<boolean>(false),
        test: ko.observable<boolean>(false)
    };

    loadedDocument = ko.observable<string>();
    loadedDocumentId = ko.observable<string>();

    testResults = ko.observableArray<Raven.Server.Documents.ETL.Providers.ElasticSearch.Test.IndexSummary>([]);
    debugOutput = ko.observableArray<string>([]);

    // all kinds of alerts:
    transformationErrors = ko.observableArray<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>([]);

    warningsCount = ko.pureComputed(() => {
        return this.transformationErrors().length;
    });

    constructor(db: database,
                validateParent: () => boolean,
                configurationProvider: () => Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchEtlConfiguration) {
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

            const dto: Raven.Server.Documents.ETL.Providers.ElasticSearch.Test.TestElasticSearchEtlScript = {
                DocumentId: this.documentId(),
                IsDelete: this.testDelete(),
                Configuration: this.configurationProvider()
            };

            eventsCollector.default.reportEvent("elastic-search-etl", "test-script");

            new testElasticSearchEtlCommand(this.db, dto)
                .execute()
                .done(simulationResult => {
                    const summaryFormatted =  simulationResult.Summary.map(x => ({
                        Commands: x.Commands.map((cmd: string) => cmd.replace(/\r\n/g, "\n")),
                        IndexName: x.IndexName
                    }));
                    
                    this.testResults(summaryFormatted);
                    
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

class editElasticSearchEtlTask extends shardViewModelBase {
    
    view = require("views/database/tasks/editElasticSearchEtlTask.html");
    connectionStringView = require("views/database/settings/connectionStringElasticSearch.html");
    taskResponsibleNodeSectionView = require("views/partial/taskResponsibleNodeSection.html");
    pinResponsibleNodeTextScriptView = require("views/partial/pinResponsibleNodeTextScript.html");

    static readonly scriptNamePrefix = "Script_";
    static isApplyToAll = ongoingTaskElasticSearchTransformationModel.isApplyToAll;
    
    enableTestArea = ko.observable<boolean>(false);
    test: elasticSearchTaskTestMode;    

    editedElasticSearchEtl = ko.observable<ongoingTaskElasticSearchEtlEditModel>();
    isAddingNewElasticSearchEtlTask = ko.observable<boolean>(true);

    transformationScriptSelectedForEdit = ko.observable<ongoingTaskElasticSearchEtlTransformationModel>();
    editedTransformationScriptSandbox = ko.observable<ongoingTaskElasticSearchEtlTransformationModel>();

    elasticSearchIndexSelectedForEdit = ko.observable<ongoingTaskElasticSearchEtlIndexModel>();
    editedElasticSearchIndexSandbox = ko.observable<ongoingTaskElasticSearchEtlIndexModel>();

    possibleMentors = ko.observableArray<string>([]);
    elasticSearchEtlConnectionStringsNames = ko.observableArray<string>([]);

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    
    spinners = {
        test: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false)
    };

    collections = collectionsTracker.default.collections;
    
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;
    
    collectionNames: KnockoutComputed<string[]>;

    showEditTransformationArea: KnockoutComputed<boolean>;
    showEditElasticSearchIndexArea: KnockoutComputed<boolean>;

    createNewConnectionString = ko.observable<boolean>(false);
    newConnectionString = ko.observable<connectionStringElasticSearchEtlModel>();
    
    hasElasticSearchEtl = licenseModel.getStatusValue("HasElasticSearchEtl");
    infoHubView: ReactInKnockout<typeof EditElasticSearchEtlInfoHub>;

    constructor(db: database) {
        super(db);
        this.bindToCurrentInstance("useConnectionString",
            "onTestConnectionElasticSearch",
            "removeTransformationScript",
            "cancelEditedTransformation",
            "cancelEditedElasticSearchIndex",
            "saveEditedTransformation",
            "saveEditedElasticSearchIndex",
            "syntaxHelp",
            "toggleTestArea",
            "deleteElasticSearchIndex",
            "editElasticSearchIndex",
            "setState");

        aceEditorBindingHandler.install();
        this.infoHubView = ko.pureComputed(() => ({
            component: EditElasticSearchEtlInfoHub
        }))
    }

    activate(args: any) {
        super.activate(args);
        const deferred = $.Deferred<void>();
        
        this.loadPossibleMentors();

        if (args.taskId) {
            // 1. Editing an Existing task
            this.isAddingNewElasticSearchEtlTask(false);

            getOngoingTaskInfoCommand.forElasticSearchEtl(this.db, args.taskId)
                .execute()
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtl) => {
                    this.editedElasticSearchEtl(new ongoingTaskElasticSearchEtlEditModel(result));
                    deferred.resolve();
                })
                .fail(() => {
                    deferred.reject();
                    router.navigate(appUrl.forOngoingTasks(this.db));
                });
        } else {
            // 2. Creating a New task
            this.isAddingNewElasticSearchEtlTask(true);
            this.editedElasticSearchEtl(ongoingTaskElasticSearchEtlEditModel.empty());

            this.editedTransformationScriptSandbox(ongoingTaskElasticSearchEtlTransformationModel.empty(this.findNameForNewTransformation()));
            this.editedElasticSearchIndexSandbox(ongoingTaskElasticSearchEtlIndexModel.empty());

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

        $('.edit-elastic-search-task [data-toggle="tooltip"]').tooltip();
    }

    /**************************************************************/
    /*** General Elastic Search ETl Model / Page Actions Region ***/
    /**************************************************************/

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.db)
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const connectionStringsNames = Object.keys(result.ElasticSearchConnectionStrings);
                this.elasticSearchEtlConnectionStringsNames(sortBy(connectionStringsNames, x => x.toUpperCase()));
            });
    }

    private initObservables() {
        // Discard test connection result when connection string has changed
        this.editedElasticSearchEtl().connectionStringName.subscribe(() => this.testConnectionResult(null));

        this.shortErrorText = ko.pureComputed(() => {
            const result = this.testConnectionResult();
            if (!result || result.Success) {
                return "";
            }
            return generalUtils.trimMessage(result.Error);
        });

        this.collectionNames = ko.pureComputed(() => {
            return collectionsTracker.default.getCollectionNames();
        });

        this.showEditElasticSearchIndexArea = ko.pureComputed(() => !!this.editedElasticSearchIndexSandbox());
        this.showEditTransformationArea = ko.pureComputed(() => !!this.editedTransformationScriptSandbox());

        this.newConnectionString(connectionStringElasticSearchEtlModel.empty());
        this.newConnectionString().setNameUniquenessValidator(name => !this.elasticSearchEtlConnectionStringsNames().find(x => x.toLocaleLowerCase() === name.toLocaleLowerCase()));

        const connectionStringName = this.editedElasticSearchEtl().connectionStringName();
        const connectionStringIsMissing = connectionStringName && !this.elasticSearchEtlConnectionStringsNames()
            .find(x => x.toLocaleLowerCase() === connectionStringName.toLocaleLowerCase());

        if (!this.elasticSearchEtlConnectionStringsNames().length || connectionStringIsMissing) {
            this.createNewConnectionString(true);
        }

        if (connectionStringIsMissing) {
            // looks like user imported data w/o connection strings, prefill form with desired name
            this.newConnectionString().connectionStringName(connectionStringName);
            this.editedElasticSearchEtl().connectionStringName(null);
        }

        this.enableTestArea.subscribe(testMode => {
            $("body").toggleClass('show-test', testMode);
        });

        const dtoProvider = () => {
            const dto = this.editedElasticSearchEtl().toDto();

            // override transforms - use only current transformation
            const transformationScriptDto = this.editedTransformationScriptSandbox().toDto();
            transformationScriptDto.Name = "Script_1"; // assign fake name
            dto.Transforms = [transformationScriptDto];

            if (!dto.Name) {
                dto.Name = "Test Elasticsearch Task"; // assign fake name
            }
            return dto;
        };

        this.test = new elasticSearchTaskTestMode(this.db, () => {
            return this.isValid(this.editedTransformationScriptSandbox().validationGroup);
        }, dtoProvider);
                
        this.test.initObservables();

        this.initDirtyFlag();
    }
    
    private initDirtyFlag() {
        const innerDirtyFlag = ko.pureComputed(() => this.editedElasticSearchEtl().dirtyFlag().isDirty());
        const editedScriptFlag = ko.pureComputed(() => !!this.editedTransformationScriptSandbox() && this.editedTransformationScriptSandbox().dirtyFlag().isDirty());
        const editedElasticSearchIndexFlag = ko.pureComputed(() => !!this.editedElasticSearchIndexSandbox() && this.editedElasticSearchIndexSandbox().dirtyFlag().isDirty());

        const scriptsCount = ko.pureComputed(() => this.editedElasticSearchEtl().transformationScripts().length);
        const tablesCount = ko.pureComputed(() => this.editedElasticSearchEtl().elasticIndexes().length);
        
        const hasAnyDirtyTransformationScript = ko.pureComputed(() => {
            let anyDirty = false;
            this.editedElasticSearchEtl().transformationScripts().forEach(script => {
                if (script.dirtyFlag().isDirty()) {
                    anyDirty = true;
                    // don't break here - we want to track all dependencies
                }
            });
            return anyDirty;
        });
        
        const hasAnyDirtySqlTable = ko.pureComputed(() => {
            let anyDirty = false;
            this.editedElasticSearchEtl().elasticIndexes().forEach(index => {
                if (index.dirtyFlag().isDirty()) {
                    anyDirty = true;
                    // don't break here - we want to track all dependencies
                }
            });
            return anyDirty;
        });

        this.dirtyFlag = new ko.DirtyFlag([
            innerDirtyFlag,
            editedScriptFlag,
            editedElasticSearchIndexFlag,
            scriptsCount,
            tablesCount,
            hasAnyDirtyTransformationScript,
            hasAnyDirtySqlTable,
            this.createNewConnectionString,
            this.newConnectionString().dirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    useConnectionString(connectionStringToUse: string) {
        this.editedElasticSearchEtl().connectionStringName(connectionStringToUse);
    }

    onTestConnectionElasticSearch(urlToTest: discoveryUrl) {
        eventsCollector.default.reportEvent("elastic-search-connection-string", "test-connection");
        this.spinners.test(true);
        this.testConnectionResult(null);
        this.newConnectionString().selectedUrlToTest(urlToTest.discoveryUrlName());

        this.newConnectionString()
            .testConnection(this.activeDatabase(), urlToTest)
            .done(result => this.testConnectionResult(result))
            .always(() => {
                this.spinners.test(false);
                this.fullErrorDetailsVisible(false);
            });
    }

    saveElasticSearchEtl() {
        let hasAnyErrors = false;
        this.spinners.save(true);
        
        // 1. Validate *edited elastic search index*
        if (!this.editedElasticSearchEtl().elasticIndexes().length) {
            hasAnyErrors = true;
        }
        
        if (this.showEditElasticSearchIndexArea()) {
            if (!this.isValid(this.editedElasticSearchIndexSandbox().validationGroup)) {
                hasAnyErrors = true;
            } else {
                this.saveEditedElasticSearchIndex();
            }
        }
        
        // 2. Validate *edited transformation script*
        if (this.showEditTransformationArea()) {
            if (!this.isValid(this.editedTransformationScriptSandbox().validationGroup)) {
                hasAnyErrors = true;
            } else {
                this.saveEditedTransformation();
            }
        }
        
        // 3. Validate *new connection string* (if relevant..)
        if (this.createNewConnectionString()) {
            const newConnectionString = this.newConnectionString();
            
            if (!this.isValid(newConnectionString.validationGroup) || !this.isValid(newConnectionString.authentication().validationGroup)) {
                hasAnyErrors = true;
            }  else {
                // Use the new connection string
                this.editedElasticSearchEtl().connectionStringName(newConnectionString.connectionStringName());
            }
        }

        // 4. Validate *general form*
        if (!this.isValid(this.editedElasticSearchEtl().validationGroup)) {
            hasAnyErrors = true;
        }
        
        if (hasAnyErrors) {
            this.spinners.save(false);
            return false;
        }
                
        // 5. All is well, Save connection string (if relevant..) 
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
        
        // 6. All is well, Save Elastic Search Etl task
        savingNewStringAction.done(() => {
            eventsCollector.default.reportEvent("elastic-search-etl", "save");
            
            const scriptsToReset = this.editedElasticSearchEtl()
                .transformationScripts()
                .filter(x => x.resetScript())
                .map(x => x.name());
            
            const dto = this.editedElasticSearchEtl().toDto();
            saveEtlTaskCommand.forElasticSearchEtl(this.db, dto, scriptsToReset)
                .execute()
                .done(() => {
                    this.dirtyFlag().reset();
                    this.goToOngoingTasksView();
                 })
                .always(() => this.spinners.save(false));
        });
    }

    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.db));
    }

    syntaxHelp() {
        const viewmodel = new transformationScriptSyntax("ElasticSearch");
        app.showBootstrapDialog(viewmodel);
    }
    
    toggleTestArea() {
        if (!this.enableTestArea()) {
            let hasErrors = false;

            // validate elastic search indexes
            if (this.showEditElasticSearchIndexArea()) {
                if (this.isValid(this.editedElasticSearchIndexSandbox().validationGroup)) {
                    this.saveEditedElasticSearchIndex();
                } else {
                    hasErrors = true;
                }
            }

            // validate global form - but only 'enterTestModeValidationGroup'
            if (!this.isValid(this.editedElasticSearchEtl().enterTestModeValidationGroup)) {
                hasErrors = true;
            }
            
            if (!hasErrors) {
                this.enableTestArea(true);
            }
        } else {
            this.enableTestArea(false);
        }
    }

    setState(state: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState): void {
        this.editedElasticSearchEtl().taskState(state);
    }

    /********************************************/
    /*** Transformation Script Actions Region ***/
    /********************************************/

    addNewTransformation() {
        this.transformationScriptSelectedForEdit(null);
        this.editedTransformationScriptSandbox(ongoingTaskElasticSearchEtlTransformationModel.empty(this.findNameForNewTransformation()));
    }

    cancelEditedTransformation() {
        this.editedTransformationScriptSandbox(null);
        this.transformationScriptSelectedForEdit(null);
        this.enableTestArea(false);
    }
    
    saveEditedTransformation() {
        this.enableTestArea(false);
        const transformation = this.editedTransformationScriptSandbox();

        if (!this.isValid(transformation.validationGroup)) {
            return;
        }

        if (transformation.isNew()) {
            const newTransformationItem = new ongoingTaskElasticSearchEtlTransformationModel(transformation.toDto(), false, false);
            newTransformationItem.name(transformation.name());
            newTransformationItem.dirtyFlag().forceDirty();
            this.editedElasticSearchEtl().transformationScripts.push(newTransformationItem);
        } else {
            const oldItem = this.editedElasticSearchEtl().transformationScripts().find(x => x.name() === transformation.name());
            const newItem = new ongoingTaskElasticSearchEtlTransformationModel(transformation.toDto(), false, transformation.resetScript());

            if (oldItem.dirtyFlag().isDirty() || newItem.hasUpdates(oldItem)) {
                newItem.dirtyFlag().forceDirty();
            }

            this.editedElasticSearchEtl().transformationScripts.replace(oldItem, newItem);
        }

        this.editedElasticSearchEtl().transformationScripts.sort((a, b) => a.name().toLowerCase().localeCompare(b.name().toLowerCase()));
        this.editedTransformationScriptSandbox(null);
    }

    private findNameForNewTransformation() {
        const scriptsWithPrefix = this.editedElasticSearchEtl().transformationScripts().filter(script => {
            return script.name().startsWith(editElasticSearchEtlTask.scriptNamePrefix);
        });

        const maxNumber = _.max(scriptsWithPrefix
            .map(x => x.name().substr(editElasticSearchEtlTask.scriptNamePrefix.length))
            .map(x => _.toInteger(x))) || 0;

        return editElasticSearchEtlTask.scriptNamePrefix + (maxNumber + 1);
    }

    removeTransformationScript(model: ongoingTaskElasticSearchEtlTransformationModel) {
        this.editedElasticSearchEtl().transformationScripts.remove(x => model.name() === x.name());
        
        if (this.transformationScriptSelectedForEdit() === model) {
            this.editedTransformationScriptSandbox(null);
            this.transformationScriptSelectedForEdit(null);
        }
    }

    editTransformationScript(model: ongoingTaskElasticSearchEtlTransformationModel) {
        this.makeSureSandboxIsVisible();
        this.transformationScriptSelectedForEdit(model);
        this.editedTransformationScriptSandbox(new ongoingTaskElasticSearchEtlTransformationModel(model.toDto(), false, model.resetScript()));

        $('.edit-elastic-search-task .js-test-area [data-toggle="tooltip"]').tooltip();
    }
    
    private makeSureSandboxIsVisible() {
        const $editArea = $(".edit-elastic-search-task");
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

            if (!_.includes(this.editedTransformationScriptSandbox().transformScriptCollections(), ongoingTaskElasticSearchTransformationModel.applyToAllCollectionsText)) {
                result.unshift(ongoingTaskElasticSearchTransformationModel.applyToAllCollectionsText);
            }

            return result;
        });
    }

    /*******************************************/
    /*** Elastic Search Index Actions Region ***/
    /*******************************************/

    addNewElasticSearchIndex() {
        this.elasticSearchIndexSelectedForEdit(null);
        this.editedElasticSearchIndexSandbox(ongoingTaskElasticSearchEtlIndexModel.empty());
    }

    cancelEditedElasticSearchIndex() {
        this.editedElasticSearchIndexSandbox(null);
        this.elasticSearchIndexSelectedForEdit(null);
    }

    saveEditedElasticSearchIndex() {
        const elasticIndexToSave = this.editedElasticSearchIndexSandbox();
        const newElasticSearchIndex = new ongoingTaskElasticSearchEtlIndexModel(elasticIndexToSave.toDto(), false);
        const overwriteAction = $.Deferred<boolean>();

        if (!this.isValid(elasticIndexToSave.validationGroup)) {
            return;
        }

        const existingElasticSearchIndex = this.editedElasticSearchEtl().elasticIndexes().find(x => x.indexName() === newElasticSearchIndex.indexName());
        
        if (existingElasticSearchIndex && (elasticIndexToSave.isNew() || existingElasticSearchIndex.indexName() !== this.elasticSearchIndexSelectedForEdit().indexName()))
        {
            // Elastic Index name exists - offer to overwrite
            this.confirmationMessage(`Index ${generalUtils.escapeHtml(existingElasticSearchIndex.indexName())} already exists in Elastic Search index list`,
                `Do you want to overwrite index ${generalUtils.escapeHtml(existingElasticSearchIndex.indexName())} data ?`, {
                    buttons: ["No", "Yes, overwrite"],
                    html: true
                })
                .done(result => {
                    if (result.can) {
                        this.overwriteExistingElasticSearchIndex(existingElasticSearchIndex, newElasticSearchIndex);
                        overwriteAction.resolve(true);
                    } else {
                        overwriteAction.resolve(false);
                    }
                });
        } else {
            // New elastic search index
            if (elasticIndexToSave.isNew()) {
                this.editedElasticSearchEtl().elasticIndexes.push(newElasticSearchIndex);
                newElasticSearchIndex.dirtyFlag().forceDirty();
                overwriteAction.resolve(true);
            }
            // Update existing elastic search index
            else {
                const elasticIndexToUpdate = this.editedElasticSearchEtl().elasticIndexes().find(x => x.indexName() === this.elasticSearchIndexSelectedForEdit().indexName());
                this.overwriteExistingElasticSearchIndex(elasticIndexToUpdate, newElasticSearchIndex);
                overwriteAction.resolve(true);
            }
        }

        overwriteAction.done(() => {
            this.editedElasticSearchEtl().elasticIndexes.sort((a, b) => a.indexName().toLowerCase().localeCompare(b.indexName().toLowerCase()));
            if (overwriteAction) { this.editedElasticSearchIndexSandbox(null); }
        });
    }
    
    overwriteExistingElasticSearchIndex(oldItem: ongoingTaskElasticSearchEtlIndexModel, newItem: ongoingTaskElasticSearchEtlIndexModel) {
        if (oldItem.dirtyFlag().isDirty() || newItem.hasUpdates(oldItem)) {
            newItem.dirtyFlag().forceDirty();
        }
        
        this.editedElasticSearchEtl().elasticIndexes.replace(oldItem, newItem);
    }

    deleteElasticSearchIndex(elasticIndexModel: ongoingTaskElasticSearchEtlIndexModel) {
        this.editedElasticSearchEtl().elasticIndexes.remove(x => elasticIndexModel.indexName() === x.indexName());

        if (this.elasticSearchIndexSelectedForEdit() === elasticIndexModel) {
            this.editedElasticSearchIndexSandbox(null);
            this.elasticSearchIndexSelectedForEdit(null);
        }
    }

    editElasticSearchIndex(elasticIndexModel: ongoingTaskElasticSearchEtlIndexModel) {
        this.elasticSearchIndexSelectedForEdit(elasticIndexModel);
        this.editedElasticSearchIndexSandbox(new ongoingTaskElasticSearchEtlIndexModel(elasticIndexModel.toDto(), false));
    }
}

export = editElasticSearchEtlTask;

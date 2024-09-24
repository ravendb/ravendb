import app = require("durandal/app");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import database = require("models/resources/database");
import ongoingTaskSqlEtlEditModel = require("models/database/tasks/ongoingTaskSqlEtlEditModel");
import getOngoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import eventsCollector = require("common/eventsCollector");
import getConnectionStringInfoCommand = require("commands/database/settings/getConnectionStringInfoCommand");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import saveEtlTaskCommand = require("commands/database/tasks/saveEtlTaskCommand");
import generalUtils = require("common/generalUtils");
import ongoingTaskSqlEtlTransformationModel = require("models/database/tasks/ongoingTaskSqlEtlTransformationModel");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import transformationScriptSyntax = require("viewmodels/database/tasks/transformationScriptSyntax");
import ongoingTaskSqlEtlTableModel = require("models/database/tasks/ongoingTaskSqlEtlTableModel");
import connectionStringSqlEtlModel = require("models/database/settings/connectionStringSqlEtlModel");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import jsonUtil = require("common/jsonUtil");
import document = require("models/database/documents/document");
import viewHelpers = require("common/helpers/view/viewHelpers");
import documentMetadata = require("models/database/documents/documentMetadata");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import testSqlReplicationCommand = require("commands/database/tasks/testSqlReplicationCommand");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import popoverUtils = require("common/popoverUtils");
import { highlight, languages } from "prismjs";
import shardViewModelBase from "viewmodels/shardViewModelBase";
import licenseModel from "models/auth/licenseModel";
import { EditSqlEtlInfoHub } from "viewmodels/database/tasks/EditSqlEtlInfoHub";
import { sortBy } from "common/typeUtils";

class sqlTaskTestMode {
    
    performRolledBackTransaction = ko.observable<boolean>(false);
    documentId = ko.observable<string>();
    testDelete = ko.observable<boolean>(false);
    docsIdsAutocompleteResults = ko.observableArray<string>([]);
    db: database;
    configurationProvider: () => Raven.Client.Documents.Operations.ETL.SQL.SqlEtlConfiguration;
    connectionProvider: () => Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString;
    
    validationGroup: KnockoutValidationGroup;
    validateParent: () => boolean;

    testAlreadyExecuted = ko.observable<boolean>(false);
    
    spinners = {
        preview: ko.observable<boolean>(false),
        test: ko.observable<boolean>(false)
    };

    loadedDocument = ko.observable<string>();
    loadedDocumentId = ko.observable<string>();
    
    testResults = ko.observableArray<Raven.Server.Documents.ETL.Providers.SQL.Test.TableQuerySummary.CommandData>([]);
    debugOutput = ko.observableArray<string>([]);
    
    // all kinds of alerts:
    transformationErrors = ko.observableArray<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>([]);
    loadErrors = ko.observableArray<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>([]);
    slowSqlWarnings = ko.observableArray<Raven.Server.NotificationCenter.Notifications.Details.SlowSqlStatementInfo>([]);
    
    warningsCount = ko.pureComputed(() => {
        const transformationCount = this.transformationErrors().length;
        const loadErrorCount = this.loadErrors().length;
        const slowSqlCount = this.slowSqlWarnings().length;
        return transformationCount + loadErrorCount + slowSqlCount;
    });
    
    constructor(db: database, validateParent: () => boolean, 
                configurationProvider: () => Raven.Client.Documents.Operations.ETL.SQL.SqlEtlConfiguration,
                connectionProvider: () => Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString) {
        this.db = db;
        this.validateParent = validateParent;
        this.configurationProvider = configurationProvider;
        this.connectionProvider = connectionProvider;
        
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
            
            const dto = {
                DocumentId: this.documentId(),
                IsDelete: this.testDelete(),
                PerformRolledBackTransaction: this.performRolledBackTransaction(),
                Configuration: this.configurationProvider(),
                Connection: this.connectionProvider()
            } as Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters.TestSqlEtlScript;

            eventsCollector.default.reportEvent("sql-etl", "test-script");
            
            new testSqlReplicationCommand(this.db, dto)
                .execute()
                .done((testResult: Raven.Server.Documents.ETL.Providers.SQL.Test.SqlEtlTestScriptResult) => {
                    this.testResults(testResult.Summary.flatMap(x => x.Commands));
                    this.debugOutput(testResult.DebugOutput);
                    this.loadErrors(testResult.LoadErrors);
                    this.slowSqlWarnings(testResult.SlowSqlWarnings); 
                    this.transformationErrors(testResult.TransformationErrors);
                    
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

class editSqlEtlTask extends shardViewModelBase {

    view = require("views/database/tasks/editSqlEtlTask.html");
    connectionStringView = require("views/database/settings/connectionStringSql.html");
    taskResponsibleNodeSectionView = require("views/partial/taskResponsibleNodeSection.html");
    pinResponsibleNodeTextScriptView = require("views/partial/pinResponsibleNodeTextScript.html");

    static readonly scriptNamePrefix = "Script_";
    static isApplyToAll = ongoingTaskSqlEtlTransformationModel.isApplyToAll;
    
    enableTestArea = ko.observable<boolean>(false);
    test: sqlTaskTestMode;
    
    editedSqlEtl = ko.observable<ongoingTaskSqlEtlEditModel>();
    isAddingNewSqlEtlTask = ko.observable<boolean>(true);
    
    transformationScriptSelectedForEdit = ko.observable<ongoingTaskSqlEtlTransformationModel>();
    editedTransformationScriptSandbox = ko.observable<ongoingTaskSqlEtlTransformationModel>();

    sqlTableSelectedForEdit = ko.observable<ongoingTaskSqlEtlTableModel>();
    editedSqlTableSandbox = ko.observable<ongoingTaskSqlEtlTableModel>();

    possibleMentors = ko.observableArray<string>([]);
    sqlEtlConnectionStrings = ko.observable<Record<string, Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString>>({});
    sqlEtlConnectionStringsNames = ko.pureComputed(() => sortBy(Object.keys(this.sqlEtlConnectionStrings() ?? {}), (x: string) => x.toUpperCase()));
    selectedConnectionStringFactoryName = ko.pureComputed(() => this.sqlEtlConnectionStrings()[this.editedSqlEtl().connectionStringName()]?.FactoryName);

    connectionStringDefined: KnockoutComputed<boolean>;
    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    
    spinners = {
        test: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false)
    };
    
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;

    collections = collectionsTracker.default.collections;
    collectionNames: KnockoutComputed<string[]>;
    
    showAdvancedOptions = ko.observable<boolean>(false);
    showEditTransformationArea: KnockoutComputed<boolean>;
    showEditSqlTableArea: KnockoutComputed<boolean>;

    createNewConnectionString = ko.observable<boolean>(false);
    newConnectionString = ko.observable<connectionStringSqlEtlModel>();
    
    hasSqlEtl = licenseModel.getStatusValue("HasSqlEtl");
    infoHubView: ReactInKnockout<typeof EditSqlEtlInfoHub>;

    constructor(db: database) {
        super(db);
        this.bindToCurrentInstance("useConnectionString",
                                   "onTestConnectionSql",
                                   "removeTransformationScript",
                                   "cancelEditedTransformation",
                                   "cancelEditedSqlTable",
                                   "saveEditedTransformation",
                                   "saveEditedSqlTable",
                                   "syntaxHelp",
                                   "toggleAdvancedArea",
                                   "toggleTestArea",
                                   "deleteSqlTable",
                                   "editSqlTable",
                                   "setState");

        aceEditorBindingHandler.install();
        this.infoHubView = ko.pureComputed(() => ({
            component: EditSqlEtlInfoHub
        }))
    }

    activate(args: any) {
        super.activate(args);
        const deferred = $.Deferred<void>();

        this.loadPossibleMentors();
        
        if (args.taskId) {
            // 1. Editing an Existing task
            this.isAddingNewSqlEtlTask(false);

            getOngoingTaskInfoCommand.forSqlEtl(this.db, args.taskId)
                .execute()
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtl) => {
                    this.editedSqlEtl(new ongoingTaskSqlEtlEditModel(result));
                    deferred.resolve();
                })
                .fail(() => {
                    deferred.reject();
                    router.navigate(appUrl.forOngoingTasks(this.db));
                });
        } else {
            // 2. Creating a New task
            this.isAddingNewSqlEtlTask(true);
            this.editedSqlEtl(ongoingTaskSqlEtlEditModel.empty());
            
            this.editedTransformationScriptSandbox(ongoingTaskSqlEtlTransformationModel.empty(this.findNameForNewTransformation()));
            this.editedSqlTableSandbox(ongoingTaskSqlEtlTableModel.empty());
            
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

        $('.edit-raven-sql-task [data-toggle="tooltip"]').tooltip();

        popoverUtils.longWithHover($("#js-sql-syntax"), {
            html: true,
            content: () => this.newConnectionString()?.syntaxHtml() ?? "",
            placement: "top"
        });
    }
    
    /***************************************************/
    /*** General Sql ETl Model / Page Actions Region ***/
    /***************************************************/

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.db)
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                this.sqlEtlConnectionStrings(result.SqlConnectionStrings);
            });
    }

    private initObservables() {
        const model = this.editedSqlEtl();
        
        this.showAdvancedOptions(model.hasAdvancedOptionsDefined());
        
        // Discard test connection result when connection string has changed
        model.connectionStringName.subscribe(() => this.testConnectionResult(null));

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
        
        this.showEditSqlTableArea = ko.pureComputed(() => !!this.editedSqlTableSandbox());
        this.showEditTransformationArea = ko.pureComputed(() => !!this.editedTransformationScriptSandbox());

        this.newConnectionString(connectionStringSqlEtlModel.empty());
        this.newConnectionString().setNameUniquenessValidator(name => !this.sqlEtlConnectionStringsNames().find(x => x.toLocaleLowerCase() === name.toLocaleLowerCase()));

        const connectionStringName = model.connectionStringName();
        const connectionStringIsMissing = connectionStringName && !this.sqlEtlConnectionStringsNames()
            .find(x => x.toLocaleLowerCase() === connectionStringName.toLocaleLowerCase());
        
        if (!this.sqlEtlConnectionStringsNames().length || connectionStringIsMissing) {
            this.createNewConnectionString(true);
        }
        
        if (connectionStringIsMissing) {
            // looks like user imported data w/o connection strings, prefill form with desired name
            this.newConnectionString().connectionStringName(connectionStringName);
            model.connectionStringName(null);
        }

        // Discard test connection result when needed
        this.createNewConnectionString.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().factoryName.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().connectionString.subscribe(() => this.testConnectionResult(null));
        
        this.connectionStringDefined = ko.pureComputed(() => {
            if (this.createNewConnectionString()) {
                return !!this.newConnectionString().connectionString();
            } else {
                return !!model.connectionStringName();
            }
        });
        
        this.enableTestArea.subscribe(testMode => {
            $("body").toggleClass('show-test', testMode);
        });

        const dtoProvider = () => {
            const dto = model.toDto();

            // override transforms - use only current transformation
            const transformationScriptDto = this.editedTransformationScriptSandbox().toDto();
            transformationScriptDto.Name = "Script_1"; // assign fake name
            dto.Transforms = [transformationScriptDto];

            if (!dto.Name) {
                dto.Name = "Test SQL Task"; // assign fake name
            }
            return dto;
        };
        
        const connectionStringProvider = () => {
            if (this.createNewConnectionString()) {
                return this.newConnectionString().toDto();
            } else {
                return null;
            }
        };
        
        this.test = new sqlTaskTestMode(this.db, () => {
            const transformationValidationGroup = this.isValid(this.editedTransformationScriptSandbox().validationGroup);
            const connectionStringValid = this.connectionStringDefined();
            
            if (this.test.performRolledBackTransaction()) {
                if (transformationValidationGroup && !connectionStringValid) {
                    // close test mode, as connection string is invalid, 
                    // but user requested rolled back transaction
                    
                    // by closing we let user know that connection string is required
                    this.enableTestArea(false);
                    // run global validation - to show connection string errors
                    this.isValid(model.validationGroup);
                    
                    return false;
                }
            }
            
            return transformationValidationGroup && connectionStringValid;
        }, dtoProvider, connectionStringProvider);

        this.initDirtyFlag();
        
        this.test.initObservables();
    }
    
    private initDirtyFlag() {
        const innerDirtyFlag = ko.pureComputed(() => this.editedSqlEtl().dirtyFlag().isDirty());
        const editedScriptFlag = ko.pureComputed(() => !!this.editedTransformationScriptSandbox() && this.editedTransformationScriptSandbox().dirtyFlag().isDirty());
        const editedSqlTableFlag = ko.pureComputed(() => !!this.editedSqlTableSandbox() && this.editedSqlTableSandbox().dirtyFlag().isDirty());
        
        const scriptsCount = ko.pureComputed(() => this.editedSqlEtl().transformationScripts().length);
        const tablesCount = ko.pureComputed(() => this.editedSqlEtl().sqlTables().length);
        
        const hasAnyDirtyTransformationScript = ko.pureComputed(() => {
            let anyDirty = false;
            this.editedSqlEtl().transformationScripts().forEach(script => {
                if (script.dirtyFlag().isDirty()) {
                    anyDirty = true;
                    // don't break here - we want to track all dependencies
                }
            });
            return anyDirty;
        });
        
        const hasAnyDirtySqlTable = ko.pureComputed(() => {
            let anyDirty = false;
            this.editedSqlEtl().sqlTables().forEach(table => {
                if (table.dirtyFlag().isDirty()) {
                    anyDirty = true;
                    // don't break here - we want to track all dependencies
                }
            });
            return anyDirty;
        });

        this.dirtyFlag = new ko.DirtyFlag([
            innerDirtyFlag,
            editedScriptFlag,
            editedSqlTableFlag,
            scriptsCount,
            tablesCount,
            hasAnyDirtyTransformationScript,
            hasAnyDirtySqlTable,
            this.createNewConnectionString,
            this.newConnectionString().dirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    useConnectionString(connectionStringToUse: string) {
        this.editedSqlEtl().connectionStringName(connectionStringToUse);
    }

    onTestConnectionSql() {
        eventsCollector.default.reportEvent("SQL-ETL-connection-string", "test-connection");
        this.spinners.test(true);
        this.testConnectionResult(null);
        
        // New connection string
        if (this.createNewConnectionString()) {
            this.newConnectionString()
                .testConnection(this.db)
                .done((testResult) => this.testConnectionResult(testResult))
                .always(()=> {
                    this.spinners.test(false);
                });
        } else {
            // Existing connection string
            getConnectionStringInfoCommand.forSqlEtl(this.db, this.editedSqlEtl().connectionStringName())
                .execute()
                .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                       new connectionStringSqlEtlModel(result.SqlConnectionStrings[this.editedSqlEtl().connectionStringName()], true, [])
                            .testConnection(this.db)
                            .done((testResult) => this.testConnectionResult(testResult))
                            .always(() => {
                                this.spinners.test(false);
                            });
                });
        }
    }

    saveSqlEtl() {
        let hasAnyErrors = false;
        this.spinners.save(true);
        
        // 1. Validate *edited sql table*
        if (this.showEditSqlTableArea()) {
            if (!this.isValid(this.editedSqlTableSandbox().validationGroup)) {
                hasAnyErrors = true;
            } else {
                this.saveEditedSqlTable();
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
            if (!this.isValid(this.newConnectionString().validationGroup)) {
                hasAnyErrors = true;
            }  else {
                // Use the new connection string
                this.editedSqlEtl().connectionStringName(this.newConnectionString().connectionStringName());
            } 
        } 
        
        // 4. Validate *general form*
        if (!this.isValid(this.editedSqlEtl().validationGroup)) {
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
        
        // 6. All is well, Save Sql Etl task
        savingNewStringAction.done(() => {
            eventsCollector.default.reportEvent("sql-etl", "save");
            
            const scriptsToReset = this.editedSqlEtl()
                .transformationScripts()
                .filter(x => x.resetScript())
                .map(x => x.name());
            
            const dto = this.editedSqlEtl().toDto();
            saveEtlTaskCommand.forSqlEtl(this.db, dto, scriptsToReset)
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
        const viewmodel = new transformationScriptSyntax("Sql");
        app.showBootstrapDialog(viewmodel);
    }
   
    toggleAdvancedArea() {
        this.showAdvancedOptions.toggle();
    }

    toggleTestArea() {
        if (!this.enableTestArea()) {
            let hasErrors = false;
            
            // validate sql tables
            if (this.showEditSqlTableArea()) {
                if (this.isValid(this.editedSqlTableSandbox().validationGroup)) {
                    this.saveEditedSqlTable();
                } else {
                    hasErrors = true;
                }
            }

            // validate connection string
            if (this.createNewConnectionString()) {
                if (!this.isValid(this.newConnectionString().validationGroup)) {
                    hasErrors = true;
                }  else {
                    this.editedSqlEtl().connectionStringName(this.newConnectionString().connectionStringName());
                }
            }

            // validate global form - but only 'enterTestModeValidationGroup'
            if (!this.isValid(this.editedSqlEtl().enterTestModeValidationGroup)) {
                hasErrors = true;
            }
            
            if (!hasErrors) {
                this.enableTestArea(true);
            }
        } else {
            this.enableTestArea(false);
        }
    }

    /********************************************/
    /*** Transformation Script Actions Region ***/
    /********************************************/
    
    addNewTransformation() {
        this.transformationScriptSelectedForEdit(null);
        this.editedTransformationScriptSandbox(ongoingTaskSqlEtlTransformationModel.empty(this.findNameForNewTransformation()));
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
            const newTransformationItem = new ongoingTaskSqlEtlTransformationModel(transformation.toDto(), false, false);
            newTransformationItem.name(transformation.name());
            newTransformationItem.dirtyFlag().forceDirty();
            this.editedSqlEtl().transformationScripts.push(newTransformationItem);
        } else {
            const oldItem = this.editedSqlEtl().transformationScripts().find(x => x.name() === transformation.name());
            const newItem = new ongoingTaskSqlEtlTransformationModel(transformation.toDto(), false, transformation.resetScript());
            
            if (oldItem.dirtyFlag().isDirty() || newItem.hasUpdates(oldItem)) {
                newItem.dirtyFlag().forceDirty();
            }

            this.editedSqlEtl().transformationScripts.replace(oldItem, newItem);
        }

        this.editedSqlEtl().transformationScripts.sort((a, b) => a.name().toLowerCase().localeCompare(b.name().toLowerCase()));
        this.editedTransformationScriptSandbox(null);
    }

    private findNameForNewTransformation() {
        const scriptsWithPrefix = this.editedSqlEtl().transformationScripts().filter(script => {
            return script.name().startsWith(editSqlEtlTask.scriptNamePrefix);
        });

        const maxNumber = _.max(scriptsWithPrefix
            .map(x => x.name().substr(editSqlEtlTask.scriptNamePrefix.length))
            .map(x => _.toInteger(x))) || 0;

        return editSqlEtlTask.scriptNamePrefix + (maxNumber + 1);
    }

    removeTransformationScript(model: ongoingTaskSqlEtlTransformationModel) {
        this.editedSqlEtl().transformationScripts.remove(x => model.name() === x.name());
        
        if (this.transformationScriptSelectedForEdit() === model) {
            this.editedTransformationScriptSandbox(null);
            this.transformationScriptSelectedForEdit(null);
        }
    }

    editTransformationScript(model: ongoingTaskSqlEtlTransformationModel) {
        this.makeSureSandboxIsVisible();
        this.transformationScriptSelectedForEdit(model);
        this.editedTransformationScriptSandbox(new ongoingTaskSqlEtlTransformationModel(model.toDto(), false, model.resetScript()));

        $('.edit-raven-sql-task .js-test-area [data-toggle="tooltip"]').tooltip();
    }
    
    private makeSureSandboxIsVisible() {
        const $editArea = $(".edit-raven-sql-task");
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

            const filteredOptions: string[] = options.filter(x => !usedOptions.includes(x));

            if (key) {
                result = filteredOptions.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                result = filteredOptions;
            }

            if (!_.includes(this.editedTransformationScriptSandbox().transformScriptCollections(), ongoingTaskSqlEtlTransformationModel.applyToAllCollectionsText)) {
                result.unshift(ongoingTaskSqlEtlTransformationModel.applyToAllCollectionsText);
            }
            
            return result;
        });
    }
    
    /********************************/
    /*** Sql Table Actions Region ***/
    /********************************/

    addNewSqlTable() {
        this.sqlTableSelectedForEdit(null);
        this.editedSqlTableSandbox(ongoingTaskSqlEtlTableModel.empty());
    }

    cancelEditedSqlTable() {
        this.editedSqlTableSandbox(null);
        this.sqlTableSelectedForEdit(null);
    }
       
    saveEditedSqlTable() {
        const sqlTableToSave = this.editedSqlTableSandbox();
        const newSqlTable = new ongoingTaskSqlEtlTableModel(sqlTableToSave.toDto(), false);
        const overwriteAction = $.Deferred<boolean>();

        if (!this.isValid(sqlTableToSave.validationGroup)) {
            return;
        }

        const existingSqlTable = this.editedSqlEtl().sqlTables().find(x => x.tableName() === newSqlTable.tableName());
        
        if (existingSqlTable && (sqlTableToSave.isNew() || existingSqlTable.tableName() !== this.sqlTableSelectedForEdit().tableName()))
        {
            // Table name exists - offer to overwrite
            this.confirmationMessage(`Table ${generalUtils.escapeHtml(existingSqlTable.tableName())} already exists in SQL Tables list`,
                                     `Do you want to overwrite table ${generalUtils.escapeHtml(existingSqlTable.tableName())} data ?`, {
                    buttons: ["No", "Yes, overwrite"],
                    html: true
                })
                .done(result => {
                    if (result.can) {
                        this.overwriteExistingSqlTable(existingSqlTable, newSqlTable);
                        overwriteAction.resolve(true);
                    } else {
                        overwriteAction.resolve(false);
                    }
                });
        } else {
            // New sql table
            if (sqlTableToSave.isNew()) {
                this.editedSqlEtl().sqlTables.push(newSqlTable);
                newSqlTable.dirtyFlag().forceDirty();
                overwriteAction.resolve(true);
            }
            // Update existing sql table (table name is the same)
            else {
                const sqlTableToUpdate = this.editedSqlEtl().sqlTables().find(x => x.tableName() === this.sqlTableSelectedForEdit().tableName());
                this.overwriteExistingSqlTable(sqlTableToUpdate, newSqlTable);
                overwriteAction.resolve(true);
            }
        } 

        overwriteAction.done(() => {
            this.editedSqlEtl().sqlTables.sort((a, b) => a.tableName().toLowerCase().localeCompare(b.tableName().toLowerCase()));
            if (overwriteAction) { this.editedSqlTableSandbox(null); } 
        });
    }
    
    overwriteExistingSqlTable(oldItem: ongoingTaskSqlEtlTableModel, newItem: ongoingTaskSqlEtlTableModel) {
        if (oldItem.dirtyFlag().isDirty() || newItem.hasUpdates(oldItem)) {
            newItem.dirtyFlag().forceDirty();
        }
        
        this.editedSqlEtl().sqlTables.replace(oldItem, newItem);
    }
    
    deleteSqlTable(sqlTableModel: ongoingTaskSqlEtlTableModel) {
        this.editedSqlEtl().sqlTables.remove(x => sqlTableModel.tableName() === x.tableName());

        if (this.sqlTableSelectedForEdit() === sqlTableModel) {
            this.editedSqlTableSandbox(null);
            this.sqlTableSelectedForEdit(null);
        }
    }
    
    editSqlTable(sqlTableModel: ongoingTaskSqlEtlTableModel) {
        this.sqlTableSelectedForEdit(sqlTableModel);
        this.editedSqlTableSandbox(new ongoingTaskSqlEtlTableModel(sqlTableModel.toDto(), false));
    }

    setState(state: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState): void {
        this.editedSqlEtl().taskState(state);
    }
}

export = editSqlEtlTask;

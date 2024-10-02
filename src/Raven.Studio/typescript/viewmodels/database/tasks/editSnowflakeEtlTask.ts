import app = require("durandal/app");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import database = require("models/resources/database");
import getOngoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import eventsCollector = require("common/eventsCollector");
import getConnectionStringInfoCommand = require("commands/database/settings/getConnectionStringInfoCommand");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import saveEtlTaskCommand = require("commands/database/tasks/saveEtlTaskCommand");
import generalUtils = require("common/generalUtils");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import transformationScriptSyntax = require("viewmodels/database/tasks/transformationScriptSyntax");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import jsonUtil = require("common/jsonUtil");
import document = require("models/database/documents/document");
import viewHelpers = require("common/helpers/view/viewHelpers");
import documentMetadata = require("models/database/documents/documentMetadata");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import popoverUtils = require("common/popoverUtils");
import { highlight, languages } from "prismjs";
import shardViewModelBase from "viewmodels/shardViewModelBase";
import licenseModel from "models/auth/licenseModel";
import { sortBy } from "common/typeUtils";
import SnowflakeEtlConfiguration = Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeEtlConfiguration;
import SnowflakeConnectionString = Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeConnectionString;
import connectionStringSnowflakeEtlModel from "models/database/settings/connectionStringSnowflakeEtlModel";
import testSnowflakeReplicationCommand from "commands/database/tasks/testSnowflakeEtlCommand";
import ongoingTaskSnowflakeEtlTransformationModel
    from "models/database/tasks/ongoingTaskSnowflakeEtlTransformationModel";
import ongoingTaskSnowflakeEtlTableModel from "models/database/tasks/ongoingTaskSnowflakeEtlTableModel";
import ongoingTaskSnowflakeEtlEditModel from "models/database/tasks/ongoingTaskSnowflakeEtlEditModel";
import { EditSnowflakeEtlInfoHub } from "viewmodels/database/tasks/EditSnowflakeEtlInfoHub";

class snowflakeTaskTestMode {
    
    performRolledBackTransaction = ko.observable<boolean>(false);
    documentId = ko.observable<string>();
    testDelete = ko.observable<boolean>(false);
    docsIdsAutocompleteResults = ko.observableArray<string>([]);
    db: database;
    configurationProvider: () => Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeEtlConfiguration;
    connectionProvider: () => Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeConnectionString; 
    
    validationGroup: KnockoutValidationGroup;
    validateParent: () => boolean;

    testAlreadyExecuted = ko.observable<boolean>(false);
    
    spinners = {
        preview: ko.observable<boolean>(false),
        test: ko.observable<boolean>(false)
    };

    loadedDocument = ko.observable<string>();
    loadedDocumentId = ko.observable<string>();
    
    testResults = ko.observableArray<Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.Test.TableQuerySummary.CommandData>([]); 
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
                configurationProvider: () => Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeEtlConfiguration,
                connectionProvider: () => Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeConnectionString) {
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
            } as Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.TestRelationalDatabaseEtlScript<SnowflakeConnectionString, SnowflakeEtlConfiguration>;

            eventsCollector.default.reportEvent("snowflake-etl", "test-script");
            
            new testSnowflakeReplicationCommand(this.db, dto)
                .execute()
                .done((testResult: Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.Test.RelationalDatabaseEtlTestScriptResult) => {
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

class editSnowflakeEtlTask extends shardViewModelBase {

    view = require("views/database/tasks/editSnowflakeEtlTask.html");
    connectionStringView = require("views/database/settings/connectionStringSnowflake.html");
    taskResponsibleNodeSectionView = require("views/partial/taskResponsibleNodeSection.html");
    pinResponsibleNodeTextScriptView = require("views/partial/pinResponsibleNodeTextScript.html");

    static readonly scriptNamePrefix = "Script_";
    static isApplyToAll = ongoingTaskSnowflakeEtlTransformationModel.isApplyToAll;
    
    enableTestArea = ko.observable<boolean>(false);
    test: snowflakeTaskTestMode;
    
    editedSnowflakeEtl = ko.observable<ongoingTaskSnowflakeEtlEditModel>();
    isAddingNewSnowflakeEtlTask = ko.observable<boolean>(true);
    
    transformationScriptSelectedForEdit = ko.observable<ongoingTaskSnowflakeEtlTransformationModel>();
    editedTransformationScriptSandbox = ko.observable<ongoingTaskSnowflakeEtlTransformationModel>();

    snowflakeTableSelectedForEdit = ko.observable<ongoingTaskSnowflakeEtlTableModel>();
    editedSnowflakeTableSandbox = ko.observable<ongoingTaskSnowflakeEtlTableModel>();

    possibleMentors = ko.observableArray<string>([]);
    snowflakeEtlConnectionStringsNames = ko.observableArray<string>([]);

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
    showEditSnowflakeTableArea: KnockoutComputed<boolean>;

    createNewConnectionString = ko.observable<boolean>(false);
    newConnectionString = ko.observable<connectionStringSnowflakeEtlModel>();
    
    hasSnowflakeEtl = licenseModel.getStatusValue("HasSnowflakeEtl");
    infoHubView: ReactInKnockout<typeof EditSnowflakeEtlInfoHub>;

    constructor(db: database) {
        super(db);
        this.bindToCurrentInstance("useConnectionString",
                                   "onTestConnectionSnowflake",
                                   "removeTransformationScript",
                                   "cancelEditedTransformation",
                                   "cancelEditedSnowflakeTable",
                                   "saveEditedTransformation",
                                   "saveEditedSnowflakeTable",
                                   "syntaxHelp",
                                   "toggleAdvancedArea",
                                   "toggleTestArea",
                                   "deleteSnowflakeTable",
                                   "editSnowflakeTable",
                                   "setState");

        aceEditorBindingHandler.install();
        this.infoHubView = ko.pureComputed(() => ({
            component: EditSnowflakeEtlInfoHub
        }))
    }

    activate(args: any) {
        super.activate(args);
        const deferred = $.Deferred<void>();

        this.loadPossibleMentors();
        
        if (args.taskId) {
            // 1. Editing an Existing task
            this.isAddingNewSnowflakeEtlTask(false);

            getOngoingTaskInfoCommand.forSnowflakeEtl(this.db, args.taskId)
                .execute()
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSnowflakeEtl) => {
                    this.editedSnowflakeEtl(new ongoingTaskSnowflakeEtlEditModel(result));
                    deferred.resolve();
                })
                .fail(() => {
                    deferred.reject();
                    router.navigate(appUrl.forOngoingTasks(this.db));
                });
        } else {
            // 2. Creating a New task
            this.isAddingNewSnowflakeEtlTask(true);
            this.editedSnowflakeEtl(ongoingTaskSnowflakeEtlEditModel.empty());
            
            this.editedTransformationScriptSandbox(ongoingTaskSnowflakeEtlTransformationModel.empty(this.findNameForNewTransformation()));
            this.editedSnowflakeTableSandbox(ongoingTaskSnowflakeEtlTableModel.empty());
            
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

        $('.edit-raven-snowflake-task [data-toggle="tooltip"]').tooltip();
    }
    
    /***************************************************/
    /*** General Snowflake ETl Model / Page Actions Region ***/
    /***************************************************/

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.db)
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const connectionStringsNames = Object.keys(result.SnowflakeConnectionStrings);
                this.snowflakeEtlConnectionStringsNames(sortBy(connectionStringsNames, (x: string) => x.toUpperCase()));
            });
    }

    private initObservables() {
        const model = this.editedSnowflakeEtl();
        
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
        
        this.showEditSnowflakeTableArea = ko.pureComputed(() => !!this.editedSnowflakeTableSandbox());
        this.showEditTransformationArea = ko.pureComputed(() => !!this.editedTransformationScriptSandbox());

        this.newConnectionString(connectionStringSnowflakeEtlModel.empty());
        this.newConnectionString().setNameUniquenessValidator(name => !this.snowflakeEtlConnectionStringsNames().find(x => x.toLocaleLowerCase() === name.toLocaleLowerCase()));

        const connectionStringName = model.connectionStringName();
        const connectionStringIsMissing = connectionStringName && !this.snowflakeEtlConnectionStringsNames()
            .find(x => x.toLocaleLowerCase() === connectionStringName.toLocaleLowerCase());
        
        if (!this.snowflakeEtlConnectionStringsNames().length || connectionStringIsMissing) {
            this.createNewConnectionString(true);
        }
        
        if (connectionStringIsMissing) {
            // looks like user imported data w/o connection strings, prefill form with desired name
            this.newConnectionString().connectionStringName(connectionStringName);
            model.connectionStringName(null);
        }

        // Discard test connection result when needed
        this.createNewConnectionString.subscribe(() => this.testConnectionResult(null));
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
                dto.Name = "Test Snowflake Task"; // assign fake name
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
        
        this.test = new snowflakeTaskTestMode(this.db, () => {
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
        const innerDirtyFlag = ko.pureComputed(() => this.editedSnowflakeEtl().dirtyFlag().isDirty());
        const editedScriptFlag = ko.pureComputed(() => !!this.editedTransformationScriptSandbox() && this.editedTransformationScriptSandbox().dirtyFlag().isDirty());
        const editedSnowflakeTableFlag = ko.pureComputed(() => !!this.editedSnowflakeTableSandbox() && this.editedSnowflakeTableSandbox().dirtyFlag().isDirty());
        
        const scriptsCount = ko.pureComputed(() => this.editedSnowflakeEtl().transformationScripts().length);
        const tablesCount = ko.pureComputed(() => this.editedSnowflakeEtl().snowflakeTables().length);
        
        const hasAnyDirtyTransformationScript = ko.pureComputed(() => {
            let anyDirty = false;
            this.editedSnowflakeEtl().transformationScripts().forEach(script => {
                if (script.dirtyFlag().isDirty()) {
                    anyDirty = true;
                    // don't break here - we want to track all dependencies
                }
            });
            return anyDirty;
        });
        
        const hasAnyDirtySnowflakeTable = ko.pureComputed(() => {
            let anyDirty = false;
            this.editedSnowflakeEtl().snowflakeTables().forEach(table => {
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
            editedSnowflakeTableFlag,
            scriptsCount,
            tablesCount,
            hasAnyDirtyTransformationScript,
            hasAnyDirtySnowflakeTable,
            this.createNewConnectionString,
            this.newConnectionString().dirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    useConnectionString(connectionStringToUse: string) {
        this.editedSnowflakeEtl().connectionStringName(connectionStringToUse);
    }

    onTestConnectionSnowflake() {
        eventsCollector.default.reportEvent("Snowflake-ETL-connection-string", "test-connection");
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
            getConnectionStringInfoCommand.forSnowflakeEtl(this.db, this.editedSnowflakeEtl().connectionStringName())
                .execute()
                .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                       new connectionStringSnowflakeEtlModel(result.SnowflakeConnectionStrings[this.editedSnowflakeEtl().connectionStringName()], true, [])
                            .testConnection(this.db)
                            .done((testResult) => this.testConnectionResult(testResult))
                            .always(() => {
                                this.spinners.test(false);
                            });
                });
        }
    }

    saveSnowflakeEtl() {
        let hasAnyErrors = false;
        this.spinners.save(true);
        
        // 1. Validate *edited snowflake table*
        if (this.showEditSnowflakeTableArea()) {
            if (!this.isValid(this.editedSnowflakeTableSandbox().validationGroup)) {
                hasAnyErrors = true;
            } else {
                this.saveEditedSnowflakeTable();
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
                this.editedSnowflakeEtl().connectionStringName(this.newConnectionString().connectionStringName());
            } 
        } 
        
        // 4. Validate *general form*
        if (!this.isValid(this.editedSnowflakeEtl().validationGroup)) {
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
        
        // 6. All is well, Save Snowflake Etl task
        savingNewStringAction.done(() => {
            eventsCollector.default.reportEvent("snowflake-etl", "save");
            
            const scriptsToReset = this.editedSnowflakeEtl()
                .transformationScripts()
                .filter(x => x.resetScript())
                .map(x => x.name());
            
            const dto = this.editedSnowflakeEtl().toDto();
            saveEtlTaskCommand.forSnowflakeEtl(this.db, dto, scriptsToReset)
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
        const viewmodel = new transformationScriptSyntax("Snowflake");
        app.showBootstrapDialog(viewmodel);
    }
   
    toggleAdvancedArea() {
        this.showAdvancedOptions.toggle();
    }

    toggleTestArea() {
        if (!this.enableTestArea()) {
            let hasErrors = false;
            
            // validate snowflake tables
            if (this.showEditSnowflakeTableArea()) {
                if (this.isValid(this.editedSnowflakeTableSandbox().validationGroup)) {
                    this.saveEditedSnowflakeTable();
                } else {
                    hasErrors = true;
                }
            }

            // validate connection string
            if (this.createNewConnectionString()) {
                if (!this.isValid(this.newConnectionString().validationGroup)) {
                    hasErrors = true;
                }  else {
                    this.editedSnowflakeEtl().connectionStringName(this.newConnectionString().connectionStringName());
                }
            }

            // validate global form - but only 'enterTestModeValidationGroup'
            if (!this.isValid(this.editedSnowflakeEtl().enterTestModeValidationGroup)) {
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
        this.editedTransformationScriptSandbox(ongoingTaskSnowflakeEtlTransformationModel.empty(this.findNameForNewTransformation()));
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
            const newTransformationItem = new ongoingTaskSnowflakeEtlTransformationModel(transformation.toDto(), false, false);
            newTransformationItem.name(transformation.name());
            newTransformationItem.dirtyFlag().forceDirty();
            this.editedSnowflakeEtl().transformationScripts.push(newTransformationItem);
        } else {
            const oldItem = this.editedSnowflakeEtl().transformationScripts().find(x => x.name() === transformation.name());
            const newItem = new ongoingTaskSnowflakeEtlTransformationModel(transformation.toDto(), false, transformation.resetScript());
            
            if (oldItem.dirtyFlag().isDirty() || newItem.hasUpdates(oldItem)) {
                newItem.dirtyFlag().forceDirty();
            }

            this.editedSnowflakeEtl().transformationScripts.replace(oldItem, newItem);
        }

        this.editedSnowflakeEtl().transformationScripts.sort((a, b) => a.name().toLowerCase().localeCompare(b.name().toLowerCase()));
        this.editedTransformationScriptSandbox(null);
    }

    private findNameForNewTransformation() {
        const scriptsWithPrefix = this.editedSnowflakeEtl().transformationScripts().filter(script => {
            return script.name().startsWith(editSnowflakeEtlTask.scriptNamePrefix);
        });

        const maxNumber = _.max(scriptsWithPrefix
            .map(x => x.name().substr(editSnowflakeEtlTask.scriptNamePrefix.length))
            .map(x => _.toInteger(x))) || 0;

        return editSnowflakeEtlTask.scriptNamePrefix + (maxNumber + 1);
    }

    removeTransformationScript(model: ongoingTaskSnowflakeEtlTransformationModel) {
        this.editedSnowflakeEtl().transformationScripts.remove(x => model.name() === x.name());
        
        if (this.transformationScriptSelectedForEdit() === model) {
            this.editedTransformationScriptSandbox(null);
            this.transformationScriptSelectedForEdit(null);
        }
    }

    editTransformationScript(model: ongoingTaskSnowflakeEtlTransformationModel) {
        this.makeSureSandboxIsVisible();
        this.transformationScriptSelectedForEdit(model);
        this.editedTransformationScriptSandbox(new ongoingTaskSnowflakeEtlTransformationModel(model.toDto(), false, model.resetScript()));

        $('.edit-raven-snowflake-task .js-test-area [data-toggle="tooltip"]').tooltip();
    }
    
    private makeSureSandboxIsVisible() {
        const $editArea = $(".edit-raven-snowflake-task");
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

            if (!_.includes(this.editedTransformationScriptSandbox().transformScriptCollections(), ongoingTaskSnowflakeEtlTransformationModel.applyToAllCollectionsText)) {
                result.unshift(ongoingTaskSnowflakeEtlTransformationModel.applyToAllCollectionsText);
            }
            
            return result;
        });
    }
    
    /********************************/
    /*** Snowflake Table Actions Region ***/
    /********************************/

    addNewSnowflakeTable() {
        this.snowflakeTableSelectedForEdit(null);
        this.editedSnowflakeTableSandbox(ongoingTaskSnowflakeEtlTableModel.empty());
    }

    cancelEditedSnowflakeTable() {
        this.editedSnowflakeTableSandbox(null);
        this.snowflakeTableSelectedForEdit(null);
    }
       
    saveEditedSnowflakeTable() {
        const snowflakeTableToSave = this.editedSnowflakeTableSandbox();
        const newSnowflakeTable = new ongoingTaskSnowflakeEtlTableModel(snowflakeTableToSave.toDto(), false);
        const overwriteAction = $.Deferred<boolean>();

        if (!this.isValid(snowflakeTableToSave.validationGroup)) {
            return;
        }

        const existingSnowflakeTable = this.editedSnowflakeEtl().snowflakeTables().find(x => x.tableName() === newSnowflakeTable.tableName());
        
        if (existingSnowflakeTable && (snowflakeTableToSave.isNew() || existingSnowflakeTable.tableName() !== this.snowflakeTableSelectedForEdit().tableName()))
        {
            // Table name exists - offer to overwrite
            this.confirmationMessage(`Table ${generalUtils.escapeHtml(existingSnowflakeTable.tableName())} already exists in Snowflake Tables list`,
                                     `Do you want to overwrite table ${generalUtils.escapeHtml(existingSnowflakeTable.tableName())} data ?`, {
                    buttons: ["No", "Yes, overwrite"],
                    html: true
                })
                .done(result => {
                    if (result.can) {
                        this.overwriteExistingSnowflakeTable(existingSnowflakeTable, newSnowflakeTable);
                        overwriteAction.resolve(true);
                    } else {
                        overwriteAction.resolve(false);
                    }
                });
        } else {
            // New snowflake table
            if (snowflakeTableToSave.isNew()) {
                this.editedSnowflakeEtl().snowflakeTables.push(newSnowflakeTable);
                newSnowflakeTable.dirtyFlag().forceDirty();
                overwriteAction.resolve(true);
            }
            // Update existing snowflake table (table name is the same)
            else {
                const snowflakeTableToUpdate = this.editedSnowflakeEtl().snowflakeTables().find(x => x.tableName() === this.snowflakeTableSelectedForEdit().tableName());
                this.overwriteExistingSnowflakeTable(snowflakeTableToUpdate, newSnowflakeTable);
                overwriteAction.resolve(true);
            }
        } 

        overwriteAction.done(() => {
            this.editedSnowflakeEtl().snowflakeTables.sort((a, b) => a.tableName().toLowerCase().localeCompare(b.tableName().toLowerCase()));
            if (overwriteAction) { this.editedSnowflakeTableSandbox(null); } 
        });
    }
    
    overwriteExistingSnowflakeTable(oldItem: ongoingTaskSnowflakeEtlTableModel, newItem: ongoingTaskSnowflakeEtlTableModel) {
        if (oldItem.dirtyFlag().isDirty() || newItem.hasUpdates(oldItem)) {
            newItem.dirtyFlag().forceDirty();
        }
        
        this.editedSnowflakeEtl().snowflakeTables.replace(oldItem, newItem);
    }
    
    deleteSnowflakeTable(snowflakeTableModel: ongoingTaskSnowflakeEtlTableModel) {
        this.editedSnowflakeEtl().snowflakeTables.remove(x => snowflakeTableModel.tableName() === x.tableName());

        if (this.snowflakeTableSelectedForEdit() === snowflakeTableModel) {
            this.editedSnowflakeTableSandbox(null);
            this.snowflakeTableSelectedForEdit(null);
        }
    }
    
    editSnowflakeTable(snowflakeTableModel: ongoingTaskSnowflakeEtlTableModel) {
        this.snowflakeTableSelectedForEdit(snowflakeTableModel);
        this.editedSnowflakeTableSandbox(new ongoingTaskSnowflakeEtlTableModel(snowflakeTableModel.toDto(), false));
    }

    setState(state: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState): void {
        this.editedSnowflakeEtl().taskState(state);
    }
}

export = editSnowflakeEtlTask;

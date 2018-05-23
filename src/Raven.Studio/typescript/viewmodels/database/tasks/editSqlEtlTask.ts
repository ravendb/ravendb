import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
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
import getPossibleMentorsCommand = require("commands/database/tasks/getPossibleMentorsCommand");
import jsonUtil = require("common/jsonUtil");

class editSqlEtlTask extends viewModelBase {

    static readonly scriptNamePrefix = "Script #";
    
    editedSqlEtl = ko.observable<ongoingTaskSqlEtlEditModel>();
    isAddingNewSqlEtlTask = ko.observable<boolean>(true);
    
    transformationScriptSelectedForEdit = ko.observable<ongoingTaskSqlEtlTransformationModel>();
    editedTransformationScriptSandbox = ko.observable<ongoingTaskSqlEtlTransformationModel>();

    sqlTableSelectedForEdit = ko.observable<ongoingTaskSqlEtlTableModel>();
    editedSqlTableSandbox = ko.observable<ongoingTaskSqlEtlTableModel>();

    possibleMentors = ko.observableArray<string>([]);
    sqlEtlConnectionStringsNames = ko.observableArray<string>([]); 

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    
    spinners = {
        test: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false)
    };
    
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;
    
    collectionNames: KnockoutComputed<string[]>;
    
    showAdvancedOptions = ko.observable<boolean>(false);
    showEditTransformationArea: KnockoutComputed<boolean>;
    showEditSqlTableArea: KnockoutComputed<boolean>;

    createNewConnectionString = ko.observable<boolean>(false);
    newConnectionString = ko.observable<connectionStringSqlEtlModel>(); 

    constructor() {
        super();
        this.bindToCurrentInstance("useConnectionString",
                                   "useCollection",
                                   "testConnection",
                                   "removeTransformationScript",
                                   "cancelEditedTransformation",
                                   "cancelEditedSqlTable",
                                   "saveEditedTransformation",
                                   "saveEditedSqlTable",
                                   "syntaxHelp",
                                   "toggleAdvancedArea",
                                   "deleteSqlTable",
                                   "editSqlTable");

        aceEditorBindingHandler.install();
    }

    activate(args: any) {
        super.activate(args);
        const deferred = $.Deferred<void>();

        if (args.taskId) {
            // 1. Editing an Existing task
            this.isAddingNewSqlEtlTask(false);

            getOngoingTaskInfoCommand.forSqlEtl(this.activeDatabase(), args.taskId)
                .execute()
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlDetails) => {
                    this.editedSqlEtl(new ongoingTaskSqlEtlEditModel(result));
                    deferred.resolve();
                })
                .fail(() => {
                    deferred.reject();
                    router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
                });
        } else {
            // 2. Creating a New task
            this.isAddingNewSqlEtlTask(true);
            this.editedSqlEtl(ongoingTaskSqlEtlEditModel.empty());
            
            this.editedTransformationScriptSandbox(ongoingTaskSqlEtlTransformationModel.empty());          
            this.editedSqlTableSandbox(ongoingTaskSqlEtlTableModel.empty());  
            
            deferred.resolve();
        }
        
        deferred.done(() => {
            this.initObservables();
        });

        return $.when<any>(this.getAllConnectionStrings(), this.loadPossibleMentors(), deferred);
    }

    private loadPossibleMentors() {
        return new getPossibleMentorsCommand(this.activeDatabase().name)
            .execute()
            .done(mentors => this.possibleMentors(mentors));
    }
    
    compositionComplete() {
        super.compositionComplete();

        $('.edit-raven-sql-task [data-toggle="tooltip"]').tooltip();
    }
    
    /***************************************************/
    /*** General Sql ETl Model / Page Actions Region ***/
    /***************************************************/

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.activeDatabase())
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const connectionStringsNames = Object.keys(result.SqlConnectionStrings);
                this.sqlEtlConnectionStringsNames(_.sortBy(connectionStringsNames, x => x.toUpperCase()));
            });
    }

    private initObservables() {
        // Discard test connection result when connection string has changed
        this.editedSqlEtl().connectionStringName.subscribe(() => this.testConnectionResult(null));

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
        
        this.initDirtyFlag();

        this.newConnectionString(connectionStringSqlEtlModel.empty());

        // Open the 'Create new conn. str.' area if no connection strings are yet defined 
        this.sqlEtlConnectionStringsNames.subscribe((value) => { this.createNewConnectionString(!value.length) });
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
            this.createNewConnectionString

        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    useConnectionString(connectionStringToUse: string) {
        this.editedSqlEtl().connectionStringName(connectionStringToUse);
    }

    testConnection() {
        eventsCollector.default.reportEvent("SQL-ETL-connection-string", "test-connection");
        this.spinners.test(true);
        this.testConnectionResult(null);
        
        // New connection string
        if (this.createNewConnectionString()) {
            this.newConnectionString()
                .testConnection(this.activeDatabase())
                .done((testResult) => this.testConnectionResult(testResult))
                .always(()=> {
                    this.spinners.test(false);
                });
        }
        else {
            // Existing connection string
            getConnectionStringInfoCommand.forSqlEtl(this.activeDatabase(), this.editedSqlEtl().connectionStringName())
                .execute()
                .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {                       
                       new connectionStringSqlEtlModel(result.SqlConnectionStrings[this.editedSqlEtl().connectionStringName()], true, [])
                            .testConnection(this.activeDatabase())
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
            } 
            else {
                this.saveEditedSqlTable();
            }
        }
        
        // 2. Validate *edited transformation script*
        if (this.showEditTransformationArea()) {
            if (!this.isValid(this.editedTransformationScriptSandbox().validationGroup)) {
                hasAnyErrors = true;  
            } 
            else {
                this.saveEditedTransformation();
            }
        }
        
        // 3. Validate *new connection string* (if relevant..)         
        if (this.createNewConnectionString()) {
            if (!this.isValid(this.newConnectionString().validationGroup)) {
                hasAnyErrors = true;  
            } 
            else {
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
        }
        else {
            savingNewStringAction.resolve();
        }
        
        // 6. All is well, Save Sql Etl task
        savingNewStringAction.done(()=> {
            const scriptsToReset = this.editedSqlEtl()
                .transformationScripts()
                .filter(x => x.resetScript())
                .map(x => x.name());
            
            const dto = this.editedSqlEtl().toDto();
            saveEtlTaskCommand.forSqlEtl(this.activeDatabase(), dto, scriptsToReset)
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
        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
    }

    syntaxHelp() {
        const viewmodel = new transformationScriptSyntax("Sql");
        app.showBootstrapDialog(viewmodel);
    }
   
    toggleAdvancedArea() {
        this.showAdvancedOptions.toggle();
    }

    /********************************************/
    /*** Transformation Script Actions Region ***/
    /********************************************/

    useCollection(collectionToUse: string) {
        this.editedTransformationScriptSandbox().collection(collectionToUse);
    }
    
    addNewTransformation() {
        this.transformationScriptSelectedForEdit(null);
        this.editedTransformationScriptSandbox(ongoingTaskSqlEtlTransformationModel.empty());
    }

    cancelEditedTransformation() {
        this.editedTransformationScriptSandbox(null);
        this.transformationScriptSelectedForEdit(null);
    }    
    
    saveEditedTransformation() {
        const transformation = this.editedTransformationScriptSandbox();
        
        if (!this.isValid(transformation.validationGroup)) {
            return;
        }

        if (transformation.isNew()) {
            const newTransformationItem = new ongoingTaskSqlEtlTransformationModel(transformation.toDto(), false, false);
            newTransformationItem.name(this.findNameForNewTransformation());
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

        const maxNumber =  _.max(scriptsWithPrefix
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
        this.transformationScriptSelectedForEdit(model);
        this.editedTransformationScriptSandbox(new ongoingTaskSqlEtlTransformationModel(model.toDto(), false, model.resetScript()));
    }

    createCollectionNameAutocompleter(collectionText: KnockoutObservable<string>) {
        return ko.pureComputed(() => {
            const key = collectionText();

            const options = this.collectionNames();

            if (key) {
                return options.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                return options;
            }
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
            this.confirmationMessage(`Table ${existingSqlTable.tableName()} already exists in SQL Tables list`,
                                     `Do you want to overwrite table ${existingSqlTable.tableName()} data ?`,
                                     ["No", "Yes, overwrite"])
                .done(result => {
                    if (result.can) {
                        this.overwriteExistingSqlTable(existingSqlTable, newSqlTable);
                        overwriteAction.resolve(true);
                    }
                    else {
                        overwriteAction.resolve(false);
                    }
                });
        }
        else {
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
        
        this.editedSqlEtl().sqlTables.replace(oldItem,  newItem);        
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
}

export = editSqlEtlTask;

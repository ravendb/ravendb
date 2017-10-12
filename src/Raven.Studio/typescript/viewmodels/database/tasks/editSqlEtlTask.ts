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
import testSqlConnectionStringCommand = require("commands/database/cluster/testSqlConnectionStringCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

class editSqlEtlTask extends viewModelBase {

    // The sql etl task model
    editedSqlEtl = ko.observable<ongoingTaskSqlEtlEditModel>();
    isAddingNewSqlEtlTask = ko.observable<boolean>(true);

    // The currently edited transformation script 
    editedTransformationScript = ko.observable<ongoingTaskSqlEtlTransformationModel>(ongoingTaskSqlEtlTransformationModel.empty());

    // The currently edited sql table 
    editedSqlTable = ko.observable<ongoingTaskSqlEtlTableModel>(ongoingTaskSqlEtlTableModel.empty());

    sqlEtlConnectionStringsNames = ko.observableArray<string>([]); 
    connectionStringIsDefined: KnockoutComputed<boolean>; // TODO: this computed and all the places using it should be refactored in RavenDB-8934 !    
    connectionStringsUrl = appUrl.forCurrentDatabase().connectionStrings();

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    
    spinners = {
        test: ko.observable<boolean>(false)
    };
    
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;
      
    collectionNames: KnockoutComputed<string[]>;    
    
    validationGroup: KnockoutValidationGroup;

    showAdvancedOptions = ko.observable<boolean>(false);
    showEditTransformationArea = ko.observable<boolean>(false);
    showEditSqlTableArea = ko.observable<boolean>(false);

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
                .done((result: Raven.Client.ServerWide.Operations.OngoingTaskSqlEtlDetails) => {
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
            deferred.resolve();
        }

        deferred.always(() => {
            this.initObservables();
            this.initValidation();
        });

        return $.when<any>(this.getAllConnectionStrings(), deferred);
    }
    
    /***************************************************/
    /*** General Sql ETl Model / Page Actions Region ***/
    /***************************************************/

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.activeDatabase())
            .execute()
            .done((result: Raven.Client.ServerWide.Operations.ConnectionStrings.GetConnectionStringsResult) => {
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

        this.connectionStringIsDefined = ko.pureComputed(() => {
            return !!(_.find(this.sqlEtlConnectionStringsNames(), (x) => x.toString() === this.editedSqlEtl().connectionStringName()));
        });

        this.collectionNames = ko.pureComputed(() => {
           return collectionsTracker.default.getCollectionNames(); 
        });
        
        // TODO ... this.dirtyFlag = 
    }

    private initValidation() {
        this.editedSqlEtl().connectionStringName.extend({
            required: true,
            validation: [
                {
                    validator: () => this.connectionStringIsDefined(),
                    message: "Connection string is Not defined"
                }
            ]
        });

        this.editedSqlEtl().sqlTables.extend({
            validation: [
                {
                    validator: () => this.editedSqlEtl().sqlTables().length > 0,
                    message: "SQL table is Not defined"
                }
            ]
        });

        this.editedSqlEtl().transformationScripts.extend({
            validation: [
                {
                    validator: () => this.editedSqlEtl().transformationScripts().length > 0,
                    message: "Transformation Script is Not defined"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.editedSqlEtl().connectionStringName,
            sqlTables: this.editedSqlEtl().sqlTables,
            transformationScripts: this.editedSqlEtl().transformationScripts
        });
    }

    useConnectionString(connectionStringToUse: string) {
        this.editedSqlEtl().connectionStringName(connectionStringToUse);
    }

    testConnection() {
        eventsCollector.default.reportEvent("SQL-ETL-connection-string", "test-connection");
        this.spinners.test(true);

        getConnectionStringInfoCommand.forSqlEtl(this.activeDatabase(), this.editedSqlEtl().connectionStringName())
            .execute()
            .done((result: Raven.Client.ServerWide.ETL.SqlConnectionString) => {
                new testSqlConnectionStringCommand(this.activeDatabase(), result.ConnectionString)
                    .execute()
                    .done(result => this.testConnectionResult(result))
                    .always(() => this.spinners.test(false));
            });
    }

    trySaveSqlEtl() {
        // TODO: 1. Handle 'dirty' edited *transfrom script* 
        // TODO: 2. Handle 'dirty' edited *sql table* 
        // 3. If both are validated and saved than we can save the sql etl task model itself
        this.saveSqlEtl();
    }

    saveSqlEtl() {
        // 1. Validate model
        if (!this.isValid(this.validationGroup)) {
            return;
        }

        // 2. Create/add the new sql-etl task
        const dto = this.editedSqlEtl().toDto();
        saveEtlTaskCommand.forSqlEtl(this.activeDatabase(), dto)
            .execute()
            .done(() => {
                // TODO: handle dirty flag state
                this.goToOngoingTasksView();
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
        this.showAdvancedOptions(!this.showAdvancedOptions());
    }

    /********************************************/
    /*** Transformation Script Actions Region ***/
    /********************************************/

    useCollection(collectionToUse: string) {
        this.editedTransformationScript().collection(collectionToUse);
    }
    
    tryAddNewTransformation() {
        // 1. TODO: Think what we want to do if there is an edited transfromation that is already opened but Not saved yet...
        // 2. add..
        this.addNewTransformation();
    }

    private addNewTransformation() {
        this.showEditTransformationArea(false);
        this.editedTransformationScript(ongoingTaskSqlEtlTransformationModel.empty());

        this.showEditTransformationArea(true);                 
    }

    cancelEditedTransformation() {
        this.showEditTransformationArea(false);
    }    
    
    saveEditedTransformation() {
        const transformation = this.editedTransformationScript();
        
        // 1. Validate
        if (!this.isValid(transformation.validationGroup)) {
            return;
        }

        // 2. Save
        if (transformation.isNew()) {
            let newTransformationItem = new ongoingTaskSqlEtlTransformationModel({
                ApplyToAllDocuments: false,
                Collections: [transformation.collection()],
                Disabled: false,
                HasLoadAttachment: false,
                Name: transformation.name(),
                Script: transformation.script()
            }, true);

            this.editedSqlEtl().transformationScripts.push(newTransformationItem);
        }
        else {
            const item = this.editedSqlEtl().transformationScripts().find(x => x.name() === transformation.name());
            item.collection(transformation.collection());
            item.script(transformation.script());
        }

        // 3. Sort
        this.editedSqlEtl().transformationScripts.sort((a, b) => a.name().toLowerCase().localeCompare(b.name().toLowerCase()));

        // 4. Clear
        this.showEditTransformationArea(false);
        // todo: handle dirty flag (reset) 
    }

    removeTransformationScript(model: ongoingTaskSqlEtlTransformationModel) {
        this.editedSqlEtl().transformationScripts.remove(x => model.name() === x.name());
        this.showEditTransformationArea(false);
    }

    editTransformationScript(model: ongoingTaskSqlEtlTransformationModel) {
        this.editedTransformationScript().update(model.toDto(), false);
        this.showEditTransformationArea(true); 
    }
    
    /********************************/
    /*** Sql Table Actions Region ***/
    /********************************/

    addNewSqlTable() {
        this.editedSqlTable(ongoingTaskSqlEtlTableModel.empty());

        this.editedSqlTable().validationGroup.errors.showAllMessages(false);
        
        this.showEditSqlTableArea(true);
        // todo: handle dirty flag (reset)          
    }

    cancelEditedSqlTable() {
        this.showEditSqlTableArea(false);
        // todo: handle dirty flag (reset)     
    }   
       
    saveEditedSqlTable() {
        // 1. Validate
        if (!this.isValid(this.editedSqlTable().validationGroup)) {
            return;
        }

        // 2. Save
        let item = this.editedSqlEtl().sqlTables().find(x => x.tableName() === this.editedSqlTable().tableName());
        if (item) {
            item.tableName(this.editedSqlTable().tableName());
            item.documentIdColumn(this.editedSqlTable().documentIdColumn());
            item.insertOnlyMode(this.editedSqlTable().insertOnlyMode());
        } else {
            let newSqlTableItem = new ongoingTaskSqlEtlTableModel({
                TableName: this.editedSqlTable().tableName(),
                DocumentIdColumn: this.editedSqlTable().documentIdColumn(),
                InsertOnlyMode: this.editedSqlTable().insertOnlyMode()
            }, true);

            this.editedSqlEtl().sqlTables.push(newSqlTableItem);
        }

        // 3. Sort & reset
        this.editedSqlEtl().sqlTables.sort((a, b) => a.tableName().toLowerCase().localeCompare(b.tableName().toLowerCase()));
        this.editedSqlTable(ongoingTaskSqlEtlTableModel.empty());

        // 4. handle dirty flag...
        // todo: ...  
    }
    
    deleteSqlTable(sqlTable: ongoingTaskSqlEtlTableModel) {
        this.editedSqlEtl().sqlTables.remove(x => sqlTable.tableName() === x.tableName());
    }

    editSqlTable(sqlTable: ongoingTaskSqlEtlTableModel) {
        this.editedSqlTable().update(sqlTable.toDto(), false);
        this.showEditSqlTableArea(true);
    }
}

export = editSqlEtlTask;

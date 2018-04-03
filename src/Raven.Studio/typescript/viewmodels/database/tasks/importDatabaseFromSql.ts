import viewModelBase = require("viewmodels/viewModelBase");
import sqlMigration = require("models/database/tasks/sql/sqlMigration");
import fetchSqlDatabaseSchemaCommand = require("commands/database/tasks/fetchSqlDatabaseSchemaCommand");
import migrateSqlDatabaseCommand = require("commands/database/tasks/migrateSqlDatabaseCommand");
import listSqlDatabasesCommand = require("commands/database/tasks/listSqlDatabasesCommand");
import sqlReference = require("models/database/tasks/sql/sqlReference");
import rootSqlTable = require("models/database/tasks/sql/rootSqlTable");
import notificationCenter = require("common/notifications/notificationCenter");
import innerSqlTable = require("models/database/tasks/sql/innerSqlTable");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import defaultAceCompleter = require("common/defaultAceCompleter");

//TODO: consider removing 'Please provide 'Database name' in field below, instead of using' - instead automatically extract this from connection string on blur
class importCollectionFromSql extends viewModelBase {
    
    static pageCount = 5; //TODO: set to 100!
    
    spinners = {
        schema: ko.observable<boolean>(false),
        importing: ko.observable<boolean>(false)
    };
    
    completer = defaultAceCompleter.completer();
    
    model = new sqlMigration();
    
    currentPage = ko.observable<number>(0);
    pageCount: KnockoutComputed<number>;
    currentTables: KnockoutComputed<Array<rootSqlTable>>;
    currentLocationHumane: KnockoutComputed<string>;
    
    inFirstStep = ko.observable<boolean>(true);
    globalSelectionState: KnockoutComputed<checkbox>;
    togglingAll = ko.observable<boolean>(false);
    
    itemBeingEdited = ko.observable<rootSqlTable>();
    
    sourceDatabaseFocus = ko.observable<boolean>(false);
    databaseNames = ko.observableArray<string>([]);
    
    validationGroup: KnockoutValidationGroup;    

    constructor() {
        super();
        
        aceEditorBindingHandler.install();

        this.bindToCurrentInstance("onActionClicked", "setCurrentPage", "enterEditMode", "closeEditedTransformation", "createDbAutocompleter");
        
        this.initObservables();
    }
    
    private initObservables() {
         this.pageCount = ko.pureComputed(() => Math.ceil(this.model.tables().length / importCollectionFromSql.pageCount) );
        
        this.currentTables = ko.pureComputed(() => {
            const start = this.currentPage() * importCollectionFromSql.pageCount;
            return this.model.tables().slice(start, start + importCollectionFromSql.pageCount);
        });
        
        this.currentLocationHumane = ko.pureComputed(() => {
            const total = this.model.tables().length;
            
            const start = this.currentPage() * importCollectionFromSql.pageCount + 1;
            const end = Math.min(total, start + importCollectionFromSql.pageCount - 1);
            
            return "Tables " + start.toLocaleString() + "-" + end.toLocaleString() + " out of " + total.toLocaleString();
        });
        
        this.globalSelectionState = ko.pureComputed<checkbox>(() => {
            if (this.togglingAll()) {
                // speed up animation!
                return;
            }
                
            const tables = this.model.tables();
            const totalCount = tables.length;
            const selectedCount = this.model.getSelectedTablesCount();
            
            if (totalCount && selectedCount === totalCount) {
                return "checked";
            }
            if (selectedCount > 0) {
                return "some_checked";
            }
            return "unchecked";
        });
        
        const throttledFetch = _.throttle(() => this.fetchDatabaseNamesAutocomplete(), 200);
        
        this.sourceDatabaseFocus.subscribe(focus => {
            if (focus) {
                throttledFetch();
            }
        })
    }
    
    private fetchDatabaseNamesAutocomplete() {
        new listSqlDatabasesCommand(this.activeDatabase(), this.model.toSourceDto())
            .execute()
            .done(dbNames => this.databaseNames(dbNames)); 
    }
    
    setCurrentPage(page: number) {
        this.currentPage(page);
        $(".js-scroll-tables").scrollTop(0);
    }
    
    nextStep() {        
        if (!this.isValid(this.model.getValidationGroup())) {
            return false;
        }
        
        const connectionString = this.model.getConnectionString(); 
        
        this.spinners.schema(true);
        
        const schemaRequestDto = {
            Provider: this.model.databaseType(),
            ConnectionString: connectionString
        } as Raven.Server.SqlMigration.Model.SourceSqlDatabase;
        
        new fetchSqlDatabaseSchemaCommand(this.activeDatabase(), schemaRequestDto)
            .execute()
            .done(schema => {
                this.inFirstStep(false);
                this.model.onSchemaUpdated(schema);
                
                this.initSecondStep();
            })
            .always(() => this.spinners.schema(false));
    }
    
    private initSecondStep() {
        this.registerDisposableHandler($("body"), "click", (event: JQueryEventObject) => {
            if ($(event.target).closest(".inline-edit").length === 0) {
                // click outside edit area - close all of them
                
                $(".inline-edit.edit-mode")
                    .removeClass("edit-mode");
            }
        });
        
        $("#js-second-step").on("click", ".inline-edit", event => {
            event.preventDefault();
          
            $(".inline-edit.edit-mode")
                .removeClass("edit-mode");
            
            const container = $(event.target).closest(".inline-edit");
            container.addClass("edit-mode");
            $("input", container).focus();
        })
    }
    
    migrate() {
        const dto = this.model.toDto();
        const db = this.activeDatabase();
        
        this.spinners.importing(true);
        
        new migrateSqlDatabaseCommand(db, dto)
            .execute()
            .done((operation: operationIdDto) => {
                notificationCenter.instance.openDetailsForOperationById(db, operation.OperationId);
                
                notificationCenter.instance.monitorOperation(db, operation.OperationId)
                    .always(() => this.spinners.importing(false));
            })
            .fail(() => this.spinners.importing(false));
    }
    
    onActionClicked(reference: sqlReference, action: sqlMigrationAction) {
        if (action === "embed" && reference.action() !== "embed") {
            const tableToEmbed = this.model.findRootTable(reference.targetTable.tableSchema, reference.targetTable.tableName);
            const innerTable = tableToEmbed.cloneForEmbed();
            reference.effectiveInnerTable(innerTable);
            
            const propertyNameFunc = (input: string) => _.upperFirst(_.camelCase(input));
            
            sqlMigration.updatePropertyNames(innerTable, propertyNameFunc);
            
            this.removeBackReference(innerTable, reference);
        }
        
        if (action === "link" && reference.action() !== "link") {
            const tableToLink = this.model.findRootTable(reference.targetTable.tableSchema, reference.targetTable.tableName);
            // no need to clone in this case
            reference.effectiveLinkTable(tableToLink);
        }
        
        reference.action(action);
        
        if (action !== "embed") {
            reference.effectiveInnerTable(null);
        }
        
        if (action !== "link") {
            reference.effectiveLinkTable(null);
        }
    }
    
    private removeBackReference(table: innerSqlTable, reference: sqlReference) {
        const refToDelete = table.references().find(t => _.isEqual(t.joinColumns, reference.joinColumns) 
            && t.targetTable.tableName === reference.sourceTable.tableName 
            && t.targetTable.tableSchema === reference.targetTable.tableSchema);
        
        table.references.remove(refToDelete);
    }
    
    toggleSelectAll() {
        this.togglingAll(true);
        const selectedCount = this.model.getSelectedTablesCount();

        if (selectedCount > 0) {
            this.model.tables().forEach(table => {
                table.checked(false);
            });
        } else {
            this.model.tables().forEach(table => {
                table.checked(true);
            });
        }
        
        this.togglingAll(false);
    }
    
    enterEditMode(table: rootSqlTable) {
        this.itemBeingEdited(table);
    }
    
    syntaxHelp() {
        //TODO:
    }
    
    closeEditedTransformation() {
        this.itemBeingEdited(null);
    }
    
    aceEditorLangMode() {
        switch (this.model.databaseType()) {
            case "MsSQL":
                return "ace/mode/sqlserver";
            case "MySQL":
                return "ace/mode/mysql";
            default: 
                return "ace/mode/sql";
        }
    }
    
    createDbAutocompleter(dbName: KnockoutObservable<string>) {
        return ko.pureComputed(()=> {
            const dbNameUnwrapped = dbName() ? dbName().toLocaleLowerCase() : "";
            
            return this.databaseNames()
                .filter(name => name.toLocaleLowerCase().includes(dbNameUnwrapped));
        });
    }
}

export = importCollectionFromSql; 

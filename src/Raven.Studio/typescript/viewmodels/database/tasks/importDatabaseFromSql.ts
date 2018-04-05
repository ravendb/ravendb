import app = require("durandal/app");
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
import abstractSqlTable = require("models/database/tasks/sql/abstractSqlTable");
import viewHelpers = require("common/helpers/view/viewHelpers");
import referenceInUseDialog = require("viewmodels/database/tasks/referenceInUseDialog");

//TODO: consider removing 'Please provide 'Database name' in field below, instead of using' - instead automatically extract this from connection string on blur
class importCollectionFromSql extends viewModelBase {
    
    static pageCount = 5; //TODO: set to 100!
    
    spinners = {
        schema: ko.observable<boolean>(false),
        importing: ko.observable<boolean>(false)
    };
    
    completer = defaultAceCompleter.completer();
    
    model = new sqlMigration();
    
    searchText = ko.observable<string>();
    
    currentPage = ko.observable<number>(0);
    pageCount: KnockoutComputed<number>;
    filteredTables = ko.observableArray<rootSqlTable>([]);
    currentTables: KnockoutComputed<Array<rootSqlTable>>;
    currentLocationHumane: KnockoutComputed<string>;
    
    inFirstStep = ko.observable<boolean>(true);
    globalSelectionState: KnockoutComputed<checkbox>;
    togglingAll = ko.observable<boolean>(false);
    
    showAdvancedOptions = ko.observable<boolean>(false);
    
    itemBeingEdited = ko.observable<rootSqlTable>();
    
    sourceDatabaseFocus = ko.observable<boolean>(false);
    databaseNames = ko.observableArray<string>([]);
    
    validationGroup: KnockoutValidationGroup;    

    constructor() {
        super();
        
        aceEditorBindingHandler.install();

        this.bindToCurrentInstance("onActionClicked", "setCurrentPage", "enterEditMode", 
            "closeEditedTransformation", "createDbAutocompleter", "goToReverseReference", "onCollapseTable");
        
        this.initObservables();
    }
    
    private initObservables() {
        this.pageCount = ko.pureComputed(() => Math.ceil(this.filteredTables().length / importCollectionFromSql.pageCount) );
        
        this.currentTables = ko.pureComputed(() => {
            const start = this.currentPage() * importCollectionFromSql.pageCount;
            return this.filteredTables().slice(start, start + importCollectionFromSql.pageCount);
        });
        
        this.currentLocationHumane = ko.pureComputed(() => {
            const total = this.filteredTables().length;
            
            const start = this.currentPage() * importCollectionFromSql.pageCount + 1;
            const end = Math.min(total, start + importCollectionFromSql.pageCount - 1);
            
            return "Tables " + start.toLocaleString() + "-" + end.toLocaleString() + " out of " + total.toLocaleString() + (this.searchText() ? " - filtered" : "");
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
        });
        
        // dont' throttle for now as we need to point to exact location
        // in case of performance issues we might replace filter with go to option
        this.searchText.subscribe(() => this.filterTables());
    }
    
    private filterTables() {
        this.setCurrentPage(0);
        const searchText = this.searchText();
        if (searchText) {
            const queryLower = searchText.toLocaleLowerCase();
            this.filteredTables(this.model.tables().filter(x => x.tableName.toLocaleLowerCase().includes(queryLower) && x.collectionName().toLocaleLowerCase().includes(queryLower)));
        } else {
            this.filteredTables(this.model.tables());
        }
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
        this.filterTables();
        
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
        if (action === reference.action()) {
            // nothing to do
            return;
        }
        
        switch (action) {
            case "embed":
                this.onEmbedTable(reference);
                return;
                
            case "link":
                this.onLinkTable(reference);
                return;
                
            case "skip":
                this.onSkipTable(reference);
                return;
        }
    }
    
    private onEmbedTable(reference: sqlReference) {
        this.model.onEmbedTable(reference);
    }
    
    private onLinkTable(reference: sqlReference) {
        const linkAction = () => {
            const tableToLink = this.model.findRootTable(reference.targetTable.tableSchema, reference.targetTable.tableName);
            reference.link(tableToLink);
            
            if (!tableToLink.checked()) {
                tableToLink.checked(true);
            }
        };
        
        // if we want to link to unchecked table then warn
        if ((reference.targetTable as rootSqlTable).checked()) {
            linkAction();
        } else {
            this.confirmationMessage("Table not selected", 
                "Table '" + reference.targetTable.tableName + "' you are trying to link is not selected. Would you like to select this table and create a link?", 
                ["Cancel", "Select table and create link"]) 
                .done(result => {
                    if (result.can) {
                        linkAction();
                    }
                });
        }
    }
    
    private onSkipTable(reference: sqlReference) {
        reference.skip();
    }
    
    toggleSelectAll() {
        //TODO: disable checks? 
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
    
    goToReverseReference(reference: sqlReference, animate = true) {
        const originalTopPosition = $("[data-ref-id=" + reference.id + "]").offset().top;
        const reverseReference = this.model.findReverseReference(reference);
        
        this.searchText(""); // make sure table is visible
        
        const table = (reverseReference.sourceTable as rootSqlTable);
        // first find page
        const targetTableIndex = this.model.tables().findIndex(x => x.tableSchema === table.tableSchema && x.tableName === table.tableName);
        if (targetTableIndex !== -1) {
            const page = Math.floor(targetTableIndex / importCollectionFromSql.pageCount);
            this.setCurrentPage(page);
            
            if (table.checked()) {
                // navigate exactly to reference position
                const $revReference = $("[data-ref-id=" + reverseReference.id + "]");
                
                $(".js-scroll-tables").scrollTop($revReference.offset().top - originalTopPosition);
                
                viewHelpers.animate($revReference, "blink-style");
            } else {
                const onPagePosition = targetTableIndex % importCollectionFromSql.pageCount;
                const $targetElement = $(".js-scroll-tables .js-root-table-panel:eq(" + onPagePosition + ")");
                
                $(".js-scroll-tables").scrollTop($targetElement[0].offsetTop - 20);
                
                if (animate) {
                    viewHelpers.animate($(".js-sql-table-name", $targetElement), "blink-style");    
                }
            }
        }
    }
    
    onCollapseTable(table: rootSqlTable, $event: JQueryMouseEventObject) {
        if (table.checked()) {
            // we are about to uncheck this, find if we have any links to this table
            const links = this.model.findLinksToTable(table);
            if (links.length > 0) {
                app.showBootstrapDialog(new referenceInUseDialog(table, links, (ref, action) => this.onActionClicked(ref, action)));
                    
                $event.preventDefault();
                $event.stopImmediatePropagation();
                return false;
            }
        }
        
        return true; // allow checked handler to be executed
    }
}

export = importCollectionFromSql; 

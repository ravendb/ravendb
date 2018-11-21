import app = require("durandal/app");
import fileDownloader = require("common/fileDownloader");
import viewModelBase = require("viewmodels/viewModelBase");
import testSqlConnectionStringCommand = require("commands/database/cluster/testSqlConnectionStringCommand");
import sqlMigration = require("models/database/tasks/sql/sqlMigration");
import fetchSqlDatabaseSchemaCommand = require("commands/database/tasks/fetchSqlDatabaseSchemaCommand");
import migrateSqlDatabaseCommand = require("commands/database/tasks/migrateSqlDatabaseCommand");
import testSqlMigrationCommand = require("commands/database/tasks/testSqlMigrationCommand");
import sqlReference = require("models/database/tasks/sql/sqlReference");
import rootSqlTable = require("models/database/tasks/sql/rootSqlTable");
import notificationCenter = require("common/notifications/notificationCenter");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import defaultAceCompleter = require("common/defaultAceCompleter");
import popoverUtils = require("common/popoverUtils");
import messagePublisher = require("common/messagePublisher");
import viewHelpers = require("common/helpers/view/viewHelpers");
import eventsCollector = require("common/eventsCollector");
import referenceUsageDialog = require("viewmodels/database/tasks/referenceUsageDialog");
import showDataDialog = require("viewmodels/common/showDataDialog");
import documentMetadata = require("models/database/documents/documentMetadata");
import generalUtils = require("common/generalUtils");

interface exportDataDto {
    Schema: Raven.Server.SqlMigration.Schema.DatabaseSchema,
    Configuration: Raven.Server.SqlMigration.Model.MigrationRequest,
    Advanced: sqlMigrationAdvancedSettingsDto,
    DatabaseName: string
    BinaryToAttachment: boolean;
}

class importDatabaseFromSql extends viewModelBase {
    
    static pageCount = 20;
    
    spinners = {
        schema: ko.observable<boolean>(false),
        importing: ko.observable<boolean>(false),
        test: ko.observable<boolean>(false),
        testConnection: ko.observable<boolean>(false)
    };
    
    completer = defaultAceCompleter.completer();
    
    model = new sqlMigration();
    
    searchText = ko.observable<string>();
    
    currentPage = ko.observable<number>(0);
    pageCount: KnockoutComputed<number>;
    filteredTables = ko.observableArray<rootSqlTable>([]);
    currentTables: KnockoutComputed<Array<rootSqlTable>>;
    currentLocationHumane: KnockoutComputed<string>;
    itemBeingEdited = ko.observable<rootSqlTable>();

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;
    
    inFirstStep = ko.observable<boolean>(true);
    globalSelectionState: KnockoutComputed<checkbox>;
    selectedCount: KnockoutComputed<number>;
    togglingAll = ko.observable<boolean>(false);
    
    showAdvancedOptions = ko.observable<boolean>(false);
    
    importedFileName = ko.observable<string>();
    
    continueFlowValidationGroup: KnockoutValidationGroup;
    
    testMode = ko.observable<Raven.Server.SqlMigration.Model.MigrationTestMode>("First");
    testPrimaryKeys = ko.observableArray<KnockoutObservable<string>>([]);
    
    constructor() {
        super();
        
        aceEditorBindingHandler.install();

        this.bindToCurrentInstance("onActionClicked", "setCurrentPage", "enterEditMode", "showIncomingReferences", "fileSelected",
            "closeEditedTransformation", "goToReverseReference", "onCollapseTable", "runTest", "testConnection");
        
        this.initObservables();
        this.initValidation();
    }
    
    private initValidation() {
        this.importedFileName.extend({
            required: true
        });
        this.continueFlowValidationGroup = ko.validatedObservable({
            importedFileName: this.importedFileName
        })
    }
    
    private initObservables() {
        this.pageCount = ko.pureComputed(() => Math.ceil(this.filteredTables().length / importDatabaseFromSql.pageCount) );
        
        this.currentTables = ko.pureComputed(() => {
            const start = this.currentPage() * importDatabaseFromSql.pageCount;
            return this.filteredTables().slice(start, start + importDatabaseFromSql.pageCount);
        });
        
        this.currentLocationHumane = ko.pureComputed(() => {
            const total = this.filteredTables().length;
            
            const start = this.currentPage() * importDatabaseFromSql.pageCount + 1;
            const end = Math.min(total, start + importDatabaseFromSql.pageCount - 1);
            
            return "Tables " + start.toLocaleString() + "-" + end.toLocaleString() + " out of " + total.toLocaleString() + (this.searchText() ? " - filtered" : "");
        });
        
        this.selectedCount = ko.pureComputed(() =>{
            if (this.togglingAll()) {
                // speed up animation!
                return 0;
            }

            return this.model.getSelectedTablesCount();
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
        
        // dont' throttle for now as we need to point to exact location
        // in case of performance issues we might replace filter with go to option
        this.searchText.subscribe(() => this.filterTables());

        this.shortErrorText = ko.pureComputed(() => {
            const result = this.testConnectionResult();
            if (!result || result.Success) {
                return "";
            }
            return generalUtils.trimMessage(result.Error);
        });
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        this.registerDisposableHandler($(document), "fullscreenchange", () => {
            $("body").toggleClass("fullscreen", $(document).fullScreen());
        });
    }
    
    fileSelected(fileName: string) {
        const isFileSelected = fileName ? !!fileName.trim() : false;
        this.importedFileName(isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null);
    }
    
    continueWithFile() {
        if (!this.isValid(this.continueFlowValidationGroup)) {
            return;
        }

        eventsCollector.default.reportEvent("import-sql", "continue-with-file");
        
        const fileInput = <HTMLInputElement>document.querySelector("#jsImportSqlFilePicker");
        const self = this;
        if (fileInput.files.length === 0) {
            return;
        }

        const file = fileInput.files[0];
        const reader = new FileReader();
        reader.onload = function () {
            self.onConfigurationFileRead(this.result);
        };
        reader.onerror = (error: any) => {
            alert(error);
        };
        reader.readAsText(file);
    }
    
    private onConfigurationFileRead(content: string) {
        let importedData: exportDataDto;
        try {
             importedData = JSON.parse(content); 
        } catch (e) {
            messagePublisher.reportError("Failed to parse json data", undefined, undefined);
            throw e;
        }

        // Check correctness of data
        if (!importedData.hasOwnProperty('Schema') || !importedData.hasOwnProperty('Configuration')) {
            messagePublisher.reportError("Invalid SQL migration file format", undefined, undefined);
        } else {
            this.togglingAll(true);
            
            this.inFirstStep(false);
            this.model.loadAdvancedSettings(importedData.Advanced);
            this.model.onSchemaUpdated(importedData.Schema, false);
            
            this.initSecondStep();
            this.model.applyConfiguration(importedData.Configuration);
            this.model.binaryToAttachment(importedData.BinaryToAttachment);
            
            this.togglingAll(false);
        }
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
        const $body = $("body");
        
        this.initHints();
        
        this.registerDisposableHandler($body, "click", (event: JQueryEventObject) => {
            if ($(event.target).closest(".inline-edit").length === 0) {
                // click outside edit area - close all of them
                
                $(".inline-edit.edit-mode")
                    .removeClass("edit-mode");
            }
        });
        
        const $secondStep = $("#js-second-step");
        
        $secondStep.on("click", ".inline-edit", event => {
            event.preventDefault();
          
            $(".inline-edit.edit-mode")
                .removeClass("edit-mode");
            
            const container = $(event.target).closest(".inline-edit");
            if (!container.hasClass("edit-disabled")) {
                container.addClass("edit-mode");
                $("input", container).focus();    
            }
        });
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
    
    setCurrentPage(page: number) {
        this.currentPage(page);
        $(".js-scroll-tables").scrollTop(0);
    }
    
    migrate() {
        const firstWithDuplicates = this.model.tables().find(x => x.hasDuplicateProperties());
        if (firstWithDuplicates) {
            this.goToTable(firstWithDuplicates);
            return;
        }
        
        const dto = this.model.toDto();
        const db = this.activeDatabase();
        
        this.spinners.importing(true);

        eventsCollector.default.reportEvent("import-sql", "migrate");
        
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
        
        // this *may* show popover (based on content provider) 
        $("[data-ref-id=" + reference.id + "] .js-btn-embed").popover('show');
    }
    
    private onLinkTable(reference: sqlReference) {
        if (reference.canLinkTargetTable()) {
            const tableToLink = this.model.findRootTable(reference.targetTable.tableSchema, reference.targetTable.tableName);
            reference.link(tableToLink);
        }
    }
    
    private onSkipTable(reference: sqlReference) {
        reference.skip();
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
        if (this.itemBeingEdited() === table) {
            // act as toggle 
            this.closeEditedTransformation();
            return;
        }
        
        this.itemBeingEdited(table);
        
        popoverUtils.longWithHover($("#editTransform .js-script-popover"),
            {
                content:
                "<div class=\"text-center\">Transform scripts are written in JavaScript </div>" +
                "<pre><span class=\"token keyword\">var</span> age = <span class=\"token keyword\">this</span>.Age;<br />" +
                "<span class=\"token keyword\">if</span> (age > <span class=\"token number\">18</span>)<br />&nbsp;&nbsp;&nbsp;&nbsp;" +
                "<span class=\"token keyword\">throw </span><span class=\"token string\">'skip'</span>; <span class=\"token comment\">// filter-out</span><br /><br />" +
                "<span class=\"token keyword\">this</span>.Adult = <span class=\"token keyword\">false</span>;<br />" +
                "</pre>" ,
                container: "#js-second-step"
            });
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
            case "NpgSQL":
                return "ace/mode/sql";
            default: 
                return "ace/mode/sql";
        }
    }
    
    goToReverseReference(reference: sqlReference, animate = true) {
        const originalTopPosition = $("[data-ref-id=" + reference.id + "] .key:visible").offset().top;
        const reverseReference = this.model.findReverseReference(reference);
        
        this.searchText(""); // make sure table is visible
        
        const table = (reverseReference.sourceTable as rootSqlTable);
        if (table.checked()) {
            // first find page
            const targetTableIndex = this.model.tables().findIndex(x => x.tableSchema === table.tableSchema && x.tableName === table.tableName);
            
            const page = Math.floor(targetTableIndex / importDatabaseFromSql.pageCount);
            this.setCurrentPage(page);
            
            // navigate exactly to reference position
            const $revReference = $("[data-ref-id=" + reverseReference.id + "] .key:visible");
            
            $(".js-scroll-tables").scrollTop($revReference.offset().top - originalTopPosition);
            
            viewHelpers.animate($revReference, "blink-style");
        } else {
            this.goToTable(table, animate);
        }
    }
    
    private goToTable(table: rootSqlTable, animate = true) {
        this.searchText(""); // make sure table is visible
        
        // first find page
        const targetTableIndex = this.model.tables().findIndex(x => x.tableSchema === table.tableSchema && x.tableName === table.tableName);
        if (targetTableIndex !== -1) {
            const page = Math.floor(targetTableIndex / importDatabaseFromSql.pageCount);
            this.setCurrentPage(page);
            
            const onPagePosition = targetTableIndex % importDatabaseFromSql.pageCount;
            const $targetElement = $(".js-scroll-tables .js-root-table-panel:eq(" + onPagePosition + ")");
            
            $(".js-scroll-tables").scrollTop($targetElement[0].offsetTop - 20);
            
            if (animate) {
                viewHelpers.animate($(".js-sql-table-name", $targetElement), "blink-style");    
            }
        }
    }
    
    onToggleAllClick(_: any, $event: JQueryMouseEventObject) {
        if (this.model.getSelectedTablesCount()) {
            
            this.confirmationMessage("Deselect all tables", "To maintain connections integrity, all references with action <strong>'link'</strong> will be set to <strong>'skip'</strong>.<br/> Do you want to proceed? ", ["Cancel", "Set references to 'skip' and deselect all"])
                .done(result => {
                    if (result.can) {
                        this.model.setAllLinksToSkip();
                        this.toggleSelectAll();
                    }
                });
            
            
            $event.preventDefault();
            $event.stopImmediatePropagation();
            return false;
        } 
        
        return true; // allow checked handler to be executed
    }
    
    createLinksCount(table: rootSqlTable) {
        return ko.pureComputed(() => {
            if (this.togglingAll()) {
                return 0;
            }
            return this.model.findLinksToTable(table).length;
        });
    }
    
    showIncomingReferences(table: rootSqlTable) {
        const links = this.model.findLinksToTable(table);
        app.showBootstrapDialog(new referenceUsageDialog(table, links, (ref, action) => this.onActionClicked(ref, action), 
                table => this.goToTable(table, true)));
    }
    
    exportConfiguration() {
        const exportFileName = `Sql-import-${moment().format("YYYY-MM-DD-HH-mm")}`;

        const exportData = {
            Schema: this.model.dbSchema,
            Configuration: this.model.toDto(),
            Advanced: this.model.advancedSettingsDto(),
            BinaryToAttachment: this.model.binaryToAttachment()
        } as exportDataDto;
        
        fileDownloader.downloadAsJson(exportData, exportFileName + ".json", exportFileName);
    }
    
    enterFullScreen() {
        $("#js-second-step").fullScreen(true);
    }

    exitFullScreen() {
        $("#js-second-step").fullScreen(false);
    }
    
    onCollapseTable(table: rootSqlTable, $event: JQueryMouseEventObject) {
        if (table.checked()) {
            // we are about to uncheck this, find if we have any links to this table
            const links = this.model.findLinksToTable(table);
            if (links.length > 0) {
                $event.preventDefault();
                $event.stopImmediatePropagation();
                return false;
            }
            
            table.setAllLinksToSkip();
        }
        
        return true; // allow checked handler to be executed
    }
    
    runTest(table: rootSqlTable) {
        let allValid = true;
        
        if (table.testMode() === 'ByPrimaryKey') {
            table.testPrimaryKeys.forEach(key => {
                if (!this.isValid(key.validationGroup)) {
                    allValid = false;
                }
            });
        }
        
        if (!allValid) {
            return;
        }
        
        this.spinners.test(true);
        const binaryToAttachment = this.model.binaryToAttachment();
        new testSqlMigrationCommand(this.activeDatabase(), {
            Source: this.model.toSourceDto(),
            Settings: {
                Mode: table.testMode(),
                Collection: table.toDto(binaryToAttachment),
                BinaryToAttachment: binaryToAttachment,
                PrimaryKeyValues: table.testPrimaryKeys.map(x => x.value()),
                CollectionsMapping: this.model.toCollectionsMappingDto()
            }
        }).execute()
            .done(result => {
                const metaDto = result.Document["@metadata"];
                documentMetadata.filterMetadata(metaDto);
                
                const text = JSON.stringify(result.Document, null, 4);
                app.showBootstrapDialog(new showDataDialog("Document: " + result.DocumentId, text, "javascript"));
            })
            .always(() => this.spinners.test(false));
    }
    
    initHints() {
        this.initLinkHints();
        this.initTableCheckboxHints();
        this.initUnselectEmbeddedTablesHints();
    }
    
    private initLinkHints() {
        const $body = $("body");
        const $secondStep = $("#js-second-step");
        
        $secondStep.on("mouseenter", ".js-btn-link", event => {
            const target = $(event.currentTarget);
            
            const reference = ko.dataFor(target[0]) as sqlReference;

            if (!target.data('bs.popover')) {
                popoverUtils.longWithHover(target, {
                    content: () => reference.canLinkTargetTable() ? undefined : this.provideSelectTablePopoverText(reference),
                    placement: "top",
                    container: "#js-second-step"
                });
            }

            target.popover('show');
        });

        // handler for selecting table before linking
        this.registerDisposableDelegateHandler($body, "click", ".popover-link-ref", (event: JQueryEventObject) => {
            const $target = $(event.target);
            const refId = $target.attr('data-popover-ref-id');

            const $ref = $("[data-ref-id=" + refId + "]");

            const reference = ko.dataFor($ref[0]) as sqlReference;
            const targetTable = reference.targetTable as rootSqlTable;

            targetTable.checked(true);
            messagePublisher.reportSuccess("Table " + targetTable.tableName + " was selected");

            $(".js-btn-link", $ref).popover('hide');
        });
    }

    private provideSelectTablePopoverText(reference: sqlReference) {
        return '<div class="text-center"><strong class="text-capitalize"><i class="icon-table"></i> ' + reference.targetTable.tableName + '</strong> table is currently not included. <br />'
            + '<button class="btn btn-sm btn-primary popover-link-ref" data-popover-ref-id="' + reference.id + '">Click here to include <strong>'+ reference.targetTable.tableName +'</strong> to migration</button><br />'
            + ' before creating link</div>';
    }
    
    private initTableCheckboxHints() {
        const $secondStep = $("#js-second-step");
        $secondStep.on("mouseenter", ".js-table-checkbox", event => {
                const target = $(event.target);

                const rootTable = ko.dataFor(target[0]) as rootSqlTable;

                if (!target.data('bs.popover')) {
                    popoverUtils.longWithHover(target, {
                        content: () => {
                            const links = this.model.findLinksToTable(rootTable);
                            
                            return links.length ? this.provideTableCheckboxHint(links.length) : undefined;
                        },
                        container: "#js-second-step"
                    })
                }
                
                target.popover('show');
            }
        );
    }
    
    private provideTableCheckboxHint(incomingLinksCount: number) {
        return "This table has <strong>" + incomingLinksCount + "</strong> incoming " + this.pluralize(incomingLinksCount, "link", "links", true) + ". <em><strong>Skip</strong></em> or <em><strong>embed</strong></em> all of them before proceeding.<br/> "
            + "<small class='text-info'><i class='icon-info'></i>  You can view incoming links by clicking on <i class='icon-sql-many-to-one'></i> button</small>";
    }
    
    private initUnselectEmbeddedTablesHints() {
        const $body = $("body");
        const $secondStep = $("#js-second-step");
        
        $secondStep.on("mouseenter", ".js-btn-embed", event => {
            const target = $(event.target);

            const reference = ko.dataFor(target[0]) as sqlReference;

            if (!target.data('bs.popover')) {
                popoverUtils.longWithHover(target, {
                    content: () => { 
                        const links = this.model.findLinksToTable(reference.targetTable);
                        const targetTable = reference.targetTable as rootSqlTable;
                        if (reference.action() === "embed" && links.length === 0 && targetTable.checked()) {
                            return this.provideDeselectTableHint(reference, targetTable);
                        }
                        return undefined;
                    },
                    container: "#js-second-step",
                    placement: "top"
                });
            }
            
            // this *may* show popover - based on logic in content provider 
            target.popover('show');
        });
        
        // handler for selecting table before linking
        this.registerDisposableDelegateHandler($body, "click", ".popover-embed-ref", (event: JQueryEventObject) => {
            const $target = $(event.target);
            const refId = $target.attr('data-popover-ref-id');

            const $ref = $("[data-ref-id=" + refId + "]");

            const reference = ko.dataFor($ref[0]) as sqlReference;
            const targetTable = reference.targetTable as rootSqlTable;

            targetTable.checked(false);
            messagePublisher.reportSuccess("Table " + targetTable.tableName + " was deselected");

            $(".js-btn-embed", $ref).popover('hide');
        });
    }
    
    private provideDeselectTableHint(reference: sqlReference, targetTable: rootSqlTable) {
        return "Table <strong>" + targetTable.tableName + "</strong> doesn't have incoming links and it is embed. <br />"
            + "Probably you can deselect this table to avoid data duplication.<br/>" 
            + '<button class="btn btn-sm btn-primary popover-embed-ref" data-popover-ref-id="' + reference.id + '">Deselect ' + targetTable.tableName + '</button>';
    }

    testConnection() : JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        this.model.databaseType();
        if (this.isValid(this.model.getValidationGroup())) {
            this.spinners.testConnection(true);
            return new testSqlConnectionStringCommand(this.activeDatabase(), this.model.getConnectionString(), this.model.getFactoryName())
                .execute()
                .done(result => this.testConnectionResult(result))
                .always(() => this.spinners.testConnection(false));
        }
        
    }
}

export = importDatabaseFromSql; 

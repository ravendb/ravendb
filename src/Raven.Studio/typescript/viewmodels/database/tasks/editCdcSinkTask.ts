import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import eventsCollector = require("common/eventsCollector");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import jsonUtil = require("common/jsonUtil");
import popoverUtils = require("common/popoverUtils");
import ongoingTaskCdcSinkTableModel = require("models/database/tasks/ongoingTaskCdcSinkTableModel");
import saveCdcSinkCommand = require("commands/database/tasks/saveCdcSinkCommand");
import ongoingTaskCdcSinkEditModel = require("models/database/tasks/ongoingTaskCdcSinkEditModel");
import viewHelpers = require("common/helpers/view/viewHelpers");
import database = require("models/resources/database");
import testCdcSinkCommand = require("commands/database/tasks/testCdcSinkCommand");
import getOngoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import fetchSqlDatabaseSchemaCommand = require("commands/database/tasks/fetchSqlDatabaseSchemaCommand");
import verifyCdcSinkSourceCommand = require("commands/database/tasks/verifyCdcSinkSourceCommand");
import patchDebugActions = require("viewmodels/database/patch/patchDebugActions");
import licenseModel = require("models/auth/licenseModel");
import EditCdcSinkTaskInfoHub = require("./EditCdcSinkTaskInfoHub");
import typeUtils = require("common/typeUtils");

class cdcSinkTaskTestMode {
    db: KnockoutObservable<database>;
    configurationProvider: () => Raven.Client.Documents.Operations.CdcSink.CdcSinkConfiguration;

    messageText = ko.observable("{}");

    validationGroup: KnockoutValidationGroup;
    validateParent: () => boolean;

    testAlreadyExecuted = ko.observable<boolean>(false);

    spinners = {
        test: ko.observable<boolean>(false)
    };

    actions = new patchDebugActions();
    debugOutput = ko.observableArray<string>([]);

    constructor(db: KnockoutObservable<database>,
                validateParent: () => boolean,
                configurationProvider: () => Raven.Client.Documents.Operations.CdcSink.CdcSinkConfiguration) {
        this.db = db;
        this.validateParent = validateParent;
        this.configurationProvider = configurationProvider;

        this.actions.showDocumentsInModified(true);
    }

    initObservables() {
        this.messageText.extend({
            required: true,
            aceValidation: true
        });

        this.validationGroup = ko.validatedObservable({
            messageText: this.messageText
        });
    }

    runTest() {
        const testValid = viewHelpers.isValid(this.validationGroup, true);
        const parentValid = this.validateParent();

        if (testValid && parentValid) {
            this.spinners.test(true);

            const dto: Raven.Server.Documents.CdcSink.Test.TestCdcSinkScript = {
                Configuration: this.configurationProvider(),
                Message: this.messageText()
            };

            eventsCollector.default.reportEvent("cdc-sink", "test-script");

            new testCdcSinkCommand(this.db(), dto)
                .execute()
                .done(simulationResult => {
                    this.actions.fill(simulationResult.Actions);
                    this.debugOutput(simulationResult.DebugOutput);
                    this.testAlreadyExecuted(true);
                })
                .fail(() => {
                    this.actions.reset();
                })
                .always(() => this.spinners.test(false));
        }
    }
}

class editCdcSinkTask extends viewModelBase {

    view = require("views/database/tasks/editCdcSinkTask.html");
    taskResponsibleNodeSectionView = require("views/partial/taskResponsibleNodeSection.html");
    pinResponsibleNodeTextScriptView = require("views/partial/pinResponsibleNodeTextScript.html");

    patchDebugActionsLoadedView = require("views/database/patch/patchDebugActionsLoaded.html");
    patchDebugActionsModifiedView = require("views/database/patch/patchDebugActionsModified.html");
    patchDebugActionsDeletedView = require("views/database/patch/patchDebugActionsDeleted.html");

    hasCdcSink = licenseModel.getStatusValue("HasCdcSink");

    static readonly tableNamePrefix = "Table_";

    enableTestArea = ko.observable<boolean>(false);
    test: cdcSinkTaskTestMode;

    infoHubView: ReactInKnockout<typeof EditCdcSinkTaskInfoHub.EditCdcSinkTaskInfoHub>;

    editedCdcSink = ko.observable<ongoingTaskCdcSinkEditModel>();

    isAddingNewCdcSinkTask = ko.observable<boolean>(true);

    sqlConnectionStringsDetails = ko.observableArray<Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString>([]);

    possibleMentors = ko.observableArray<string>([]);

    // Schema discovery
    schemaFetched = ko.observable<boolean>(false);
    discoveredTables = ko.observableArray<ongoingTaskCdcSinkTableModel>([]);
    selectedDiscoveredTable = ko.observable<ongoingTaskCdcSinkTableModel>();

    // Verify source
    verifyResult = ko.observable<Raven.Server.Documents.CdcSink.CdcSinkVerificationResult>();

    spinners = {
        test: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false),
        fetchSchema: ko.observable<boolean>(false),
        verify: ko.observable<boolean>(false)
    };

    fullErrorDetailsVisible = ko.observable<boolean>(false);

    isSharded = ko.pureComputed(() => {
        const db = this.activeDatabase();
        return db ? db.isSharded() : false;
    });

    selectedConnectionStringDetails = ko.pureComputed(() => {
        const name = this.editedCdcSink() ? this.editedCdcSink().connectionStringName() : null;
        if (!name) {
            return null;
        }
        return this.sqlConnectionStringsDetails().find(x => x.Name === name) || null;
    });

    canFetchSchema = ko.pureComputed(() => {
        return !!this.selectedConnectionStringDetails() && !this.spinners.fetchSchema();
    });

    canVerifySource = ko.pureComputed(() => {
        const name = this.editedCdcSink() ? this.editedCdcSink().connectionStringName() : null;
        return !!name && !this.spinners.verify();
    });

    selectedTablesCount = ko.pureComputed(() => {
        return this.discoveredTables().filter(t => t.isSelected()).length;
    });

    constructor() {
        super();

        aceEditorBindingHandler.install();
        this.bindToCurrentInstance("useConnectionString", "removeTable",
            "cancelEditedTable", "saveEditedTable", "toggleTestArea", "setState",
            "fetchSchema", "verifySource", "applySelectedTables", "toggleTableSelection",
            "selectDiscoveredTableForPreview");

        this.infoHubView = ko.pureComputed(() => ({
            component: EditCdcSinkTaskInfoHub.EditCdcSinkTaskInfoHub
        }));
    }

    activate(args: any) {
        super.activate(args);
        const deferred = $.Deferred<void>();

        this.loadPossibleMentors();

        if (args.taskId) {
            this.isAddingNewCdcSinkTask(false);

            getOngoingTaskInfoCommand.forCdcSink(this.activeDatabase(), args.taskId)
                .execute()
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskCdcSink) => {
                    this.editedCdcSink(new ongoingTaskCdcSinkEditModel(result));
                    deferred.resolve();
                })
                .fail(() => {
                    deferred.reject();
                    router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
                });
        } else {
            this.isAddingNewCdcSinkTask(true);
            this.editedCdcSink(ongoingTaskCdcSinkEditModel.empty());
            this.editedCdcSink().editedTableSandbox(ongoingTaskCdcSinkTableModel.empty(this.findNameForNewTable()));
            deferred.resolve();
        }

        return $.when<any>(this.getAllConnectionStrings(), deferred)
            .done(() => {
                this.initObservables();
            });
    }

    private loadPossibleMentors() {
        const db = this.activeDatabase();
        const members = db.nodes()
            .filter(x => x.type === "Member")
            .map(x => x.tag);

        this.possibleMentors(members);
    }

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.activeDatabase())
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const sqlStrings = Object.values(result.SqlConnectionStrings);
                this.sqlConnectionStringsDetails(typeUtils.sortBy(sqlStrings, x => x.Name.toUpperCase()));
            });
    }

    private initObservables() {
        const dtoProvider = () => {
            const dto = this.editedCdcSink().toDto();

            if (!dto.Name) {
                dto.Name = "Test CDC Sink Task";
            }
            return dto;
        };

        this.test = new cdcSinkTaskTestMode(this.activeDatabase, () => {
            return this.isValid(this.editedCdcSink().editedTableSandbox().testValidationGroup);
        }, dtoProvider);

        this.test.initObservables();

        this.dirtyFlag = new ko.DirtyFlag([
            this.editedCdcSink().dirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    useConnectionString(connectionStringToUse: string) {
        this.editedCdcSink().connectionStringName(connectionStringToUse);
        // Reset schema state when connection string changes
        this.schemaFetched(false);
        this.discoveredTables([]);
        this.selectedDiscoveredTable(null);
        this.verifyResult(null);
    }

    private factoryNameToProvider(factoryName: string): Raven.Server.SqlMigration.MigrationProvider {
        if (!factoryName) {
            return "MsSQL";
        }

        switch (factoryName) {
            case "Npgsql":
                return "NpgSQL";
            case "Microsoft.Data.SqlClient":
            case "System.Data.SqlClient":
                return "MsSQL";
            case "MySql.Data.MySqlClient":
                return "MySQL_MySql_Data";
            case "MySqlConnector.MySqlConnectorFactory":
                return "MySQL_MySqlConnector";
            case "Oracle.ManagedDataAccess.Client":
                return "Oracle";
            default:
                return "MsSQL";
        }
    }

    fetchSchema() {
        const connDetails = this.selectedConnectionStringDetails();
        if (!connDetails) {
            return;
        }

        this.spinners.fetchSchema(true);
        eventsCollector.default.reportEvent("cdc-sink", "fetch-schema");

        const sourceSqlDb: Raven.Server.SqlMigration.Model.SourceSqlDatabase = {
            ConnectionString: connDetails.ConnectionString,
            Provider: this.factoryNameToProvider(connDetails.FactoryName),
            Schemas: []
        };

        new fetchSqlDatabaseSchemaCommand(this.activeDatabase(), sourceSqlDb)
            .execute()
            .done((schema: Raven.Server.SqlMigration.Schema.DatabaseSchema) => {
                const tables = schema.Tables.map(t => ongoingTaskCdcSinkTableModel.fromSchemaTable(t));

                // If we already have configured tables, mark matching ones as selected
                const existingTables = this.editedCdcSink().tables();
                tables.forEach(discovered => {
                    const existing = existingTables.find(e =>
                        e.sourceTableName() === discovered.sourceTableName() &&
                        e.sourceTableSchema() === discovered.sourceTableSchema());
                    if (existing) {
                        discovered.isSelected(true);
                        discovered.name(existing.name());
                        discovered.patch(existing.patch());
                    }
                });

                this.discoveredTables(tables);
                this.schemaFetched(true);

                if (tables.length > 0) {
                    this.selectDiscoveredTableForPreview(tables[0]);
                }
            })
            .always(() => this.spinners.fetchSchema(false));
    }

    verifySource() {
        const connName = this.editedCdcSink().connectionStringName();
        if (!connName) {
            return;
        }

        this.spinners.verify(true);
        eventsCollector.default.reportEvent("cdc-sink", "verify-source");

        new verifyCdcSinkSourceCommand(this.activeDatabase(), connName)
            .execute()
            .done((result: Raven.Server.Documents.CdcSink.CdcSinkVerificationResult) => {
                this.verifyResult(result);
            })
            .always(() => this.spinners.verify(false));
    }

    toggleTableSelection(table: ongoingTaskCdcSinkTableModel) {
        table.isSelected(!table.isSelected());
    }

    selectDiscoveredTableForPreview(table: ongoingTaskCdcSinkTableModel) {
        this.selectedDiscoveredTable(table);
    }

    applySelectedTables() {
        const selected = this.discoveredTables().filter(t => t.isSelected());

        // Replace existing tables with the selected discovered tables
        const newTables: ongoingTaskCdcSinkTableModel[] = [];

        selected.forEach(discovered => {
            const tableDto = discovered.toDto();
            const model = new ongoingTaskCdcSinkTableModel(tableDto, true);
            model.columns(discovered.columns());
            model.primaryKeyColumns(discovered.primaryKeyColumns());
            model.dirtyFlag().forceDirty();
            newTables.push(model);
        });

        newTables.sort((a, b) => a.name().toLowerCase().localeCompare(b.name().toLowerCase()));
        this.editedCdcSink().tables(newTables);
    }

    saveCdcSink() {
        let hasAnyErrors = false;
        this.spinners.save(true);
        const editedSink = this.editedCdcSink();

        if (editedSink.showEditTableArea()) {
            if (!this.isValid(editedSink.editedTableSandbox().validationGroup)) {
                hasAnyErrors = true;
            } else {
                this.saveEditedTable();
            }
        }

        if (!this.isValid(editedSink.validationGroup)) {
            hasAnyErrors = true;
        }

        if (hasAnyErrors) {
            this.spinners.save(false);
            return false;
        }

        eventsCollector.default.reportEvent("cdc-sink", "save");

        const dto = editedSink.toDto();
        new saveCdcSinkCommand(this.activeDatabase(), dto)
            .execute()
            .done(() => {
                this.dirtyFlag().reset();
                this.goToOngoingTasksView();
            })
            .always(() => this.spinners.save(false));
    }

    addNewTable() {
        this.editedCdcSink().tableSelectedForEdit(null);
        this.editedCdcSink().editedTableSandbox(ongoingTaskCdcSinkTableModel.empty(this.findNameForNewTable()));
    }

    cancelEditedTable() {
        this.editedCdcSink().editedTableSandbox(null);
        this.editedCdcSink().tableSelectedForEdit(null);
        this.enableTestArea(false);
    }

    saveEditedTable() {
        this.enableTestArea(false);
        const table = this.editedCdcSink().editedTableSandbox();
        if (!this.isValid(table.validationGroup)) {
            return;
        }

        if (table.isNew()) {
            const newTableItem = new ongoingTaskCdcSinkTableModel(table.toDto(), true);
            newTableItem.name(table.name());
            newTableItem.dirtyFlag().forceDirty();
            this.editedCdcSink().tables.push(newTableItem);
        } else {
            const oldItem = this.editedCdcSink().tableSelectedForEdit();
            const newItem = new ongoingTaskCdcSinkTableModel(table.toDto(), false);

            if (oldItem.dirtyFlag().isDirty() || newItem.hasUpdates(oldItem)) {
                newItem.dirtyFlag().forceDirty();
            }

            this.editedCdcSink().tables.replace(oldItem, newItem);
        }

        this.editedCdcSink().tables.sort((a, b) => a.name().toLowerCase().localeCompare(b.name().toLowerCase()));
        this.editedCdcSink().editedTableSandbox(null);
        this.editedCdcSink().tableSelectedForEdit(null);
    }

    private findNameForNewTable() {
        const tablesWithPrefix = this.editedCdcSink().tables().filter(table => {
            return table.name().startsWith(editCdcSinkTask.tableNamePrefix);
        });

        const maxNumber = _.max(tablesWithPrefix
            .map(x => x.name().substring(editCdcSinkTask.tableNamePrefix.length))
            .map(x => _.toInteger(x))) || 0;

        return editCdcSinkTask.tableNamePrefix + (maxNumber + 1);
    }

    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
    }

    removeTable(model: ongoingTaskCdcSinkTableModel) {
        this.editedCdcSink().deleteTable(model);
    }

    toggleTestArea() {
        this.enableTestArea(!this.enableTestArea());
    }

    setState(state: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState): void {
        this.editedCdcSink().taskState(state);
    }
}

export = editCdcSinkTask;

import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import getOngoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import eventsCollector = require("common/eventsCollector");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import saveEtlTaskCommand = require("commands/database/tasks/saveEtlTaskCommand");
import generalUtils = require("common/generalUtils");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import transformationScriptSyntax = require("viewmodels/database/tasks/transformationScriptSyntax");
import connectionStringOlapEtlModel = require("models/database/settings/connectionStringOlapEtlModel");
import ongoingTaskOlapEtlEditModel = require("models/database/tasks/ongoingTaskOlapEtlEditModel");
import ongoingTaskOlapEtlTransformationModel = require("models/database/tasks/ongoingTaskOlapEtlTransformationModel");
import ongoingTaskOlapEtlTableModel = require("models/database/tasks/ongoingTaskOlapEtlTableModel");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import getPossibleMentorsCommand = require("commands/database/tasks/getPossibleMentorsCommand");
import jsonUtil = require("common/jsonUtil");
import popoverUtils = require("common/popoverUtils");
import tasksCommonContent = require("models/database/tasks/tasksCommonContent");
import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");
import testPeriodicBackupCredentialsCommand = require("commands/serverWide/testPeriodicBackupCredentialsCommand");
import getPeriodicBackupConfigCommand = require("commands/database/tasks/getPeriodicBackupConfigCommand");
import database = require("models/resources/database");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import viewHelpers = require("common/helpers/view/viewHelpers");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import documentMetadata = require("models/database/documents/documentMetadata");
import document = require("models/database/documents/document");
import testOlapEtlCommand = require("commands/database/tasks/testOlapEtlCommand");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualGrid = require("widgets/virtualGrid/virtualGrid");

class partitionTable {
    key: string;
    private dto: Raven.Server.Documents.ETL.Providers.OLAP.Test.OlapEtlTestScriptResult.PartitionItems;

    height: number;
    data: pagedResult<any>;
    
    gridController = ko.observable<virtualGridController<any>>();
    
    constructor(dto: Raven.Server.Documents.ETL.Providers.OLAP.Test.OlapEtlTestScriptResult.PartitionItems) {
        this.key = dto.Key;
        this.dto = dto;
        
        this.data = this.initData();
        this.height = Math.max(200, Math.min(500, (this.data.totalResultCount + 2) * virtualGrid.rowHeightRegular));
        
        _.bindAll(this, "init");
    }
    
    private generateColumns(): virtualColumn[] {
        const columns = this.dto.Columns;
        const width = Math.floor(Math.max(20, 100 / columns.length));
        return columns.map(c => new textColumn<any>(this.gridController(), x => x[c.Name], c.Name + " (" + c.Type + ")", width + "%"));
    }
    
    init() {
        const grid = this.gridController();
        grid.headerVisible(true);
        grid.init(() => this.fetchData(), () => this.generateColumns());
    }
    
    initData(): pagedResult<any> {
        const itemsCount = this.dto.Columns[0].Values.length;

        const items: any[] = [];

        for (let i = 0; i < itemsCount; i++) {
            const item: any = {};
            this.dto.Columns.forEach(column => {
                item[column.Name] = column.Values[i];
            })
            items.push(item);
        }
        
        return {
            totalResultCount: itemsCount,
            items
        }
    }
    
    fetchData(): JQueryPromise<pagedResult<any>> {
        return $.when(this.data);
    }
}

class olapTaskTestMode {

    documentId = ko.observable<string>();
    docsIdsAutocompleteResults = ko.observableArray<string>([]);
    db: KnockoutObservable<database>;
    configurationProvider: () => Raven.Client.Documents.Operations.ETL.OLAP.OlapEtlConfiguration;

    validationGroup: KnockoutValidationGroup;
    validateParent: () => boolean;

    testAlreadyExecuted = ko.observable<boolean>(false);

    spinners = {
        preview: ko.observable<boolean>(false),
        test: ko.observable<boolean>(false)
    };

    loadedDocument = ko.observable<string>();
    loadedDocumentId = ko.observable<string>();

    testResults = ko.observableArray<partitionTable>([]);
    debugOutput = ko.observableArray<string>([]);
    transformationErrors = ko.observableArray<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>([]);

    warningsCount = ko.pureComputed(() => this.transformationErrors().length);

    constructor(db: KnockoutObservable<database>, validateParent: () => boolean,
                configurationProvider: () => Raven.Client.Documents.Operations.ETL.OLAP.OlapEtlConfiguration) {
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

            new getDocumentsMetadataByIDPrefixCommand(item, 10, this.db())
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
                    new getDocumentWithMetadataCommand(documentId(), this.db())
                        .execute()
                        .done((doc: document) => {
                            const docDto = doc.toDto(true);
                            const metaDto = docDto["@metadata"];
                            documentMetadata.filterMetadata(metaDto);
                            const text = JSON.stringify(docDto, null, 4);
                            this.loadedDocument(Prism.highlight(text, (Prism.languages as any).javascript));
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

            const dto: Raven.Server.Documents.ETL.Providers.OLAP.Test.TestOlapEtlScript = {
                DocumentId: this.documentId(),
                Configuration: this.configurationProvider(),
            };

            eventsCollector.default.reportEvent("olap-etl", "test-replication");

            new testOlapEtlCommand(this.db(), dto)
                .execute()
                .done((testResult: Raven.Server.Documents.ETL.Providers.OLAP.Test.OlapEtlTestScriptResult) => {
                    this.testResults(testResult.ItemsByPartition.map(x => new partitionTable(x)));
                    this.debugOutput(testResult.DebugOutput);
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

class editOlapEtlTask extends viewModelBase {

    static readonly scriptNamePrefix = "Script_";
    enableTestArea = ko.observable<boolean>(false);

    test: olapTaskTestMode;
    
    static isApplyToAll = ongoingTaskOlapEtlTransformationModel.isApplyToAll;
    
    editedOlapEtl = ko.observable<ongoingTaskOlapEtlEditModel>();
    isAddingNewOlapEtlTask = ko.observable<boolean>(true);
    
    transformationScriptSelectedForEdit = ko.observable<ongoingTaskOlapEtlTransformationModel>();
    editedTransformationScriptSandbox = ko.observable<ongoingTaskOlapEtlTransformationModel>();

    olapTableSelectedForEdit = ko.observable<ongoingTaskOlapEtlTableModel>();
    editedOlapTableSandbox = ko.observable<ongoingTaskOlapEtlTableModel>();

    possibleMentors = ko.observableArray<string>([]);
    olapEtlConnectionStringsNames = ko.observableArray<string>([]);

    connectionStringDefined: KnockoutComputed<boolean>;
    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    
    spinners = {
        test: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false)
    };
    
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;
    
    collections = collectionsTracker.default.collections;
    
    showEditTransformationArea: KnockoutComputed<boolean>;
    showEditOlapTableArea: KnockoutComputed<boolean>;

    createNewConnectionString = ko.observable<boolean>(false);
    newConnectionString = ko.observable<connectionStringOlapEtlModel>();

    serverConfiguration = ko.observable<periodicBackupServerLimitsResponse>(); // needed for olap local destination in connection string

    constructor() {
        super();
        this.bindToCurrentInstance("useConnectionString",
                                   "testCredentials",
                                   "toggleTestArea",
                                   "removeTransformationScript",
                                   "cancelEditedTransformation",
                                   "cancelEditedOlapTable",
                                   "saveEditedTransformation",
                                   "saveEditedOlapTable",
                                   "syntaxHelp",
                                   "deleteOlapTable",
                                   "editOlapTable");

        aceEditorBindingHandler.install();
    }

    activate(args: any) {
        super.activate(args);
        const deferred = $.Deferred<void>();

        if (args.taskId) {
            // 1. Editing an Existing task
            this.isAddingNewOlapEtlTask(false);

            getOngoingTaskInfoCommand.forOlapEtl(this.activeDatabase(), args.taskId)
                .execute()
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtlDetails) => {
                    this.editedOlapEtl(new ongoingTaskOlapEtlEditModel(result));
                    deferred.resolve();
                })
                .fail(() => {
                    deferred.reject();
                    router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
                });
        } else {
            // 2. Creating a New task
            this.isAddingNewOlapEtlTask(true);
            this.editedOlapEtl(ongoingTaskOlapEtlEditModel.empty());
            
            this.editedTransformationScriptSandbox(ongoingTaskOlapEtlTransformationModel.empty());
            
            deferred.resolve();
        }
        
        return $.when<any>(this.getAllConnectionStrings(), this.loadPossibleMentors(), this.loadServerSideConfiguration(), deferred)
            .done(() => {
                this.initObservables();
            });
    }

    private loadPossibleMentors() {
        return new getPossibleMentorsCommand(this.activeDatabase().name)
            .execute()
            .done(mentors => this.possibleMentors(mentors));
    }
    
    compositionComplete() {
        super.compositionComplete();

        $('.edit-raven-olap-task [data-toggle="tooltip"]').tooltip();

        popoverUtils.longWithHover($(".responsible-node"),
            {
                content: tasksCommonContent.responsibleNodeInfo
            });

        popoverUtils.longWithHover($(".keep-files-on-disk"),
            {
                content: `<small>Keep output parquet files on disk even when 'Local' is not defined in conection string</small>`
            });

        popoverUtils.longWithHover($(".custom-field"),
            {
                content: `<ul class="margin-top margin-top-xs margin-right">
                              <li><small>Set the <strong>value</strong> for the <strong>custom partition</strong> in the target parquet file path</small></li>
                              <li><small>The custom partition itself must be defined inside the script</small></li>
                              <li><small>Example parquet file path: <code>{RemoteFolderName}/{CollectionName}/{customPartitionName=$customPartitionValue} </code></small></li>
                              <li><small>Useful to differentiate file locations when using the same connection string</small></li>
                              <li><small>Queries can be made on the custom partition as well</small></li>
                          </ul>`
            });

        popoverUtils.longWithHover($(".run-frequency"),
            {
                content: `<small>Frequency at which this task will be triggered</small>`
            });
    }
    
    /****************************************************/
    /*** General Olap ETl Model / Page Actions Region ***/
    /****************************************************/

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.activeDatabase())
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const connectionStringsNames = Object.keys(result.OlapConnectionStrings);
                this.olapEtlConnectionStringsNames(_.sortBy(connectionStringsNames, x => x.toUpperCase()));
            });
    }

    private initObservables() {
        // Discard test connection result when connection string has changed
        this.editedOlapEtl().connectionStringName.subscribe(() => this.testConnectionResult(null));

        this.shortErrorText = ko.pureComputed(() => {
            const result = this.testConnectionResult();
            if (!result || result.Success) {
                return "";
            }
            return generalUtils.trimMessage(result.Error);
        });
        
        this.showEditOlapTableArea = ko.pureComputed(() => !!this.editedOlapTableSandbox());
        this.showEditTransformationArea = ko.pureComputed(() => !!this.editedTransformationScriptSandbox());

        this.newConnectionString(connectionStringOlapEtlModel.empty());
        this.newConnectionString().setNameUniquenessValidator(name => !this.olapEtlConnectionStringsNames().find(x => x.toLocaleLowerCase() === name.toLocaleLowerCase()));

        const connectionStringName = this.editedOlapEtl().connectionStringName();
        const connectionStringIsMissing = connectionStringName && !this.olapEtlConnectionStringsNames()
            .find(x => x.toLocaleLowerCase() === connectionStringName.toLocaleLowerCase());
        
        if (!this.olapEtlConnectionStringsNames().length || connectionStringIsMissing) {
            this.createNewConnectionString(true);
        }
        
        if (connectionStringIsMissing) {
            // looks like user imported data w/o connection strings, prefill form with desired name
            this.newConnectionString().connectionStringName(connectionStringName);
            this.editedOlapEtl().connectionStringName(null);
        }
        
        this.connectionStringDefined = ko.pureComputed(() => {
            const editedEtl = this.editedOlapEtl();
            if (this.createNewConnectionString()) {
                return this.isValid(this.newConnectionString().validationGroup);
            } else {
                return !!editedEtl.connectionStringName();
            }
        });

        const dtoProvider = () => {
            const dto = this.editedOlapEtl().toDto();

            // override transforms - use only current transformation
            const transformationScriptDto = this.editedTransformationScriptSandbox().toDto();
            transformationScriptDto.Name = "Script_1"; // assign fake name
            dto.Transforms = [transformationScriptDto];

            if (!dto.Name) {
                dto.Name = "Test OLAP Task"; // assign fake name
            }
            return dto;
        };
        
        this.test = new olapTaskTestMode(this.activeDatabase, () => this.isValid(this.editedTransformationScriptSandbox().validationGroup), dtoProvider);

        this.initDirtyFlag();
        
        this.test.initObservables();
    }
    
    private initDirtyFlag() {
        const innerDirtyFlag = ko.pureComputed(() => this.editedOlapEtl().dirtyFlag().isDirty());
        const editedScriptFlag = ko.pureComputed(() => !!this.editedTransformationScriptSandbox() && this.editedTransformationScriptSandbox().dirtyFlag().isDirty());
        const editedOlapTableFlag = ko.pureComputed(() => !!this.editedOlapTableSandbox() && this.editedOlapTableSandbox().dirtyFlag().isDirty());
        
        const scriptsCount = ko.pureComputed(() => this.editedOlapEtl().transformationScripts().length);
        const tablesCount = ko.pureComputed(() => this.editedOlapEtl().olapTables().length);
        
        const hasAnyDirtyTransformationScript = ko.pureComputed(() => {
            let anyDirty = false;
            this.editedOlapEtl().transformationScripts().forEach(script => {
                if (script.dirtyFlag().isDirty()) {
                    anyDirty = true;
                    // don't break here - we want to track all dependencies
                }
            });
            return anyDirty;
        });
        
        const hasAnyDirtyOlapTable = ko.pureComputed(() => {
            let anyDirty = false;
            this.editedOlapEtl().olapTables().forEach(table => {
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
            editedOlapTableFlag,
            scriptsCount,
            tablesCount,
            hasAnyDirtyTransformationScript,
            hasAnyDirtyOlapTable,
            this.createNewConnectionString,
            this.newConnectionString().dirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    useConnectionString(connectionStringToUse: string) {
        this.editedOlapEtl().connectionStringName(connectionStringToUse);
    }

    testCredentials(bs: backupSettings) {
        if (!this.isValid(bs.effectiveValidationGroup())) {
            return;
        }

        bs.isTestingCredentials(true);
        bs.testConnectionResult(null);

        new testPeriodicBackupCredentialsCommand(bs.connectionType, bs.toDto())
            .execute()
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                bs.testConnectionResult(result);
            })
            .always(() => bs.isTestingCredentials(false));
    }

    private loadServerSideConfiguration() {
        return new getPeriodicBackupConfigCommand(this.activeDatabase())
            .execute()
            .done(config => {
                this.serverConfiguration(config);
            });
    }

    saveOlapEtl() {
        let hasAnyErrors = false;
        this.spinners.save(true);
        
        // 1. Validate *edited olap table*
        if (this.showEditOlapTableArea() && this.editedOlapTableSandbox().hasContent()) {
            if (!this.isValid(this.editedOlapTableSandbox().validationGroup)) {
                hasAnyErrors = true;
            } else {
                this.saveEditedOlapTable();
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
            if (!this.isValidConnectionString()) {
                hasAnyErrors = true;
            }  else {
                // Use the new connection string
                this.editedOlapEtl().connectionStringName(this.newConnectionString().connectionStringName());
            } 
        } 
        
        // 4. Validate *general form*
        if (!this.isValid(this.editedOlapEtl().validationGroup)) {
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
        } else {
            savingNewStringAction.resolve();
        }
        
        // 6. All is well, Save Olap Etl task
        savingNewStringAction.done(() => {
            eventsCollector.default.reportEvent("olap-etl", "save");
            
            const scriptsToReset = this.editedOlapEtl()
                .transformationScripts()
                .filter(x => x.resetScript())
                .map(x => x.name());
            
            const dto = this.editedOlapEtl().toDto();
            saveEtlTaskCommand.forOlapEtl(this.activeDatabase(), dto, scriptsToReset)
                .execute()
                .done(() => {
                    this.dirtyFlag().reset();
                    this.goToOngoingTasksView();
                 })
                .always(() => this.spinners.save(false));
        });
    }

    isValidConnectionString() {
        const conStr = this.newConnectionString();
        let isValid = true;

        if (!this.isValid(conStr.validationGroup)) {
            isValid = false;
        }

        const localSettings = conStr.localSettings();
        if (localSettings.enabled() && !this.isValid(localSettings.effectiveValidationGroup()))
            isValid = false;

        const s3Settings = conStr.s3Settings();
        if (s3Settings.enabled() && !this.isValid(s3Settings.effectiveValidationGroup()))
            isValid = false;

        const azureSettings = conStr.azureSettings();
        if (azureSettings.enabled() && !this.isValid(azureSettings.effectiveValidationGroup()))
            isValid = false;

        const googleCloudSettings = conStr.googleCloudSettings();
        if (googleCloudSettings.enabled() && !this.isValid(googleCloudSettings.effectiveValidationGroup()))
            isValid = false;

        const glacierSettings = conStr.glacierSettings();
        if (glacierSettings.enabled() && !this.isValid(glacierSettings.effectiveValidationGroup()))
            isValid = false;

        const ftpSettings = conStr.ftpSettings();
        if (ftpSettings.enabled() && !this.isValid(ftpSettings.effectiveValidationGroup()))
            isValid = false;

        return isValid;
    }

    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
    }

    syntaxHelp() {
        const viewmodel = new transformationScriptSyntax("Olap");
        app.showBootstrapDialog(viewmodel);
    }
    
    toggleTestArea() {
        if (!this.enableTestArea()) {
            let hasErrors = false;

            // validate global form - but only 'enterTestModeValidationGroup'
            if (!this.isValid(this.editedOlapEtl().enterTestModeValidationGroup)) {
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
        this.editedTransformationScriptSandbox(ongoingTaskOlapEtlTransformationModel.empty());
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
            const newTransformationItem = new ongoingTaskOlapEtlTransformationModel(transformation.toDto(), false, false);
            newTransformationItem.name(this.findNameForNewTransformation());
            newTransformationItem.dirtyFlag().forceDirty();
            this.editedOlapEtl().transformationScripts.push(newTransformationItem);
        } else {
            const oldItem = this.editedOlapEtl().transformationScripts().find(x => x.name() === transformation.name());
            const newItem = new ongoingTaskOlapEtlTransformationModel(transformation.toDto(), false, transformation.resetScript());
            
            if (oldItem.dirtyFlag().isDirty() || newItem.hasUpdates(oldItem)) {
                newItem.dirtyFlag().forceDirty();
            }

            this.editedOlapEtl().transformationScripts.replace(oldItem, newItem);
        }

        this.editedOlapEtl().transformationScripts.sort((a, b) => a.name().toLowerCase().localeCompare(b.name().toLowerCase()));
        this.editedTransformationScriptSandbox(null);
    }

    private findNameForNewTransformation() {
        const scriptsWithPrefix = this.editedOlapEtl().transformationScripts().filter(script => {
            return script.name().startsWith(editOlapEtlTask.scriptNamePrefix);
        });

        const maxNumber = _.max(scriptsWithPrefix
            .map(x => x.name().substr(editOlapEtlTask.scriptNamePrefix.length))
            .map(x => _.toInteger(x))) || 0;

        return editOlapEtlTask.scriptNamePrefix + (maxNumber + 1);
    }

    removeTransformationScript(model: ongoingTaskOlapEtlTransformationModel) {
        this.editedOlapEtl().transformationScripts.remove(x => model.name() === x.name());
        
        if (this.transformationScriptSelectedForEdit() === model) {
            this.editedTransformationScriptSandbox(null);
            this.transformationScriptSelectedForEdit(null);
        }
    }

    editTransformationScript(model: ongoingTaskOlapEtlTransformationModel) {
        this.makeSureSandboxIsVisible();
        this.transformationScriptSelectedForEdit(model);
        this.editedTransformationScriptSandbox(new ongoingTaskOlapEtlTransformationModel(model.toDto(), false, model.resetScript()));

        $('.edit-raven-olap-task .js-test-area [data-toggle="tooltip"]').tooltip();
    }
    
    private makeSureSandboxIsVisible() {
        const $editArea = $(".edit-raven-olap-task");
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

            const filteredOptions = _.difference(options, usedOptions);

            if (key) {
                result = filteredOptions.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                result = filteredOptions;
            }

            if (!_.includes(this.editedTransformationScriptSandbox().transformScriptCollections(), ongoingTaskOlapEtlTransformationModel.applyToAllCollectionsText)) {
                result.unshift(ongoingTaskOlapEtlTransformationModel.applyToAllCollectionsText);
            }

            return result;
        });
    }
    
    /*********************************/
    /*** Olap Table Actions Region ***/
    /*********************************/

    addNewOlapTable() {
        this.olapTableSelectedForEdit(null);
        this.editedOlapTableSandbox(ongoingTaskOlapEtlTableModel.empty());
    }

    cancelEditedOlapTable() {
        this.editedOlapTableSandbox(null);
        this.olapTableSelectedForEdit(null);
    }
       
    saveEditedOlapTable() {
        const olapTableToSave = this.editedOlapTableSandbox();
        const newOlapTable = new ongoingTaskOlapEtlTableModel(olapTableToSave.toDto(), false);
        const overwriteAction = $.Deferred<boolean>();

        if (!this.isValid(olapTableToSave.validationGroup)) {
            return;
        }

        const existingOlapTable = this.editedOlapEtl().olapTables().find(x => x.tableName() === newOlapTable.tableName());
        
        if (existingOlapTable && (olapTableToSave.isNew() || existingOlapTable.tableName() !== this.olapTableSelectedForEdit().tableName()))
        {
            // Table name exists - offer to overwrite
            this.confirmationMessage(`Table ${generalUtils.escapeHtml(existingOlapTable.tableName())} already exists in OLAP Tables list`,
                                     `Do you want to overwrite table ${generalUtils.escapeHtml(existingOlapTable.tableName())} data ?`, {
                    buttons: ["No", "Yes, overwrite"],
                    html: true
                })
                .done(result => {
                    if (result.can) {
                        this.overwriteExistingOlapTable(existingOlapTable, newOlapTable);
                        overwriteAction.resolve(true);
                    } else {
                        overwriteAction.resolve(false);
                    }
                });
        } else {
            // New olap table
            if (olapTableToSave.isNew()) {
                this.editedOlapEtl().olapTables.push(newOlapTable);
                newOlapTable.dirtyFlag().forceDirty();
                overwriteAction.resolve(true);
            }
            // Update existing olap table (table name is the same)
            else {
                const olapTableToUpdate = this.editedOlapEtl().olapTables().find(x => x.tableName() === this.olapTableSelectedForEdit().tableName());
                this.overwriteExistingOlapTable(olapTableToUpdate, newOlapTable);
                overwriteAction.resolve(true);
            }
        } 

        overwriteAction.done(() => {
            this.editedOlapEtl().olapTables.sort((a, b) => a.tableName().toLowerCase().localeCompare(b.tableName().toLowerCase()));
            if (overwriteAction) {
                this.editedOlapTableSandbox(null);
            } 
        });
    }
    
    overwriteExistingOlapTable(oldItem: ongoingTaskOlapEtlTableModel, newItem: ongoingTaskOlapEtlTableModel) {
        if (oldItem.dirtyFlag().isDirty() || newItem.hasUpdates(oldItem)) {
            newItem.dirtyFlag().forceDirty();
        }
        
        this.editedOlapEtl().olapTables.replace(oldItem, newItem);
    }
    
    deleteOlapTable(olapTableModel: ongoingTaskOlapEtlTableModel) {
        this.editedOlapEtl().olapTables.remove(x => olapTableModel.tableName() === x.tableName());

        if (this.olapTableSelectedForEdit() === olapTableModel) {
            this.editedOlapTableSandbox(null);
            this.olapTableSelectedForEdit(null);
        }
    }
    
    editOlapTable(olapTableModel: ongoingTaskOlapEtlTableModel) {
        this.olapTableSelectedForEdit(olapTableModel);
        this.editedOlapTableSandbox(new ongoingTaskOlapEtlTableModel(olapTableModel.toDto(), false));
    }
}

export = editOlapEtlTask;

import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import ongoingTaskSubscriptionEdit = require("models/database/tasks/ongoingTaskSubscriptionEditModel");
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import saveSubscriptionTaskCommand = require("commands/database/tasks/saveSubscriptionTaskCommand");
import testSubscriptionTaskCommand = require("commands/database/tasks/testSubscriptionTaskCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import documentBasedColumnsProvider = require("widgets/virtualGrid/columns/providers/documentBasedColumnsProvider");
import columnsSelector = require("viewmodels/partial/columnsSelector");
import documentObject = require("models/database/documents/document");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import subscriptionConnectionDetailsCommand = require("commands/database/tasks/getSubscriptionConnectionDetailsCommand");
import queryCompleter = require("common/queryCompleter");
import subscriptionRqlSyntax = require("viewmodels/database/tasks/subscriptionRqlSyntax");

type fetcherType = (skip: number, take: number) => JQueryPromise<pagedResult<documentObject>>;

class editSubscriptionTask extends viewModelBase {

    queryCompleter = queryCompleter.remoteCompleter(this.activeDatabase, ko.observableArray([]), "Select"); // we intentionally pass empty indexes here as subscriptions works only on collections
    editedSubscription = ko.observable<ongoingTaskSubscriptionEdit>();
    isAddingNewSubscriptionTask = ko.observable<boolean>(true);

    enableTestArea = ko.observable<boolean>(false);
    testResultsLimit = ko.observable<number>(10);

    private gridController = ko.observable<virtualGridController<any>>();
    columnsSelector = new columnsSelector<documentObject>();
    fetcher = ko.observable<fetcherType>();
    private columnPreview = new columnPreviewPlugin<documentObject>();

    dirtyResult = ko.observable<boolean>(false);
    isFirstRun = true;

    spinners = {
        globalToggleDisable: ko.observable<boolean>(false)
    };

    constructor() {
        super();
        this.bindToCurrentInstance("setStartingPointType");
        aceEditorBindingHandler.install();
    }

    activate(args: any) { 
        super.activate(args);
        const deferred = $.Deferred<void>();

        if (args.taskId) { 

            // 1. Editing an existing task
            this.isAddingNewSubscriptionTask(false);

            // 1.1 Get general info
            ongoingTaskInfoCommand.forSubscription(this.activeDatabase(), args.taskId, args.taskName)
                .execute()
                .done((result: Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails) => {
                    this.editedSubscription(new ongoingTaskSubscriptionEdit(result, false));

                    deferred.resolve();

                    // 1.2 Check if connection is live
                    this.editedSubscription().liveConnection(false);
                    new subscriptionConnectionDetailsCommand(this.activeDatabase(), args.taskId, args.taskName, this.editedSubscription().responsibleNode().NodeUrl)
                        .execute()
                        .done((result: Raven.Server.Documents.TcpHandlers.SubscriptionConnectionDetails) => {
                            this.editedSubscription().liveConnection(!!result.ClientUri);
                        });
                })
                .fail(() => { 
                    deferred.reject();
                    router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
                });
        }
        else {
            // 2. Creating a new task
            this.isAddingNewSubscriptionTask(true);
            this.editedSubscription(ongoingTaskSubscriptionEdit.empty());
            deferred.resolve();
        }
        
        return deferred;
    }

    compositionComplete() {
        super.compositionComplete();
        document.getElementById('taskName').focus(); 
    }

    saveSubscription() {
        //1. Validate model
        if (!this.validate()) { 
             return;
        }

        // 2. Create/add the new replication task
        const dtoDataFromUI = this.editedSubscription().dataFromUI();

        new saveSubscriptionTaskCommand(this.activeDatabase(), dtoDataFromUI, this.editedSubscription().taskId, this.editedSubscription().taskState()) 
            .execute()
            .done(() => {
                this.goToOngoingTasksView();
            });
    }

    cloneSubscription() {
        this.isAddingNewSubscriptionTask(true);
        this.editedSubscription().taskName("");
        this.editedSubscription().taskId = null;
        document.getElementById('taskName').focus(); 
    }
   
    cancelOperation() {
        this.goToOngoingTasksView();
    }

    setStartingPointType(startingPointType: subscriptionStartType) {
        this.editedSubscription().startingPointType(startingPointType);
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
    }

    private validate(): boolean {
        let valid = true;

        if (!this.isValid(this.editedSubscription().validationGroup))
            valid = false;

        return valid;
    }

    runTest() {
        if (!this.isValid(this.editedSubscription().validationGroup) || this.testResultsLimit() < 1) {
            return;
        }

        this.columnsSelector.reset();

        const fetcherMethod = (s: number, t: number) => this.fetchTestDocuments(s, t);
        this.fetcher(fetcherMethod);

        if (this.isFirstRun) {
            const extraClassProvider = (item: documentObject | Raven.Server.Documents.Handlers.DocumentWithException) => {
                const documentItem = item as Raven.Server.Documents.Handlers.DocumentWithException;
                return documentItem.Exception ? "exception-row" : "";
            };

            const documentsProvider = new documentBasedColumnsProvider(this.activeDatabase(), this.gridController(), {
                showRowSelectionCheckbox: false,
                showSelectAllCheckbox: false,
                enableInlinePreview: true,
                columnOptions: {
                    extraClass: extraClassProvider
                }
            });
         
            this.columnsSelector.init(this.gridController(),
                fetcherMethod,
                (w, r) => documentsProvider.findColumns(w, r, ["Exception", "__metadata"]),
                (results: pagedResult<documentObject>) => documentBasedColumnsProvider.extractUniquePropertyNames(results));

            const grid = this.gridController();
            grid.dirtyResults.subscribe(dirty => this.dirtyResult(dirty));

            grid.headerVisible(true);

            this.columnPreview.install("virtual-grid", ".tooltip", (doc: documentObject, column: virtualColumn, e: JQueryEventObject, onValue: (context: any) => void) => {
                if (column instanceof textColumn) {
                    const value = column.getCellValue(doc);
                    if (!_.isUndefined(value)) {
                        const json = JSON.stringify(value, null, 4);
                        const html = Prism.highlight(json, (Prism.languages as any).javascript);
                        onValue(html);
                    }
                }
            });

            this.fetcher.subscribe(() => grid.reset());
        }

        this.isFirstRun = false;
    }

    private fetchTestDocuments(start: number, take: number): JQueryPromise<pagedResult<documentObject>> {
        const dtoDataFromUI = this.editedSubscription().dataFromUI();
        const resultsLimit = this.testResultsLimit() || 1;

        this.spinners.globalToggleDisable(true);

        return new testSubscriptionTaskCommand(this.activeDatabase(), dtoDataFromUI, resultsLimit)
            .execute()
            .always(() => this.spinners.globalToggleDisable(false));
    }

    toggleTestArea() {
        // 1. Test area is closed and we want to open it
        if (!this.enableTestArea()) {
            if (this.isValid(this.editedSubscription().validationGroup)) {
                this.enableTestArea(true);
                this.runTest();
            }
            else return;
        }
        else {
            // 2. Test area is open and we want to close it
            this.enableTestArea(false);
            this.columnsSelector.reset();
        }
    }

    syntaxHelp() {
        const viewModel = new subscriptionRqlSyntax();
            app.showBootstrapDialog(viewModel);
        }
}

export = editSubscriptionTask;

import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import ongoingTaskSubscriptionEdit = require("models/database/tasks/ongoingTaskSubscriptionEditModel");
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import collection = require("models/database/documents/collection");
import saveSubscriptionTaskCommand = require("commands/database/tasks/saveSubscriptionTaskCommand");
import testSubscriptionTaskCommand = require("commands/database/tasks/testSubscriptionTaskCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import popoverUtils = require("common/popoverUtils");
import jsonUtil = require("common/jsonUtil");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import documentBasedColumnsProvider = require("widgets/virtualGrid/columns/providers/documentBasedColumnsProvider");
import columnsSelector = require("viewmodels/partial/columnsSelector");
import documentObject = require("models/database/documents/document");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");

type fetcherType = (skip: number, take: number) => JQueryPromise<pagedResult<documentObject>>;

class editSubscriptionTask extends viewModelBase {

    editedSubscription = ko.observable<ongoingTaskSubscriptionEdit>();
    isSaveEnabled: KnockoutComputed<boolean>;
    isAddingNewSubscriptionTask = ko.observable<boolean>(true);

    canOpenTestArea: KnockoutComputed<boolean>; 
    canRunTest: KnockoutComputed<boolean>; 
    enableTestArea = ko.observable<boolean>(false);
    testResultsLimit = ko.observable<number>();

    private gridController = ko.observable<virtualGridController<any>>();
    private customFunctionsContext: object;
    columnsSelector = new columnsSelector<documentObject>();
    fetcher = ko.observable<fetcherType>();
    private columnPreview = new columnPreviewPlugin<documentObject>();
    dirtyResult = ko.observable<boolean>(false);

    constructor() {
        super();
        this.bindToCurrentInstance("useCollection", "setStartingPointType");
        aceEditorBindingHandler.install();
    }

    activate(args: any) { 
        super.activate(args);
        const deferred = $.Deferred<void>();

        if (args.taskId) { 

            // 1. Editing an existing task
            this.isAddingNewSubscriptionTask(false);

            new ongoingTaskInfoCommand(this.activeDatabase(), "Subscription", args.taskId, args.taskName)
                .execute()
                .done((result: Raven.Client.Documents.Subscriptions.SubscriptionState) => {
                    this.editedSubscription(new ongoingTaskSubscriptionEdit(result));

                    if (this.editedSubscription().collection()) {
                        this.editedSubscription().getCollectionRevisionsSettings()
                            .done(() => deferred.resolve());
                    } else {
                        deferred.resolve();
                    }
                })
                .fail(() => router.navigate(appUrl.forOngoingTasks(this.activeDatabase())));
        }
        else {
            // 2. Creating a new task
            this.isAddingNewSubscriptionTask(true);
            this.editedSubscription(ongoingTaskSubscriptionEdit.empty());
            deferred.resolve();
        }

        deferred.always(() => this.initObservables());
        return deferred;
    }

    private initObservables() {

        this.dirtyFlag = new ko.DirtyFlag([
            this.editedSubscription().taskName,
            this.editedSubscription().collection,
            this.editedSubscription().includeRevisions,
            this.editedSubscription().script,
            this.editedSubscription().startingPointType
        ], false, jsonUtil.newLineNormalizingHashFunction); 

        this.isSaveEnabled = ko.pureComputed(() => {
            return this.dirtyFlag().isDirty();
        });   

        this.canOpenTestArea = ko.pureComputed(() => {
            return this.isValid(this.editedSubscription().validationGroup);
        });

        this.canRunTest = ko.pureComputed(() => {
                return this.isValid(this.editedSubscription().validationGroup) && this.testResultsLimit() >= 1;
        });
    }

    attached() {
        super.attached();

        const jsCode = Prism.highlight("if (this.Votes < 10)\r\n" +
            "return;\r\n" +
            "var customer = LoadDocument(this.CustomerId);\r\n" +
            "return {\r\n" +
            "   Issue: this.Issue,\r\n" +
            "   Votes: this.Votes,\r\n" +
            "   Customer: {\r\n" +
            "        Name: customer.Name,\r\n" +
            "        Email: customer.Email\r\n" +
            "};",
            (Prism.languages as any).javascript);
            
         $("#scriptInfo").popover({
            html: true,
            trigger: "hover",
            container: "body",
            template: popoverUtils.longPopoverTemplate,
            content: `<p>Subscription Scripts are written in JavaScript. <br />Example: <pre>${jsCode}</pre></p>`
                + `<p>You can use following functions in your patch script:</p>`
                + `<ul>`
                + `<li><code>LoadDocument(documentIdToLoad)</code> - loads document by id`
                + `</ul>`
        });
    }

    compositionComplete() {
        super.compositionComplete();
        document.getElementById('taskName').focus(); 
        document.getElementById("toggle-transform-script").click();
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
                this.dirtyFlag().reset(); 
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

    useCollection(collectionToUse: collection) {
        this.editedSubscription().collection(collectionToUse.name);
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
        this.columnsSelector.reset();
        const fetcherMethod = (s: number, t: number) => this.fetchTestDocuments(s, t);

        this.fetcher(fetcherMethod);

        const grid = this.gridController();

        grid.withEvaluationContext(this.customFunctionsContext);

        const documentsProvider = new documentBasedColumnsProvider(this.activeDatabase(), grid, this.editedSubscription().collections().map(x => x.name), {
            showRowSelectionCheckbox: false,
            showSelectAllCheckbox: false,
            enableInlinePreview: true
        });

        this.columnsSelector.init(grid,
            fetcherMethod,
            (w, r) => documentsProvider.findColumns(w, r),
            (results: pagedResult<documentObject>) => documentBasedColumnsProvider.extractUniquePropertyNames(results));

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

    private fetchTestDocuments(start: number, take: number): JQueryPromise<pagedResult<documentObject>> {
        const dtoDataFromUI = this.editedSubscription().dataFromUI();
        const resultsLimit = this.testResultsLimit() || 1;

        return new testSubscriptionTaskCommand(this.activeDatabase(), dtoDataFromUI, resultsLimit)
            .execute();
    }

    toggleTestArea() {
        this.enableTestArea(!this.enableTestArea());

        if (!this.enableTestArea()) {
            this.columnsSelector.reset();
            this.fetcher((s: number, t: number) => this.fetchTestDocuments(s, t));
        }
    }
}

export = editSubscriptionTask;
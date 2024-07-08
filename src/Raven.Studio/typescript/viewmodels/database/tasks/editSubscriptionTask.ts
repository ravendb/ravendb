import app = require("durandal/app");
import appUrl = require("common/appUrl");
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
import subscriptionRqlSyntax = require("viewmodels/database/tasks/subscriptionRqlSyntax");
import eventsCollector = require("common/eventsCollector");
import generalUtils = require("common/generalUtils");
import rqlLanguageService = require("common/rqlLanguageService");
import { highlight, languages } from "prismjs";
import shardViewModelBase from "viewmodels/shardViewModelBase";
import database from "models/resources/database";
import licenseModel from "models/auth/licenseModel";
import { EditSubscriptionTaskInfoHub } from "./EditSubscriptionTaskInfoHub";
import assertUnreachable from "components/utils/assertUnreachable";
import popoverUtils = require("common/popoverUtils");
import { LicenseLimitReachStatus, getLicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import getDatabaseLicenseLimitsUsage = require("commands/licensing/getDatabaseLicenseLimitsUsage");
import getClusterLicenseLimitsUsage = require("commands/licensing/getClusterLicenseLimitsUsage");

type testTabName = "results" | perCollectionIncludes;
type fetcherType = (skip: number, take: number) => JQueryPromise<pagedResult<documentObject>>;


class perCollectionIncludes {
    name: string;
    total = ko.observable<number>(0);
    items = new Map<string, documentObject>();

    constructor(name: string) {
        this.name = name;
    }
}

class editSubscriptionTask extends shardViewModelBase {

    languageService: rqlLanguageService;
    
    view = require("views/database/tasks/editSubscriptionTask.html");
    taskResponsibleNodeSectionView = require("views/partial/taskResponsibleNodeSection.html");
    pinResponsibleNodeTextScriptView = require("views/partial/pinResponsibleNodeTextScript.html");

    infoHubView: ReactInKnockout<typeof EditSubscriptionTaskInfoHub>;

    editedSubscription = ko.observable<ongoingTaskSubscriptionEdit>();
    isAddingNewSubscriptionTask = ko.observable<boolean>(true);

    cloneButtonTitle: KnockoutComputed<string>;
    clusterLimitStatus: KnockoutComputed<LicenseLimitReachStatus>;
    databaseLimitStatus: KnockoutComputed<LicenseLimitReachStatus>;     
    databaseLicenseLimitsUsage = ko.observable<Raven.Server.Commercial.DatabaseLicenseLimitsUsage>();
    clusterLicenseLimitsUsage = ko.observable<Raven.Server.Commercial.LicenseLimitsUsage>();

    hasRevisionsInSubscriptions = licenseModel.getStatusValue("HasRevisionsInSubscriptions");
    maxNumberOfSubscriptionsPerCluster = licenseModel.getStatusValue("MaxNumberOfSubscriptionsPerCluster");
    maxNumberOfSubscriptionsPerDatabase = licenseModel.getStatusValue("MaxNumberOfSubscriptionsPerDatabase");

    possibleMentors = ko.observableArray<string>([]);
    
    enableTestArea = ko.observable<boolean>(false);
    testResultsLimit = ko.observable<number>(10);
    testTimeLimit = ko.observable<number>();

    private gridController = ko.observable<virtualGridController<any>>();
    columnsSelector = new columnsSelector<documentObject>();
    resultsFetcher = ko.observable<fetcherType>();
    effectiveFetcher = ko.observable<fetcherType>();
    private columnPreview = new columnPreviewPlugin<documentObject>();

    dirtyResult = ko.observable<boolean>(false);
    isFirstRun = true;
    
    resultsCount = ko.observable<number>(0);
    includesCache = ko.observableArray<perCollectionIncludes>([]);
    
    currentTab = ko.observable<testTabName>("results");
    
    canUseChangeVectorAsStartingPoint: KnockoutComputed<boolean>;

    spinners = {
        globalToggleDisable: ko.observable<boolean>(false)
    };

    constructor(db: database) {
        super(db);
        this.bindToCurrentInstance("setStartingPointType", "goToTab", "setState");
        aceEditorBindingHandler.install();
        
        this.languageService = new rqlLanguageService(this.db, ko.observableArray([]), "Select"); // we intentionally pass empty indexes here as subscriptions works only on collections
        
        this.canUseChangeVectorAsStartingPoint = ko.pureComputed(() => !this.db.isSharded());

        this.infoHubView = ko.pureComputed(() => ({
            component: EditSubscriptionTaskInfoHub
        }));

        this.databaseLimitStatus = ko.pureComputed(() => {
            return getLicenseLimitReachStatus(this.databaseLicenseLimitsUsage()?.NumberOfSubscriptions, this.maxNumberOfSubscriptionsPerDatabase);
        });
        
        this.clusterLimitStatus = ko.pureComputed(() => {
            return getLicenseLimitReachStatus(this.clusterLicenseLimitsUsage()?.NumberOfSubscriptionsInCluster, this.maxNumberOfSubscriptionsPerCluster);
        });

        this.cloneButtonTitle = ko.pureComputed(() => {
            if (this.databaseLimitStatus() === "limitReached") {
                return "The database has reached the maximum number of subscriptions allowed by your license per database.";
            }
            if (this.clusterLimitStatus() === "limitReached") {
                return "The cluster has reached the maximum number of subscriptions allowed by your license per cluster.";
            }

            return "Clone this subscription";
        });
    }

    activate(args: any) { 
        super.activate(args);
        const deferred = $.Deferred<void>();

        this.loadPossibleMentors();
        
        this.fetchDatabaseLicenseLimitsUsage();
        this.fetchClusterLicenseLimitsUsage();
        
        if (args.taskId) { 

            // 1. Editing an existing task
            this.isAddingNewSubscriptionTask(false);

            // 1.1 Get general info
            ongoingTaskInfoCommand.forSubscription(this.db, args.taskId, args.taskName)
                .execute()
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription) => {
                    this.editedSubscription(new ongoingTaskSubscriptionEdit(result));

                    deferred.resolve();

                    // 1.2 Check if connection is live
                    this.editedSubscription().liveConnection(false);
                    new subscriptionConnectionDetailsCommand(this.db, args.taskId, args.taskName, this.editedSubscription().responsibleNode().NodeTag)
                        .execute()
                        .done((result: Raven.Server.Documents.TcpHandlers.SubscriptionConnectionsDetails) => {
                            this.editedSubscription().liveConnection(!!result.Results.length);
                        });
                })
                .fail(() => { 
                    deferred.reject();
                    router.navigate(appUrl.forOngoingTasks(this.db));
                });
        } else {
            // 2. Creating a new task
            this.isAddingNewSubscriptionTask(true);
            this.editedSubscription(ongoingTaskSubscriptionEdit.empty());
            deferred.resolve();
        }
        
        deferred
            .done(() => this.dirtyFlag = this.editedSubscription().dirtyFlag);

        return $.when<any>(deferred);
    }

    attached() {
        super.attached();

        popoverUtils.longWithHover($(".archive-info"),
            {
                content:
                    `<div class="margin-bottom-sm margin-top-sm margin-left-xs">
                         <div class="margin-bottom-xs">
                              Available options:
                         </div>
                         <ul class="small">
                             <li class="margin-bottom-xs"><strong>Archived Only</strong><br />
                                 Only archived documents will be included in processing.
                             </li>
                             <li class="margin-bottom-xs"><strong>Exclude Archived</strong> (Default behavior)<br />
                                 Only non-archived documents will be included in processing.
                             </li>
                             <li class="margin-bottom-xs"><strong>Include Archived</strong><br />
                                 Both archived and non-archived documents will be included in processing.
                             </li>
                         </ul>
                     </div>`
            });
    }
    
    detached() {
        super.detached();
        
        this.languageService.dispose();
    }

    private loadPossibleMentors() {
        const members = this.db.nodes()
            .filter(x => x.type === "Member")
            .map(x => x.tag);

        this.possibleMentors(members);
    }

    compositionComplete() {
        super.compositionComplete();

        $('.edit-subscription-task [data-toggle="tooltip"]').tooltip();

        document.getElementById('taskName').focus();

        const queryEditor = aceEditorBindingHandler.getEditorBySelection($(".query-source"));
        
        this.editedSubscription().query.throttle(500).subscribe(() => {
            this.languageService.syntaxCheck(queryEditor);
        });
    }

    saveSubscription() {
        //1. Validate model
        if (!this.validate()) { 
             return;
        }
        
        eventsCollector.default.reportEvent("subscription-task", "save");

        // 2. Create/add the new replication task
        const dto = this.editedSubscription().toDto();

        new saveSubscriptionTaskCommand(this.db, dto, this.editedSubscription().taskId)
            .execute()
            .done(() => {
                this.dirtyFlag().reset();
                this.goToOngoingTasksView();
            });
    }
    
    goToTab(tabToUse: testTabName) {
        this.currentTab(tabToUse);
        
        if (tabToUse === "results") {
            this.effectiveFetcher(this.resultsFetcher());
        } else {
            this.effectiveFetcher(() => {
                return $.when({
                    items: Array.from(tabToUse.items.values()).map(x => new documentObject(x)),
                    totalResultCount: tabToUse.total()
                });
            });
        }
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
        if (startingPointType === "Change Vector" && !this.canUseChangeVectorAsStartingPoint()) {
            return;
        }
        
        this.editedSubscription().startingPointType(startingPointType);
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.db));
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
        
        this.includesCache.removeAll();

        eventsCollector.default.reportEvent("subscription-task", "test");
        
        this.columnsSelector.reset();

        const fetcherMethod = () => this.fetchTestDocuments();
        this.effectiveFetcher(fetcherMethod);

        if (this.isFirstRun) {
            const extraClassProvider = (item: documentObject | Raven.Server.Documents.Handlers.DocumentWithException) => {
                const documentItem = item as Raven.Server.Documents.Handlers.DocumentWithException;
                return documentItem.Exception ? "exception-row" : "";
            };

            const documentsProvider = new documentBasedColumnsProvider(this.db, this.gridController(), {
                showRowSelectionCheckbox: false,
                showSelectAllCheckbox: false,
                enableInlinePreview: true,
                columnOptions: {
                    extraClass: extraClassProvider
                }
            });
         
            this.columnsSelector.init(this.gridController(),
                (s, t) => this.effectiveFetcher()(s, t),
                (w, r) => documentsProvider.findColumns(w, r, ["Exception", "__metadata"]),
                (results: pagedResult<documentObject>) => documentBasedColumnsProvider.extractUniquePropertyNames(results));

            const grid = this.gridController();
            grid.dirtyResults.subscribe(dirty => this.dirtyResult(dirty));

            grid.headerVisible(true);

            this.columnPreview.install("virtual-grid", ".js-subscription-task-tooltip", 
                (doc: documentObject, column: virtualColumn, e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy: string) => void) => {
                if (column instanceof textColumn) {
                    const value = column.getCellValue(doc);
                    if (!_.isUndefined(value)) {
                        if (column.header === "Exception" && _.isString(value)) {
                            const formattedValue = _.replace(generalUtils.escapeHtml(value), "\r\n", "<Br />");
                            onValue(formattedValue, value);
                        } else {
                            const json = JSON.stringify(value, null, 4);
                            const html = highlight(json, languages.javascript, "js");
                            onValue(html, json);
                        }
                    }
                }
            });

            this.effectiveFetcher.subscribe(() => {
                this.columnsSelector.reset();
                grid.reset();
            });
        }

        this.isFirstRun = false;
    }

    private fetchTestDocuments(): JQueryPromise<pagedResult<documentObject>> {
        const dto = this.editedSubscription().toDto();
        const resultsLimit = this.testResultsLimit() || 1;
        const timeLimit = this.testTimeLimit() || 15;

        this.spinners.globalToggleDisable(true);

        return new testSubscriptionTaskCommand(this.db, dto, resultsLimit, timeLimit)
            .execute()
            .done(result => {
                this.resultsCount(result.items.length);
                this.cacheResults(result.items);
                this.onIncludesLoaded(result.includes);
            })
            .always(() => this.spinners.globalToggleDisable(false));
    }

    private fetchDatabaseLicenseLimitsUsage() {
        new getDatabaseLicenseLimitsUsage(this.db)
            .execute()
            .done((x) => {
                this.databaseLicenseLimitsUsage(x);
            });
    }

    private fetchClusterLicenseLimitsUsage() {
        new getClusterLicenseLimitsUsage()
            .execute()
            .done((x) => {
                this.clusterLicenseLimitsUsage(x);
            });
    }
    
    private cacheResults(items: Array<documentObject>) {
        // precompute value
        const mappedResult = {
            items: Array.from(items.values()).map(x => new documentObject(x)),
            totalResultCount: items.length
        };
        
        this.resultsFetcher(() => $.when(mappedResult));
    }

    toggleTestArea() {
        // 1. Test area is closed and we want to open it
        if (!this.enableTestArea()) {
            if (this.isValid(this.editedSubscription().validationGroup)) {
                this.enableTestArea(true);
                this.runTest();
            } else return;
        } else {
            // 2. Test area is open and we want to close it
            this.enableTestArea(false);
            this.columnsSelector.reset();
        }
    }

    formatArchivedDataProcessingBehavior(mode: Raven.Client.Documents.DataArchival.ArchivedDataProcessingBehavior) {
        if (!mode) {
            return "Default";
        }
        switch (mode) {
            case "ArchivedOnly":
                return "Archived Only";
            case "IncludeArchived":
                return "Include Archived";
            case "ExcludeArchived":
                return "Exclude Archived";
            default:
                assertUnreachable(mode);
        }
    }
    
    private onIncludesLoaded(includes: dictionary<any>) {
        _.forIn(includes, (doc, id) => {
            const metadata = doc['@metadata'];
            const collection = (metadata ? metadata["@collection"] : null) || "@unknown";

            let perCollectionCache = this.includesCache().find(x => x.name === collection);
            if (!perCollectionCache) {
                perCollectionCache = new perCollectionIncludes(collection);
                this.includesCache.push(perCollectionCache);
            }
            perCollectionCache.items.set(id, doc);
        });

        this.includesCache().forEach(cache => {
            cache.total(cache.items.size);
        });
    }

    syntaxHelp() {
        const viewModel = new subscriptionRqlSyntax();
            app.showBootstrapDialog(viewModel);
    }

    setState(state: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState): void {
        this.editedSubscription().taskState(state);
    }
}

export = editSubscriptionTask;

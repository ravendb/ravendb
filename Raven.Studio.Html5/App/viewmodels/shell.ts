/// <reference path="../../Scripts/typings/nprogress/nprogress.d.ts" />
/// <reference path="../../Scripts/typings/bootstrap/bootstrap.d.ts" />
/// <reference path="../../Scripts/typings/jquery.blockUI/jquery.blockUI.d.ts" />
import router = require("plugins/router");
import app = require("durandal/app");
import sys = require("durandal/system");
import viewLocator = require("durandal/viewLocator");

import resource = require("models/resource");
import database = require("models/database");
import fileSystem = require("models/filesystem/filesystem");
import counterStorage = require("models/counter/counterStorage");
import documentClass = require("models/document");
import collection = require("models/collection");
import uploadItem = require("models/uploadItem");
import changeSubscription = require("models/changeSubscription");
import license = require("models/license");

import appUrl = require("common/appUrl");
import uploadQueueHelper = require("common/uploadQueueHelper");
import alertArgs = require("common/alertArgs");
import alertType = require("common/alertType");
import pagedList = require("common/pagedList");
import dynamicHeightBindingHandler = require("common/dynamicHeightBindingHandler");
import autoCompleteBindingHandler = require("common/autoCompleteBindingHandler");
import enableResizeBindingHandler = require("common/enableResizeBindingHandler");
import helpBindingHandler = require("common/helpBindingHandler");
import changesApi = require("common/changesApi");
import changesContext = require("common/changesContext");
import oauthContext = require("common/oauthContext");
import messagePublisher = require("common/messagePublisher");
import apiKeyLocalStorage = require("common/apiKeyLocalStorage");

import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getDatabasesCommand = require("commands/getDatabasesCommand");
import getServerBuildVersionCommand = require("commands/getServerBuildVersionCommand");
import getLatestServerBuildVersionCommand = require("commands/getLatestServerBuildVersionCommand");
import getClientBuildVersionCommand = require("commands/getClientBuildVersionCommand");
import getLicenseStatusCommand = require("commands/getLicenseStatusCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/getDocumentsMetadataByIDPrefixCommand");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import getFileSystemsCommand = require("commands/filesystem/getFileSystemsCommand");
import getFileSystemStatsCommand = require("commands/filesystem/getFileSystemStatsCommand");
import getCounterStoragesCommand = require("commands/counter/getCounterStoragesCommand");
import getSystemDocumentCommand = require("commands/getSystemDocumentCommand");
import getServerConfigsCommand = require("commands/getServerConfigsCommand");
import getStudioConfig = require("commands/getStudioConfig");

import viewModelBase = require("viewmodels/viewModelBase");
import recentErrors = require("viewmodels/recentErrors");
import enterApiKey = require("viewmodels/enterApiKey");
import latestBuildReminder = require("viewmodels/latestBuildReminder");
import extensions = require("common/extensions");
import serverBuildReminder = require("common/serverBuildReminder");
import eventSourceSettingStorage = require("common/eventSourceSettingStorage");
import environmentColor = require("models/environmentColor");
import recentQueriesStorage = require("common/recentQueriesStorage");

class shell extends viewModelBase {
    private router = router;
    static studioConfigDocumentId = "Raven/StudioConfig";
    static selectedEnvironmentColorStatic = ko.observable<environmentColor>(new environmentColor("Default", "#f8f8f8"));
    static originalEnviromentColor = ko.observable<environmentColor>(shell.selectedEnvironmentColorStatic());
    selectedColor = shell.selectedEnvironmentColorStatic;
    selectedEnviromentText = ko.computed(() => this.selectedColor().name + " Enviroment");
    canShowEnviromentText = ko.computed(() => this.selectedColor().name != "Default");

    renewOAuthTokenTimeoutId: number;
    showContinueTestButton = ko.computed(() => viewModelBase.hasContinueTestOption());
    showLogOutButton: KnockoutComputed<boolean>;
    static isGlobalAdmin = ko.observable<boolean>(false);
    static canReadWriteSettings = ko.observable<boolean>(false);
    static canReadSettings = ko.observable<boolean>(false);
    static canExposeConfigOverTheWire = ko.observable<boolean>(false);
    maxResourceNameWidth: KnockoutComputed<string>;
    isLoadingStatistics = ko.computed(() => !!this.lastActivatedResource() && !this.lastActivatedResource().statistics()).extend({ rateLimit: 100 });

    static databases = ko.observableArray<database>();
    listedResources: KnockoutComputed<resource[]>;
    systemDatabase: database;
    isSystemConnected: KnockoutComputed<boolean>;
    isActiveDatabaseDisabled: KnockoutComputed<boolean>;
    canShowDatabaseNavbar = ko.computed(() =>
        !!this.lastActivatedResource()
        && this.lastActivatedResource().type == database.type
        && (this.appUrls.isAreaActive('databases')() || this.appUrls.isAreaActive('resources')()));
    databasesLoadedTask: JQueryPromise<any>;
    goToDocumentSearch: KnockoutObservable<string>;
    goToDocumentSearchResults = ko.observableArray<string>();

    static fileSystems = ko.observableArray<fileSystem>();
    isActiveFileSystemDisabled: KnockoutComputed<boolean>;
    canShowFileSystemNavbar = ko.computed(() =>
        !!this.lastActivatedResource()
        && this.lastActivatedResource().type === fileSystem.type
        && (this.appUrls.isAreaActive('filesystems')() || this.appUrls.isAreaActive('resources')()));

    canShowFileSystemSettings = ko.computed(() => {
        if (!this.canShowFileSystemNavbar()) return false;
        var fs = <fileSystem> this.lastActivatedResource();
        return fs.activeBundles.contains("Versioning");
    });

    static counterStorages = ko.observableArray<counterStorage>();
    isCounterStorageDisabled: KnockoutComputed<boolean>;
    canShowCountersNavbar = ko.computed(() =>
        !!this.lastActivatedResource()
        && this.lastActivatedResource().type == counterStorage.type
        && (this.appUrls.isAreaActive('counterstorages')() || this.appUrls.isAreaActive('resources')()));

    canShowResourcesNavbar = ko.computed(() => {
        var canDb = this.canShowDatabaseNavbar();
        var canFs = this.canShowFileSystemNavbar();
        var canCnt = this.canShowCountersNavbar();
        return canDb || canFs || canCnt;
    });

    static resources = ko.computed(() => {
        var result = [].concat(shell.databases(), shell.fileSystems());
        return result.sort((a, b) => a.name.toLowerCase() > b.name.toLowerCase() ? 1 : -1);
    });

    currentConnectedResource: resource;
    currentAlert = ko.observable<alertArgs>();
    queuedAlert: alertArgs;
    serverBuildVersion = ko.observable<serverBuildVersionDto>();
    clientBuildVersion = ko.observable<clientBuildVersionDto>();
    localLicenseStatus: KnockoutObservable<licenseStatusDto> = license.licenseStatus;
    windowHeightObservable: KnockoutObservable<number>;
    appUrls: computedAppUrls;
    recordedErrors = ko.observableArray<alertArgs>();
    newIndexUrl = appUrl.forCurrentDatabase().newIndex;
    newTransformerUrl = appUrl.forCurrentDatabase().newTransformer;
    currentRawUrl = ko.observable<string>("");
    rawUrlIsVisible = ko.computed(() => this.currentRawUrl().length > 0);
    activeArea = ko.observable<string>("Databases");
    hasReplicationSupport = ko.computed(() => !!this.activeDatabase() && this.activeDatabase().activeBundles.contains("Replication"));
    showSplash = viewModelBase.showSplash;

    private globalChangesApi: changesApi;
    private static changeSubscriptionArray: changeSubscription[];

    constructor() {
        super();

        this.preLoadRecentErrorsView();
        extensions.install();

        this.showLogOutButton = ko.computed(() => {
            var lsApiKey = apiKeyLocalStorage.get();
            var contextApiKey = oauthContext.apiKey();
            return lsApiKey || contextApiKey;
        });
        oauthContext.enterApiKeyTask = this.setupApiKey();
        oauthContext.enterApiKeyTask.done(() => {
            this.globalChangesApi = new changesApi(appUrl.getSystemDatabase());
            this.notifications = this.createNotifications();
        });

        ko.postbox.subscribe("Alert", (alert: alertArgs) => this.showAlert(alert));
        ko.postbox.subscribe("LoadProgress", (alertType?: alertType) => this.dataLoadProgress(alertType));
        ko.postbox.subscribe("ActivateDatabaseWithName", (databaseName: string) => this.activateDatabaseWithName(databaseName));
        ko.postbox.subscribe("SetRawJSONUrl", (jsonUrl: string) => this.currentRawUrl(jsonUrl));
        ko.postbox.subscribe("SelectNone", () => this.selectNone());
        ko.postbox.subscribe("ActivateDatabase", (db: database) => this.activateDatabase(db));
        ko.postbox.subscribe("ActivateFilesystem", (fs: fileSystem) => this.activateFileSystem(fs));
        ko.postbox.subscribe("UploadFileStatusChanged", (uploadStatus: uploadItem) => this.uploadStatusChanged(uploadStatus));
        ko.postbox.subscribe("ChangesApiReconnected", (rs: resource) => this.reloadDataAfterReconnection(rs));

        this.currentConnectedResource = appUrl.getSystemDatabase();
        this.appUrls = appUrl.forCurrentDatabase();

        this.goToDocumentSearch = ko.observable<string>();
        this.goToDocumentSearch.throttle(250).subscribe(search => this.fetchGoToDocSearchResults(search));
        dynamicHeightBindingHandler.install();
        autoCompleteBindingHandler.install();
        enableResizeBindingHandler.install();
        helpBindingHandler.install();

        this.isSystemConnected = ko.computed(() => {
            var activeDb = this.activeDatabase();
            var systemDb = this.systemDatabase;
            return (!!activeDb && !!systemDb) ? systemDb.name != activeDb.name : false;
        });

        this.isActiveDatabaseDisabled = ko.computed(() => {
            var activeDb: database = this.activeDatabase();
            return !!activeDb ? activeDb.disabled() || !activeDb.isLicensed() : false;
        });

        this.isActiveFileSystemDisabled = ko.computed(() => {
            var activeFs = this.activeFilesystem();
            return !!activeFs ? activeFs.disabled() || !activeFs.isLicensed() : false;
        });

        this.isCounterStorageDisabled = ko.computed(() => {
            var activeCs = this.activeCounterStorage();
            return !!activeCs ? activeCs.disabled() : false;
        });

        this.listedResources = ko.computed(() => {
            var currentResource = this.lastActivatedResource();
            if (!!currentResource) {
                return shell.resources().filter(rs => (rs.type != currentResource.type || (rs.type == currentResource.type && rs.name != currentResource.name)) && rs.name != '<system>');
            }
            return shell.resources();
        });

        this.clientBuildVersion.subscribe(v => viewModelBase.clientVersion("3.0." + v.BuildVersion));
    }

    // Override canActivate: we can always load this page, regardless of any system db prompt.
    canActivate(args: any): any {
        return true;
    }

    activate(args: any) {
        super.activate(args, true);

        oauthContext.enterApiKeyTask.done(() => this.connectToRavenServer());

        NProgress.set(.7);
        router.map([
            { route: "admin/settings*details", title: "Admin Settings", moduleId: "viewmodels/adminSettings", nav: true, hash: this.appUrls.adminSettings },
            { route: ["", "resources"], title: "Resources", moduleId: "viewmodels/resources", nav: true, hash: this.appUrls.resourcesManagement },
            { route: "databases/documents", title: "Documents", moduleId: "viewmodels/documents", nav: true, hash: this.appUrls.documents },
            { route: "databases/conflicts", title: "Conflicts", moduleId: "viewmodels/conflicts", nav: true, hash: this.appUrls.conflicts },
            { route: "databases/patch", title: "Patch", moduleId: "viewmodels/patch", nav: true, hash: this.appUrls.patch },
            { route: "databases/upgrade", title: "Upgrade in progress", moduleId: "viewmodels/upgrade", nav: false, hash: this.appUrls.upgrade },
            { route: "databases/indexes*details", title: "Indexes", moduleId: "viewmodels/indexesShell", nav: true, hash: this.appUrls.indexes },
            { route: "databases/transformers*details", title: "Transformers", moduleId: "viewmodels/transformersShell", nav: false, hash: this.appUrls.transformers },
            { route: "databases/query*details", title: "Query", moduleId: "viewmodels/queryShell", nav: true, hash: this.appUrls.query(null) },
            { route: "databases/tasks*details", title: "Tasks", moduleId: "viewmodels/tasks", nav: true, hash: this.appUrls.tasks, },
            { route: "databases/settings*details", title: "Settings", moduleId: "viewmodels/settings", nav: true, hash: this.appUrls.settings },
            { route: "databases/status*details", title: "Status", moduleId: "viewmodels/status", nav: true, hash: this.appUrls.status },
            { route: "databases/edit", title: "Edit Document", moduleId: "viewmodels/editDocument", nav: false },
            { route: "filesystems/files", title: "Files", moduleId: "viewmodels/filesystem/filesystemFiles", nav: true, hash: this.appUrls.filesystemFiles },
            { route: "filesystems/search", title: "Search", moduleId: "viewmodels/filesystem/search", nav: true, hash: this.appUrls.filesystemSearch },
            { route: "filesystems/synchronization*details", title: "Synchronization", moduleId: "viewmodels/filesystem/synchronization", nav: true, hash: this.appUrls.filesystemSynchronization },
            { route: "filesystems/status*details", title: "Status", moduleId: "viewmodels/filesystem/status", nav: true, hash: this.appUrls.filesystemStatus },
            { route: "filesystems/tasks*details", title: "Tasks", moduleId: "viewmodels/filesystem/tasks", nav: true, hash: this.appUrls.filesystemTasks },
            { route: "filesystems/settings*details", title: "Settings", moduleId: "viewmodels/filesystem/settings", nav: true, hash: this.appUrls.filesystemSettings },
            { route: "filesystems/configuration", title: "Configuration", moduleId: "viewmodels/filesystem/configuration", nav: true, hash: this.appUrls.filesystemConfiguration },
            { route: "filesystems/edit", title: "Edit File", moduleId: "viewmodels/filesystem/filesystemEditFile", nav: false },
            { route: ["", "counterstorages"], title: "Counter Storages", moduleId: "viewmodels/counter/counterStorages", nav: true, hash: this.appUrls.couterStorages },
            { route: "counterstorages/counters", title: "counters", moduleId: "viewmodels/counter/counterStoragecounters", nav: true, hash: this.appUrls.counterStorageCounters },
            { route: "counterstorages/replication", title: "replication", moduleId: "viewmodels/counter/counterStorageReplication", nav: true, hash: this.appUrls.counterStorageReplication },
            { route: "counterstorages/stats", title: "stats", moduleId: "viewmodels/counter/counterStorageStats", nav: true, hash: this.appUrls.counterStorageStats },
            { route: "counterstorages/configuration", title: "configuration", moduleId: "viewmodels/counter/counterStorageConfiguration", nav: true, hash: this.appUrls.counterStorageConfiguration }
        ]).buildNavigationModel();

        // Show progress whenever we navigate.
        router.isNavigating.subscribe(isNavigating => this.showNavigationProgress(isNavigating));
        router.on('router:navigation:cancelled', () => this.showNavigationProgress(false));

        appUrl.mapUnknownRoutes(router);

        var self = this;

        window.addEventListener("beforeunload", self.destroyChangesApi.bind(self));

        $(window).bind('storage', (e: any) => {
            if (e.originalEvent.key == eventSourceSettingStorage.localStorageName) {
                if (!JSON.parse(e.originalEvent.newValue)) {
                    self.destroyChangesApi();
                } else {
                    // enable changes api
                    this.globalChangesApi = new changesApi(appUrl.getSystemDatabase());
                    this.notifications = this.createNotifications();
                }
            } else if (e.originalEvent.key == apiKeyLocalStorage.localStorageName) {
                this.onLogOut();
            }
        });

        this.maxResourceNameWidth = ko.computed(() => {
            if (this.canShowResourcesNavbar() && !!this.lastActivatedResource()) {
                var navigationLinksWidth = 50;
                if (this.canShowDatabaseNavbar()) {
                    navigationLinksWidth += 804;
                } else if (this.canShowFileSystemNavbar()) {
                    navigationLinksWidth += 600;
                } else if (this.canShowCountersNavbar()) {
                    navigationLinksWidth += 600; //todo: calculate
                }

                var brandWidth = this.getWidth("brand");
                var logOutWidth = this.getWidth("logOut");
                var continueTestWidth = this.getWidth("continueTest");
                var featureNameWidth = this.getWidth("featureName");

                var freeSpace = $(window).width() - (navigationLinksWidth + brandWidth + logOutWidth + continueTestWidth + featureNameWidth);
                var maxWidth = Math.floor(freeSpace);
                return maxWidth + "px";
            }
            return "1px";
        });

        $(window).resize(() => self.lastActivatedResource.valueHasMutated());
    }

    private getWidth(tag: string): number {
        return $("#" + tag).length > 0 ? $("#" + tag).width() : 0;
    }

    private destroyChangesApi() {
        this.cleanupNotifications();
        this.globalChangesApi.dispose();
        shell.disconnectFromResourceChangesApi();
    }

    // Called by Durandal when shell.html has been put into the DOM.
    // The view must be attached to the DOM before we can hook up keyboard shortcuts.
    attached() {
        super.attached();

        var target = document.getElementById("splash-spinner");
        //this.spinner.stop();
        this.showSplash.subscribe((show: boolean) => {
            if (show) {
                this.spinner.spin(target);
            } else {
                this.spinner.stop();
            }
        });

        jwerty.key("ctrl+alt+n", e => {
            e.preventDefault();
            this.newDocument();
        });

        jwerty.key("enter", e => {
            e.preventDefault();
            return false;
        }, this, "#goToDocInput");

        router.activeInstruction.subscribe(val => {
            if (!!val && val.config.route.split('/').length == 1) //if it's a root navigation item.
                this.activeArea(val.config.title);
        });

        sys.error = (e) => {
            console.error(e);
            messagePublisher.reportError("Failed to load routed module!", e);
        };
    }

    private preLoadRecentErrorsView() {
        // preload this view as in case of failure server can't serve it.
        viewLocator.locateView("views/recentErrors");
    }

    private fecthStudioConfigForDatabase(db: database) {
        new getStudioConfig(db)
            .execute()
            .done((doc: documentClass) => {
                var envColor = doc["EnvironmentColor"];
                if (envColor != null) {
                    shell.selectedEnvironmentColorStatic(new environmentColor(envColor.Name, envColor.BackgroundColor));
                }
            })
            .fail(() => shell.selectedEnvironmentColorStatic(shell.originalEnviromentColor()));
    }

    private activateDatabase(db: database) {
        if (db == null) {
            this.disconnectFromCurrentResource();
            return;
        }

        this.fecthStudioConfigForDatabase(db);

        var changeSubscriptionArray = () => [
            changesContext.currentResourceChangesApi().watchAllDocs(() => this.fetchDbStats(db)),
            changesContext.currentResourceChangesApi().watchAllIndexes(() => this.fetchDbStats(db)),
            changesContext.currentResourceChangesApi().watchBulks(() => this.fetchDbStats(db))
        ];
        var isNotADatabase = this.currentConnectedResource instanceof database === false;
        this.updateChangesApi(db, isNotADatabase, () => this.fetchDbStats(db), changeSubscriptionArray);

        shell.resources().forEach((r: resource) => r.isSelected(r instanceof database && r.name === db.name));
    }

    private fetchDbStats(db: database) {
        if (!!db && !db.disabled() && db.isLicensed()) {
            new getDatabaseStatsCommand(db, true)
                .execute()
                .done((result: databaseStatisticsDto) => db.saveStatistics(result));
        }
    }

    private activateFileSystem(fs: fileSystem) {
        if (fs == null) {
            this.disconnectFromCurrentResource();
            return;
        }

        this.fecthStudioConfigForDatabase(new database(fs.name));

        var changesSubscriptionArray = () => [
            changesContext.currentResourceChangesApi().watchFsFolders("", () => this.fetchFsStats(fs))
        ];
        var isNotAFileSystem = this.currentConnectedResource instanceof fileSystem === false;
        this.updateChangesApi(fs, isNotAFileSystem, () => this.fetchFsStats(fs), changesSubscriptionArray);

        shell.resources().forEach((r: resource) => r.isSelected(r instanceof fileSystem && r.name === fs.name));
    }

    private fetchFsStats(fs: fileSystem) {
        if (!!fs && !fs.disabled() && fs.isLicensed()) {
            new getFileSystemStatsCommand(fs)
                .execute()
                .done((result: filesystemStatisticsDto) => fs.saveStatistics(result))
                .fail((response: JQueryXHR) => messagePublisher.reportError("Failed to get file system stats", response.responseText, response.statusText));
        }
    }

    private disconnectFromCurrentResource() {
        shell.disconnectFromResourceChangesApi();
        this.lastActivatedResource(null);
        this.currentConnectedResource = appUrl.getSystemDatabase();
    }

    private updateChangesApi(rs: resource, isPreviousDifferentKind: boolean, fetchStats: () => void, subscriptionsArray: () => changeSubscription[]) {
        fetchStats();

        if (isPreviousDifferentKind || this.currentConnectedResource.name !== rs.name) {
            // disconnect from the current resource changes api and set the current connected resource
            shell.disconnectFromResourceChangesApi();
            this.currentConnectedResource = rs;
        }

        if ((!rs.disabled() && rs.isLicensed()) &&
            (isPreviousDifferentKind || changesContext.currentResourceChangesApi() == null)) {
            // connect to changes api, if it's not disabled and the changes api isn't already connected
            var changes = new changesApi(rs, 5000);
            changes.connectToChangesApiTask.done(() => {
                fetchStats();
                changesContext.currentResourceChangesApi(changes);
                shell.changeSubscriptionArray = subscriptionsArray();
            });
        }
    }

    setupApiKey() {
        // try to find api key as studio hash parameter
        var hash = window.location.hash;
        if (hash === "#has-api-key") {
            return this.showApiKeyDialog();
        } else if (hash.match(/#api-key/g)) {
            var match = /#api-key=(.*)/.exec(hash);
            if (match && match.length == 2) {
                oauthContext.apiKey(match[1]);
                apiKeyLocalStorage.setValue(match[1]);
            }
            var splittedHash = hash.split("&#api-key");
            var url = (splittedHash.length == 1) ? "#resources" : splittedHash[0];
            window.location.href = url;
        } else {
            var apiKeyFromStorage = apiKeyLocalStorage.get();
            if (apiKeyFromStorage) {
                oauthContext.apiKey(apiKeyFromStorage);
            }
        }

        oauthContext.authHeader.subscribe(h => {
            if (this.renewOAuthTokenTimeoutId) {
                clearTimeout(this.renewOAuthTokenTimeoutId);
                this.renewOAuthTokenTimeoutId = null;
            }
            if (h) {
                this.renewOAuthTokenTimeoutId = setTimeout(() => this.renewOAuthToken(), 25 * 60 * 1000);
            }
        });

        return $.Deferred().resolve();
    }

    private renewOAuthToken() {
        oauthContext.authHeader(null);
        new getDatabaseStatsCommand(this.systemDatabase).execute();
    }

    showNavigationProgress(isNavigating: boolean) {
        if (isNavigating) {
            NProgress.start();

            var currentProgress = parseFloat(NProgress.status);
            var newProgress = isNaN(currentProgress) ? 0.5 : currentProgress + (currentProgress / 2);
            NProgress.set(newProgress);
        } else {
            NProgress.done();
            this.activeArea(appUrl.checkIsAreaActive("filesystems") ? "File Systems" : "Databases");
        }
    }

    static reloadDatabases(active: database): JQueryPromise<database[]> {
        return new getDatabasesCommand()
            .execute()
            .done((results: database[]) => shell.updateResourceObservableArray(shell.databases, results, active));
    }

    static reloadFilesystems(active: fileSystem): JQueryPromise<fileSystem[]> {
        return new getFileSystemsCommand()
            .execute()
            .done((results: fileSystem[]) => shell.updateResourceObservableArray(shell.fileSystems, results, active));
    }

    static reloadCounterStorages(active: counterStorage): JQueryPromise<counterStorage[]> {
        return new getCounterStoragesCommand()
            .execute()
            .done((results: counterStorage[]) => shell.updateResourceObservableArray(shell.counterStorages, results, active));
    }

    private reloadDataAfterReconnection(rs: resource) {
        if (rs.name === "<system>") {
            this.fetchStudioConfig();
            this.fetchServerBuildVersion();
            this.fetchClientBuildVersion();
            this.fetchLicenseStatus();
            this.loadServerConfig();

            var databasesLoadTask = shell.reloadDatabases(this.activeDatabase());
            var fileSystemsLoadTask = shell.reloadFilesystems(this.activeFilesystem());
            var counterStoragesLoadTask = shell.reloadCounterStorages(this.activeCounterStorage());

            $.when(databasesLoadTask, fileSystemsLoadTask, counterStoragesLoadTask)
                .done(() => {
                    var connectedResource = this.currentConnectedResource;
                    var resourceObservableArray: any = (connectedResource instanceof database) ? shell.databases : (connectedResource instanceof fileSystem) ? shell.fileSystems : shell.counterStorages;
                    var activeResourceObservable: any = (connectedResource instanceof database) ? this.activeDatabase : (connectedResource instanceof fileSystem) ? this.activeFilesystem : this.activeCounterStorage;
                    this.selectNewActiveResourceIfNeeded(resourceObservableArray, activeResourceObservable);
                });
        }
    }

    private static updateResourceObservableArray(resourceObservableArray: KnockoutObservableArray<any>,
        recievedResourceArray: Array<any>, activeResourceObservable: any) {

        var deletedResources = [];

        resourceObservableArray().forEach((rs: resource) => {
            if (rs.name != '<system>') {
                var existingResource = recievedResourceArray.first((recievedResource: resource) => recievedResource.name == rs.name);
                if (existingResource == null) {
                    deletedResources.push(rs);
                }
            }
        });

        resourceObservableArray.removeAll(deletedResources);

        recievedResourceArray.forEach((recievedResource: resource) => {
            var foundResource: resource = resourceObservableArray().first((rs: resource) => rs.name == recievedResource.name);
            if (foundResource == null) {
                resourceObservableArray.push(recievedResource);
            } else {
                foundResource.disabled(recievedResource.disabled());
            }
        });
    }

    private selectNewActiveResourceIfNeeded(resourceObservableArray: KnockoutObservableArray<any>, activeResourceObservable: any) {
        var activeResource = activeResourceObservable();
        var actualResourceObservableArray = resourceObservableArray().filter(rs => rs.name != '<system>');

        if (!!activeResource && actualResourceObservableArray.contains(activeResource) == false) {
            if (actualResourceObservableArray.length > 0) {
                resourceObservableArray().first().activate();
            } else { //if (actualResourceObservableArray.length == 0)
                shell.disconnectFromResourceChangesApi();
                activeResourceObservable(null);
            }

            this.navigate(appUrl.forResources());
        }
    }

    createNotifications(): Array<changeSubscription> {
        return [
            this.globalChangesApi.watchDocsStartingWith("Raven/Databases/", (e) => this.changesApiFiredForResource(e, shell.databases, this.activeDatabase, logTenantType.Database)),
            this.globalChangesApi.watchDocsStartingWith("Raven/FileSystems/", (e) => this.changesApiFiredForResource(e, shell.fileSystems, this.activeFilesystem, logTenantType.Filesystem)),
            this.globalChangesApi.watchDocsStartingWith("Raven/Counters/", (e) => this.changesApiFiredForResource(e, shell.counterStorages, this.activeCounterStorage, logTenantType.CounterStorage)),
            this.globalChangesApi.watchDocsStartingWith(shell.studioConfigDocumentId, () => this.fetchStudioConfig()),
            this.globalChangesApi.watchDocsStartingWith("Raven/Alerts", () => this.fetchSystemDatabaseAlerts())
        ];
    }

    private changesApiFiredForResource(e: documentChangeNotificationDto,
        resourceObservableArray: KnockoutObservableArray<any>, activeResourceObservable: any, resourceType: logTenantType) {

        if (!!e.Id && (e.Type === "Delete" || e.Type === "Put")) {
            var receivedResourceName = e.Id.slice(e.Id.lastIndexOf('/') + 1);

            if (e.Type === "Delete") {
                var resourceToDelete = resourceObservableArray.first((rs: resource) => rs.name == receivedResourceName);
                if (!!resourceToDelete) {
                    resourceObservableArray.remove(resourceToDelete);

                    //this.selectNewActiveResourceIfNeeded(resourceObservableArray, activeResourceObservable);
                    if (resourceType == logTenantType.Database)
                        recentQueriesStorage.removeRecentQueries(resourceToDelete);
                }
            } else { // e.Type === "Put"
                var getSystemDocumentTask = new getSystemDocumentCommand(e.Id).execute();
                getSystemDocumentTask.done((dto: databaseDocumentDto) => {
                    var existingResource = resourceObservableArray.first((rs: resource) => rs.name == receivedResourceName);

                    if (existingResource == null) { // new resource
                        existingResource = this.createNewResource(resourceType, receivedResourceName, dto);
                        resourceObservableArray.unshift(existingResource);
                    } else {
                        if (existingResource.disabled() != dto.Disabled) { //disable status change
                            existingResource.disabled(dto.Disabled);
                            if (dto.Disabled == false && this.currentConnectedResource.name == receivedResourceName) {
                                existingResource.activate();
                            }
                        }
                    }

                    if (resourceType == logTenantType.Database) { //for databases, bundle change or indexing change
                        var bundles = !!dto.Settings["Raven/ActiveBundles"] ? dto.Settings["Raven/ActiveBundles"].split(";") : [];
                        existingResource.activeBundles(bundles);


                        var indexingDisabled = this.getIndexingDisbaledValue(dto.Settings["Raven/IndexingDisabled"]);
                        existingResource.indexingDisabled(indexingDisabled);

                        var isRejectclientsEnabled = this.getIndexingDisbaledValue(dto.Settings["Raven/RejectClientsModeEnabled"]);
                        existingResource.rejectClientsMode(isRejectclientsEnabled);
                    }
                });
            }
        }
    }

    private getIndexingDisbaledValue(indexingDisabledString: string) {
        if (indexingDisabledString === undefined || indexingDisabledString == null)
            return false;

        if (indexingDisabledString.toLowerCase() == 'true')
            return true;

        return false;
    }

    private createNewResource(resourceType: logTenantType, resourceName: string, dto: databaseDocumentDto) {
        var newResource = null;

        if (resourceType == logTenantType.Database) {
            newResource = new database(resourceName, true, dto.Disabled);
        } else if (resourceType == logTenantType.Filesystem) {
            newResource = new fileSystem(resourceName, true, dto.Disabled);
        } else if (resourceType == logTenantType.CounterStorage) {
            newResource = new counterStorage(resourceName, true, dto.Disabled);
        }

        return newResource;
    }

    selectResource(rs) {
        rs.activate();

        var locationHash = window.location.hash;
        var isMainPage = locationHash == appUrl.forResources();
        if (isMainPage == false) {
            var updatedUrl = appUrl.forCurrentPage(rs);
            this.navigate(updatedUrl);
        }
    }

    private databasesLoaded(databases: database[]) {
        // we can't use appUrl.getSystemDatabase() here as it isn't loaded yet!
        this.systemDatabase = new database("<system>");
        this.systemDatabase.isSystem = true;
        this.systemDatabase.isVisible(false);
        shell.databases(databases.concat([this.systemDatabase]));
    }

    launchDocEditor(docId?: string, docsList?: pagedList) {
        var editDocUrl = appUrl.forEditDoc(docId, docsList ? docsList.collectionName : null, docsList ? docsList.currentItemIndex() : null, this.activeDatabase());
        this.navigate(editDocUrl);
    }

    loadDatabases(): JQueryPromise<any> {
        var deferred = $.Deferred();

        this.databasesLoadedTask = new getDatabasesCommand()
            .execute()
            .fail(result => this.handleRavenConnectionFailure(result))
            .done((results: database[]) => {
                this.databasesLoaded(results);
                this.fetchStudioConfig();
                this.fetchServerBuildVersion();
                this.fetchClientBuildVersion();
                this.fetchLicenseStatus();
                this.fetchSystemDatabaseAlerts();
                router.activate();
            })
            .always(() => deferred.resolve());

        return deferred;
    }

    loadFileSystems(): JQueryPromise<any> {
        var deferred = $.Deferred();

        new getFileSystemsCommand()
            .execute()
            .done((results: fileSystem[]) => shell.fileSystems(results))
            .always(() => deferred.resolve());

        return deferred;
    }

    loadCounterStorages(): JQueryPromise<any> {
        return $.Deferred().resolve();

        //TODO: uncomment this for counter storages
        /*var deferred = $.Deferred();

        new getCounterStoragesCommand()
            .execute()
            .done((results: counterStorage[]) => shell.counterStorages(results))
            .always(() => deferred.resolve());

        return deferred;*/
    }

    loadServerConfig() {
        var deferred = $.Deferred();

        new getServerConfigsCommand()
            .execute()
            .done((serverConfigs: serverConfigsDto) => {
                shell.isGlobalAdmin(serverConfigs.IsGlobalAdmin);
                shell.canReadWriteSettings(serverConfigs.CanReadWriteSettings);
                shell.canReadSettings(serverConfigs.CanReadSettings);
                shell.canExposeConfigOverTheWire(serverConfigs.CanExposeConfigOverTheWire);
            })
            .always(() => deferred.resolve());

        return deferred;
    }

    connectToRavenServer() {
        var serverConfigsLoadTask: JQueryPromise<any> = this.loadServerConfig();
        var databasesLoadTask: JQueryPromise<any> = this.loadDatabases();
        var fileSystemsLoadTask: JQueryPromise<any> = this.loadFileSystems();
        var counterStoragesLoadTask: JQueryPromise<any> = this.loadCounterStorages();
        $.when(serverConfigsLoadTask, databasesLoadTask, fileSystemsLoadTask, counterStoragesLoadTask)
            .always(() => {
                var locationHash = window.location.hash;
                if (appUrl.getFileSystem()) { //filesystems section
                    this.activateResource(appUrl.getFileSystem(), shell.fileSystems, appUrl.forResources);
                } else if (appUrl.getCounterStorage()) { //counter storages section
                    this.activateResource(appUrl.getCounterStorage(), shell.counterStorages, appUrl.forResources);
                } else if ((locationHash.indexOf(appUrl.forAdminSettings()) == -1)) { //databases section
                    this.activateResource(appUrl.getDatabase(), shell.databases, appUrl.forResources);
                }
            });
    }

    private activateResource(resource: resource, resourceObservableArray: KnockoutObservableArray<any>, url) {
        if (!!resource) {
            var newResource = resourceObservableArray.first(rs => rs.name == resource.name);
            if (newResource != null) {
                newResource.activate();
            } else {
                messagePublisher.reportError("The resource " + resource.name + " doesn't exist!");
                this.navigate(url());
            }
        }
    }

    navigateToResources() {
        shell.disconnectFromResourceChangesApi();

        if (!!this.activeDatabase()) {
            shell.databases().length == 1 ? this.activeDatabase(null) : this.activeDatabase().activate();
        } else if (!!this.activeFilesystem()) {
            this.activeFilesystem().activate();
        } else if (!!this.activeCounterStorage()) {
            this.activeCounterStorage().activate();
        }

        this.navigate(appUrl.forResources());
    }

    fetchStudioConfig() {
        new getDocumentWithMetadataCommand(shell.studioConfigDocumentId, this.systemDatabase)
            .execute()
            .done((doc: documentClass) => {
                appUrl.warnWhenUsingSystemDatabase = doc["WarnWhenUsingSystemDatabase"];
                var envColor = doc["EnvironmentColor"];
                if (envColor != null) {
                    var color = new environmentColor(envColor.Name, envColor.BackgroundColor);
                    shell.selectedEnvironmentColorStatic(color);
                    shell.originalEnviromentColor(color);
                }
            });
    }

    private handleRavenConnectionFailure(result) {
        NProgress.done();

        if (result.status === 401) {
            // Unauthorized might be caused by invalid credentials. 
            // Remove them from both local storage and oauth context.
            apiKeyLocalStorage.clean();
            oauthContext.clean();
        }

        sys.log("Unable to connect to Raven.", result);
        var tryAgain = 'Try again';
        var messageBoxResultPromise = this.confirmationMessage(':-(', "Couldn't connect to Raven. Details in the browser console.", [tryAgain]);
        messageBoxResultPromise.done(() => {
            NProgress.start();
            this.connectToRavenServer();
        });
    }

    dataLoadProgress(splashType?: alertType) {
        if (!splashType) {
            NProgress.configure({ showSpinner: false });
            NProgress.done();
        } else if (splashType == alertType.warning) {
            NProgress.configure({ showSpinner: true });
            NProgress.start();
        } else {
            NProgress.done();
            NProgress.configure({ showSpinner: false });
            //this.showAlert(new alertArgs(alertType.danger, "Load time is too long", "The server might not be responding."));
            $.blockUI({ message: '<div id="longTimeoutMessage"><span> This is taking longer than usual</span><br/><span>(Waiting for server to respond)</span></div>' });
        }
    }

    showAlert(alert: alertArgs) {
        if (alert.displayInRecentErrors && (alert.type === alertType.danger || alert.type === alertType.warning)) {
            this.recordedErrors.unshift(alert);
        }

        var currentAlert = this.currentAlert();
        if (currentAlert) {
            this.queuedAlert = alert;
            this.closeAlertAndShowNext(currentAlert);
        } else {
            this.currentAlert(alert);
            var fadeTime = 2000; // If there are no pending alerts, show it for 2 seconds before fading out.
/*            if (alert.title.indexOf("Changes stream was disconnected.") == 0) {
                fadeTime = 100000000;
            }*/
            if (alert.type === alertType.danger || alert.type === alertType.warning) {
                fadeTime = 5000; // If there are pending alerts, show the error alert for 4 seconds before fading out.
            }
            setTimeout(() => {
                this.closeAlertAndShowNext(alert);
            }, fadeTime);
        }
    }

    closeAlertAndShowNext(alertToClose: alertArgs) {
        var alertElement = $('#' + alertToClose.id);
        if (alertElement.length === 0) {
            return;
        }

        // If the mouse is over the alert, keep it around.
        if (alertElement.is(":hover")) {
            setTimeout(() => this.closeAlertAndShowNext(alertToClose), 1000);
        } else {
            alertElement.alert('close');
        }
    }

    onAlertHidden() {
        this.currentAlert(null);
        var nextAlert = this.queuedAlert;
        if (nextAlert) {
            this.queuedAlert = null;
            this.showAlert(nextAlert);
        }
    }

    newDocument() {
        this.launchDocEditor(null);
    }

    private activateDatabaseWithName(databaseName: string) {
        if (this.databasesLoadedTask) {
            this.databasesLoadedTask.done(() => {
                var matchingDatabase = shell.databases().first(db => db.name == databaseName);
                if (matchingDatabase && this.activeDatabase() !== matchingDatabase) {
                    ko.postbox.publish("ActivateDatabase", matchingDatabase);
                }
            });
        }
    }

    public static disconnectFromResourceChangesApi() {
        if (changesContext.currentResourceChangesApi()) {
            shell.changeSubscriptionArray.forEach((subscripbtion: changeSubscription) => subscripbtion.off());
            shell.changeSubscriptionArray = [];
            changesContext.currentResourceChangesApi().dispose();
            if (changesContext.currentResourceChangesApi().getResourceName() != '<system>') {
                viewModelBase.isConfirmedUsingSystemDatabase = false;
            }
            changesContext.currentResourceChangesApi(null);
        }
    }

    getCurrentActiveFeatureName() {
        if (this.appUrls.isAreaActive('admin')()) {
            return 'Manage Your Server';
        } else {
            return 'Resources';
        }
    }

    getCurrentActiveFeatureHref() {
        if (this.appUrls.isAreaActive('admin')()) {
            return this.appUrls.adminSettings();
        } else {
            return this.appUrls.resources();
        }
    }

    goToDoc(doc: documentMetadataDto) {
        this.goToDocumentSearch("");
        this.navigate(appUrl.forEditDoc(doc['@metadata']['@id'], null, null, this.activeDatabase()));
    }

    getDocCssClass(doc: documentMetadataDto) {
        return collection.getCollectionCssClass(doc['@metadata']['Raven-Entity-Name'], this.activeDatabase());
    }

    fetchServerBuildVersion() {
        new getServerBuildVersionCommand()
            .execute()
            .done((serverBuildResult: serverBuildVersionDto) => {
                this.serverBuildVersion(serverBuildResult);

                var currentBuildVersion = serverBuildResult.BuildVersion;
                if (serverBuildReminder.isReminderNeeded() && currentBuildVersion !== 13) {
                    new getLatestServerBuildVersionCommand(true, 3000, 34999) //pass false as a parameter to get the latest unstable
                        .execute()
                        .done((latestServerBuildResult: latestServerBuildVersionDto) => {
                            if (latestServerBuildResult.LatestBuild > currentBuildVersion) { //
                                var latestBuildReminderViewModel = new latestBuildReminder(latestServerBuildResult);
                                app.showDialog(latestBuildReminderViewModel);
                            }
                        });
                }
            });
    }

    fetchClientBuildVersion() {
        new getClientBuildVersionCommand()
            .execute()
            .done((result: clientBuildVersionDto) => { this.clientBuildVersion(result); });
    }

    fetchLicenseStatus() {
        new getLicenseStatusCommand()
            .execute()
            .done((result: licenseStatusDto) => {
                if (result.Status.contains("AGPL")) {
                    result.Status = "Development Only";
                }
                license.licenseStatus(result);
            });
    }

    fetchGoToDocSearchResults(query: string) {
        if (query.length >= 2) {
            new getDocumentsMetadataByIDPrefixCommand(query, 10, this.activeDatabase())
                .execute()
                .done((results: string[]) => {
                    if (this.goToDocumentSearch() === query) {
                        this.goToDocumentSearchResults(results);
                    }
                });
        } else if (query.length == 0) {
            this.goToDocumentSearchResults.removeAll();
        }
    }

    showApiKeyDialog() {
        var dialog = new enterApiKey();
        return app.showDialog(dialog).then(() => window.location.href = "#resources");
    }

    showErrorsDialog() {
        var errorDetails: recentErrors = new recentErrors(this.recordedErrors);
        app.showDialog(errorDetails);
    }

    uploadStatusChanged(item: uploadItem) {
        var queue: uploadItem[] = uploadQueueHelper.parseUploadQueue(window.localStorage[uploadQueueHelper.localStorageUploadQueueKey + item.filesystem.name], item.filesystem);
        uploadQueueHelper.updateQueueStatus(item.id(), item.status(), queue);
        uploadQueueHelper.updateLocalStorage(queue, item.filesystem);
    }

    showLicenseStatusDialog() {
        require(["viewmodels/licensingStatus"], licensingStatus => {
            var dialog = new licensingStatus(license.licenseStatus());
            app.showDialog(dialog);
        });
    }

    fetchSystemDatabaseAlerts() {
        new getDocumentWithMetadataCommand("Raven/Alerts", this.systemDatabase)
            .execute()
            .done((doc: documentClass) => {
                //
            });
    }

    iconName(rs: resource) {
        if (rs.type === database.type) {
            return "fa fa-database";
        } else if (rs.type === fileSystem.type) {
            return "fa fa-file-image-o";
        } else {
            return "fa fa-calculator";
        }
    }

    logOut() {
        // this call dispatches storage event to all tabs in browser and calls onLogOut inside each.
        apiKeyLocalStorage.clean();
        apiKeyLocalStorage.notifyAboutLogOut();
    }

    onLogOut() {
        window.location.hash = this.appUrls.hasApiKey();
        window.location.reload();
    }

    static getResoucresNames(): string[]
    {
        var arr = shell.databases().map(db => db.name)
            .concat(shell.fileSystems().map(fs => fs.name)).sort();
        var result: string[] = [];
        for (var i = 0; i < arr.length; i++) {
            if ((arr[i].localeCompare(arr[i+1]))!==0) {
                result.push(arr[i]);
            }
        }
        return result;
    }

    private spinnerOptions = {
        lines: 17, // The number of lines to draw
        length: 28, // The length of each line
        width: 14, // The line thickness
        radius: 44, // The radius of the inner circle
        scale: 1, // Scales overall size of the spinner
        corners: 1, // Corner roundness (0..1)
        color: ["#d74c0c", "#CC0000"], // #rgb or #rrggbb or array of colors
        opacity: 0.35, // Opacity of the lines
        rotate: 0, // The rotation offset
        direction: 1, // 1: clockwise, -1: counterclockwise
        speed: 0.8, // Rounds per second
        trail: 60, // Afterglow percentage
        fps: 20, // Frames per second when using setTimeout() as a fallback for CSS
        zIndex: 2e9, // The z-index (defaults to 2000000000)
        className: "spinner", // The CSS class to assign to the spinner
        top: "50%", // Top position relative to parent
        left: "50%", // Left position relative to parent
        shadow: false, // Whether to render a shadow
        hwaccel: false, // Whether to use hardware acceleration
        position: "absolute" // Element positioning
    };
    private spinner = new Spinner(this.spinnerOptions);

    selectNone() {
        this.activateDatabase(null);
        this.activeFilesystem(null);
    }
}

export = shell;

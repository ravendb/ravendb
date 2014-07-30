/// <reference path="../../Scripts/typings/nprogress/nprogress.d.ts" />
/// <reference path="../../Scripts/typings/bootstrap/bootstrap.d.ts" />

import router = require("plugins/router");
import app = require("durandal/app");
import sys = require("durandal/system");

import forge = require("forge/forge_custom.min");
import viewModelBase = require("viewmodels/viewModelBase");
import resource = require("models/resource");
import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");
import counterStorage = require("models/counter/counterStorage");
import document = require("models/document");
import collection = require("models/collection");
import uploadItem = require("models/uploadItem");
import changeSubscription = require("models/changeSubscription");

import appUrl = require("common/appUrl");
import uploadQueueHelper = require("common/uploadQueueHelper");
import deleteDocuments = require("viewmodels/deleteDocuments");
import dialogResult = require("common/dialogResult");
import alertArgs = require("common/alertArgs");
import alertType = require("common/alertType");
import pagedList = require("common/pagedList");
import dynamicHeightBindingHandler = require("common/dynamicHeightBindingHandler");
import autoCompleteBindingHandler = require("common/autoCompleteBindingHandler");
import changesApi = require("common/changesApi");
import oauthContext = require("common/oauthContext");
import messagePublisher = require("common/messagePublisher");

import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getDatabasesCommand = require("commands/getDatabasesCommand");
import getServerBuildVersionCommand = require("commands/getServerBuildVersionCommand");
import getClientBuildVersionCommand = require("commands/getClientBuildVersionCommand");
import getLicenseStatusCommand = require("commands/getLicenseStatusCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/getDocumentsMetadataByIDPrefixCommand");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import getFileSystemsCommand = require("commands/filesystem/getFileSystemsCommand");
import getFileSystemStatsCommand = require("commands/filesystem/getFileSystemStatsCommand");
import getCounterStoragesCommand = require("commands/counter/getCounterStoragesCommand");
import getSystemDocumentCommand = require("commands/getSystemDocumentCommand");

import recentErrors = require("viewmodels/recentErrors");
import enterApiKey = require("viewmodels/enterApiKey");
import extensions = require("common/extensions");

class shell extends viewModelBase {
    private router = router;
    static databases = ko.observableArray<database>();
    listedDatabases: KnockoutComputed<database[]>;
    systemDatabase: database;
    isActiveDatabaseDisabled: KnockoutComputed<boolean>;
    databasesLoadedTask: JQueryPromise<any>;
    goToDocumentSearch = ko.observable<string>();
    goToDocumentSearchResults = ko.observableArray<string>();

    static fileSystems = ko.observableArray<filesystem>();
    listedFileSystems: KnockoutComputed<filesystem[]>;
    isActiveFileSystemDisabled: KnockoutComputed<boolean>;
    canShowFileSystemNavbar = ko.computed(() => shell.fileSystems().length > 0 && this.appUrls.isAreaActive('filesystems')());

    static counterStorages = ko.observableArray<counterStorage>();
    listedCounterStorages: KnockoutComputed<counterStorage[]>;
    isCounterStorageDisabled: KnockoutComputed<boolean>;
    canShowCountersNavbar = ko.computed(() => shell.counterStorages().length > 0 && this.appUrls.isAreaActive('counterstorages')());

    currentConnectedResource: resource;
    currentAlert = ko.observable<alertArgs>();
    queuedAlert: alertArgs;
    serverBuildVersion = ko.observable<serverBuildVersionDto>();
    clientBuildVersion = ko.observable<clientBuildVersionDto>();
    static licenseStatus = ko.observable<licenseStatusDto>();
    localLicenseStatus: KnockoutObservable<licenseStatusDto> = shell.licenseStatus;
    windowHeightObservable: KnockoutObservable<number>;
    appUrls: computedAppUrls;
    recordedErrors = ko.observableArray<alertArgs>();
    newIndexUrl = appUrl.forCurrentDatabase().newIndex;
    newTransformerUrl = appUrl.forCurrentDatabase().newTransformer;
    currentRawUrl = ko.observable<string>("");
    rawUrlIsVisible = ko.computed(() => this.currentRawUrl().length > 0);
    activeArea = ko.observable<string>("Databases");

    private globalChangesApi: changesApi;
    static currentResourceChangesApi = ko.observable<changesApi>(null);
    private changeSubscriptionArray: changeSubscription[];

    constructor() {
        super();

		extensions.install();
        oauthContext.enterApiKeyTask = this.setupApiKey();
        oauthContext.enterApiKeyTask.done(() => this.globalChangesApi = new changesApi(appUrl.getSystemDatabase()));

        ko.postbox.subscribe("Alert", (alert: alertArgs) => this.showAlert(alert));
        ko.postbox.subscribe("LoadProgress", (alertType?: alertType) => this.dataLoadProgress(alertType));
        ko.postbox.subscribe("ActivateDatabaseWithName", (databaseName: string) => this.activateDatabaseWithName(databaseName));
        ko.postbox.subscribe("SetRawJSONUrl", (jsonUrl: string) => this.currentRawUrl(jsonUrl));
        ko.postbox.subscribe("ActivateDatabase", (db: database) => { this.updateDbChangesApi(db); shell.fetchDbStats(db); });
        ko.postbox.subscribe("ActivateFilesystem", (fs: filesystem) => { this.updateFsChangesApi(fs); shell.fetchFsStats(fs); });
        ko.postbox.subscribe("ActivateCounterStorage", (cs: counterStorage) => { this.updateCsChangesApi(cs); shell.fetchCsStats(cs); });
        ko.postbox.subscribe("UploadFileStatusChanged", (uploadStatus: uploadItem) => this.uploadStatusChanged(uploadStatus));
        ko.postbox.subscribe("ChangesApiReconnected", (rs: resource) => this.reloadDataAfterReconnection(rs));

        this.currentConnectedResource = appUrl.getSystemDatabase();
        this.appUrls = appUrl.forCurrentDatabase();

        this.goToDocumentSearch.throttle(250).subscribe(search => this.fetchGoToDocSearchResults(search));
        dynamicHeightBindingHandler.install();
        autoCompleteBindingHandler.install();

        this.isActiveDatabaseDisabled = ko.computed(() => {
            var activeDb = this.activeDatabase();
            return !!activeDb ? activeDb.disabled() : false;
        });

        this.isActiveFileSystemDisabled = ko.computed(() => {
            var activeFs = this.activeFilesystem();
            return !!activeFs ? activeFs.disabled() : false;
        });

        this.isCounterStorageDisabled = ko.computed(() => {
            var activeCs = this.activeCounterStorage();
            return !!activeCs ? activeCs.disabled() : false;
        });

        this.listedDatabases = ko.computed(() => {
            var currentDatabase = this.activeDatabase();
            if (!!currentDatabase) {
                return shell.databases().filter(database => database.name != currentDatabase.name);
            }
            return shell.databases();
        });

        this.listedFileSystems = ko.computed(() => {
            var currentFileSystem = this.activeFilesystem();
            if (!!currentFileSystem) {
                return shell.fileSystems().filter(fileSystem => fileSystem.name != currentFileSystem.name);
            }
            return shell.fileSystems();
        });

        this.listedCounterStorages = ko.computed(() => {
            var currentCounterStorage = this.activeCounterStorage();
            if (!!currentCounterStorage) {
                return shell.counterStorages().filter(counterStorage => counterStorage.name != currentCounterStorage.name);
            }
            return shell.counterStorages();
        });
    }

    // Override canActivate: we can always load this page, regardless of any system db prompt.
    canActivate(args: any): any {
        return true;
    }

    activate(args: any) {
        super.activate(args);

        oauthContext.enterApiKeyTask.done(() => this.connectToRavenServer());

        NProgress.set(.7);
        router.map([
            { route: ['', 'databases'], title: 'Databases', moduleId: 'viewmodels/databases', nav: true, hash: this.appUrls.databasesManagement },
            { route: 'databases/documents', title: 'Documents', moduleId: 'viewmodels/documents', nav: true, hash: this.appUrls.documents },
            { route: 'databases/conflicts', title: 'Conflicts', moduleId: 'viewmodels/conflicts', nav: true, hash: this.appUrls.conflicts },
            { route: 'databases/patch', title: 'Patch', moduleId: 'viewmodels/patch', nav: true, hash: this.appUrls.patch },
            { route: 'databases/indexes*details', title: 'Indexes', moduleId: 'viewmodels/indexesShell', nav: true, hash: this.appUrls.indexes },
            { route: 'databases/transformers*details', title: 'Transformers', moduleId: 'viewmodels/transformersShell', nav: false, hash: this.appUrls.transformers },
            { route: 'databases/query*details', title: 'Query', moduleId: 'viewmodels/queryShell', nav: true, hash: this.appUrls.query(null) },
            { route: 'databases/tasks*details', title: 'Tasks', moduleId: 'viewmodels/tasks', nav: true, hash: this.appUrls.tasks, },
            { route: 'databases/settings*details', title: 'Settings', moduleId: 'viewmodels/settings', nav: true, hash: this.appUrls.settings },
            { route: 'databases/status*details', title: 'Status', moduleId: 'viewmodels/status', nav: true, hash: this.appUrls.status },
            { route: 'databases/edit', title: 'Edit Document', moduleId: 'viewmodels/editDocument', nav: false },
            { route: ['', 'filesystems'], title: 'File Systems', moduleId: 'viewmodels/filesystem/filesystems', nav: true, hash: this.appUrls.filesystemsManagement },
            { route: 'filesystems/files', title: 'Files', moduleId: 'viewmodels/filesystem/filesystemFiles', nav: true, hash: this.appUrls.filesystemFiles },
            { route: 'filesystems/search', title: 'Search', moduleId: 'viewmodels/filesystem/search', nav: true, hash: this.appUrls.filesystemSearch },
            { route: 'filesystems/synchronization*details', title: 'Synchronization', moduleId: 'viewmodels/filesystem/synchronization', nav: true, hash: this.appUrls.filesystemSynchronization },
            { route: 'filesystems/status*details', title: 'Status', moduleId: 'viewmodels/filesystem/status', nav: true, hash: this.appUrls.filesystemStatus },
            { route: 'filesystems/configuration', title: 'Configuration', moduleId: 'viewmodels/filesystem/configuration', nav: true, hash: this.appUrls.filesystemConfiguration },
            { route: 'filesystems/upload', title: 'Upload File', moduleId: 'viewmodels/filesystem/filesystemUploadFile', nav: false },
            { route: 'filesystems/edit', title: 'Upload File', moduleId: 'viewmodels/filesystem/filesystemEditFile', nav: false },
            { route: ['', 'counterstorages'], title: 'Counter Storages', moduleId: 'viewmodels/counter/counterStorages', nav: true, hash: this.appUrls.couterStorages },
            { route: 'counterstorages/counters', title: 'counters', moduleId: 'viewmodels/counter/counterStoragecounters', nav: true, hash: this.appUrls.counterStorageCounters },
            { route: 'counterstorages/replication', title: 'replication', moduleId: 'viewmodels/counter/counterStorageReplication', nav: true, hash: this.appUrls.counterStorageReplication },
            { route: 'counterstorages/stats', title: 'stats', moduleId: 'viewmodels/counter/counterStorageStats', nav: true, hash: this.appUrls.counterStorageStats },
            { route: 'counterstorages/configuration', title: 'configuration', moduleId: 'viewmodels/counter/counterStorageConfiguration', nav: true, hash: this.appUrls.counterStorageConfiguration }
        ]).buildNavigationModel();

        // Show progress whenever we navigate.
        router.isNavigating.subscribe(isNavigating => this.showNavigationProgress(isNavigating));

        appUrl.mapUnknownRoutes(router);

        window.addEventListener("beforeunload", () => {
            this.cleanupNotifications();
            this.globalChangesApi.dispose();
            this.disconnectFromResourceChangesApi();
        });
    }

    // Called by Durandal when shell.html has been put into the DOM.
    attached() {
        // The view must be attached to the DOM before we can hook up keyboard shortcuts.
        jwerty.key("ctrl+alt+n", e=> {
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

    setupApiKey() {
        // try to find api key as studio hash parameter
        var hash = window.location.hash;
        if (hash === "#has-api-key") {
            return this.showApiKeyDialog();
        } else if (hash.match(/#api-key/g)) {
            var match = /#api-key=(.*)/.exec(hash);
            if (match && match.length == 2) {
                oauthContext.apiKey(match[1]);
                window.location.hash = "#";
            }
        }
        return $.Deferred().resolve();
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
            $('.tooltip.fade').remove(); // Fix for tooltips that are shown right before navigation - they get stuck and remain in the UI.
        }
    }

    private reloadDataAfterReconnection(rs: resource) {
        if (rs.name === "<system>") {
            this.fetchStudioConfig();
            this.fetchServerBuildVersion();
            this.fetchClientBuildVersion();
            this.fetchLicenseStatus();

            new getDatabasesCommand()
                .execute()
                .done((results: database[]) => this.updateResourceObservableArray(shell.databases, results, this.activeDatabase));
            new getFileSystemsCommand()
                .execute()
                .done((results: filesystem[]) => this.updateResourceObservableArray(shell.fileSystems, results, this.activeFilesystem));
            new getCounterStoragesCommand()
                .execute()
                .done((results: counterStorage[]) => this.updateResourceObservableArray(shell.counterStorages, results, this.activeCounterStorage));
        }
    }

    private updateResourceObservableArray(resourceObservableArray: KnockoutObservableArray<any>,
        recievedResourceArray: Array<any>, activeResourceObservable: any) {

        var deletedResources = [];

        resourceObservableArray().forEach((rs: resource) => {
            var existingResource = recievedResourceArray.first((recievedResource: resource) => recievedResource.name == rs.name || rs.name == "<system>");
            if (existingResource == null) {
                deletedResources.push(rs);
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

        if (deletedResources.length > 0) {
            this.selectNewActiveResourceIfNeeded(deletedResources[0], resourceObservableArray, activeResourceObservable);
        }
    }

    private selectNewActiveResourceIfNeeded(resourceToDelete: resource, resourceObservableArray: KnockoutObservableArray<any>, activeResourceObservable: any) {
        var isSameInstanceAsCurrent = resourceToDelete instanceof database && this.currentConnectedResource instanceof database ||
            resourceToDelete instanceof filesystem && this.currentConnectedResource instanceof filesystem ||
            resourceToDelete instanceof counterStorage && this.currentConnectedResource instanceof counterStorage;

        if (isSameInstanceAsCurrent && resourceObservableArray.contains(activeResourceObservable()) == false) {
            if (resourceObservableArray().length > 0) {
                this.selectResource(resourceObservableArray().first());
            } else if (resourceObservableArray().length == 0) {
                //if we are in file systems or counter storages page
                this.disconnectFromResourceChangesApi();
                var url = (activeResourceObservable() instanceof filesystem) ? "#filesystems" : "#counterstorages";
                activeResourceObservable(null);
                this.navigate(url);
            }
        }
    }

    createNotifications(): Array<changeSubscription> {
        return [
            this.globalChangesApi.watchDocsStartingWith("Raven/Databases/", (e) => this.changesApiFiredForResource(e, shell.databases, this.activeDatabase)),
            this.globalChangesApi.watchDocsStartingWith("Raven/FileSystems/", (e) => this.changesApiFiredForResource(e, shell.fileSystems, this.activeFilesystem)),
            this.globalChangesApi.watchDocsStartingWith("Raven/Counters/", (e) => this.changesApiFiredForResource(e, shell.counterStorages, this.activeCounterStorage))
        ];
    }

    private changesApiFiredForResource(e: documentChangeNotificationDto,
        resourceObservableArray: KnockoutObservableArray<any>, activeResourceObservable: any) {

        if (!!e.Id && (e.Type === documentChangeType.Delete || e.Type === documentChangeType.Put)) {
            var receivedResourceName = e.Id.slice(e.Id.lastIndexOf('/') + 1);
            
            if (e.Type === documentChangeType.Delete) {
                var resourceToDelete = resourceObservableArray.first((rs: resource) => rs.name == receivedResourceName);
                if (!!resourceToDelete) {
                    resourceObservableArray.remove(resourceToDelete);

                    this.selectNewActiveResourceIfNeeded(resourceToDelete, resourceObservableArray, activeResourceObservable);
                }
            } else { // e.Type === documentChangeType.Put
                var getSystemDocumentTask = new getSystemDocumentCommand(e.Id).execute();
                getSystemDocumentTask.done((dto: databaseDocumentDto) => {
                    var existingResource = resourceObservableArray.first((rs: resource) => rs.name == receivedResourceName);

                    if (existingResource == null) { // new database
                        existingResource = this.createNewResource(activeResourceObservable(), receivedResourceName, dto);
                        resourceObservableArray.unshift(existingResource);
                    } else {
                        if (existingResource.disabled() != dto.Disabled) { //disable status change
                            existingResource.disabled(dto.Disabled);
                            if (dto.Disabled == false && this.currentConnectedResource.name == receivedResourceName) {
                                existingResource.activate();
                            }
                        }
                    }

                    if (activeResourceObservable() instanceof database) { //for databases, bundle change
                        var bundles = !!dto.Settings["Raven/ActiveBundles"] ? dto.Settings["Raven/ActiveBundles"].split(";") : [];
                        existingResource.activeBundles(bundles);
                    }
                });
            }
        }
    }

    private createNewResource(activeResource: any, resourceName: string, dto: databaseDocumentDto) {
        var newResource = null;

        if (activeResource instanceof database) {
            newResource = new database(resourceName, dto.Disabled);
        }
        else if (activeResource instanceof filesystem) {
            newResource = new filesystem(resourceName, dto.Disabled);
        }
        else if (activeResource instanceof counterStorage) {
            newResource = new counterStorage(resourceName, dto.Disabled);
        }

        return newResource;
    }

    selectResource(rs) {
        rs.activate();

        var updatedUrl = appUrl.forCurrentPage(rs);
        this.navigate(updatedUrl);
    }

    private databasesLoaded(databases: database[]) {
        this.systemDatabase = new database("<system>");
        this.systemDatabase.isSystem = true;
        this.systemDatabase.isVisible(false);
        shell.databases(databases.concat([this.systemDatabase]));
    }

    private fileSystemsLoaded(fileSystems: filesystem[]) {
        shell.fileSystems(fileSystems);
    }

    private counterStoragesLoaded(results: counterStorage[]) {
        shell.counterStorages(results);
    }

    launchDocEditor(docId?: string, docsList?: pagedList) {
        var editDocUrl = appUrl.forEditDoc(docId, docsList ? docsList.collectionName : null, docsList ? docsList.currentItemIndex() : null, this.activeDatabase());
        this.navigate(editDocUrl);
    }

    connectToRavenServer() {
        this.databasesLoadedTask = new getDatabasesCommand()
            .execute()
            .fail(result => this.handleRavenConnectionFailure(result))
            .done((results: database[]) => {
                this.databasesLoaded(results);
                this.fetchStudioConfig();
                this.fetchServerBuildVersion();
                this.fetchClientBuildVersion();
                this.fetchLicenseStatus();
                router.activate();
            });

        var fileSystemsLoadedTask: JQueryPromise<any> = new getFileSystemsCommand()
            .execute()
            .done((results: filesystem[]) => this.fileSystemsLoaded(results));

        var counterStoragesLoadedTask: JQueryPromise < any> = new getCounterStoragesCommand()
            .execute()
            .done((results: counterStorage[]) => this.counterStoragesLoaded(results));

        $.when(this.databasesLoadedTask, fileSystemsLoadedTask, counterStoragesLoadedTask)
            .done(() => {
                var locationHash = window.location.hash;

                if (locationHash.indexOf("#filesystems") == 0) {

                    this.activateResource(appUrl.getFileSystem(), shell.fileSystems);
                }
                else if (locationHash.indexOf("#counterstorages") == 0) {
                    this.activateResource(appUrl.getCounterStorage(), shell.counterStorages);
                } else {
                    this.activateResource(appUrl.getDatabase(), shell.databases);
                }
            });
    }

    private activateResource(resource: resource, resourceObservableArray: KnockoutObservableArray<any>) {
        var arrayLength = resourceObservableArray().length;

        if (arrayLength > 0) {
            var newResource;

            if (resource != null && (newResource = resourceObservableArray.first(rs => rs.name == resource.name)) != null) {
                newResource.activate();
            } else {
                resourceObservableArray.first().activate();
            }
        }
    }

    navigateToResourceGroup(resourceHash) {
        this.disconnectFromResourceChangesApi();

        if (resourceHash == this.appUrls.databases()) {
            this.activateResource(appUrl.getDatabase(), shell.databases);
        }
        else if (resourceHash == this.appUrls.filesystems()) {
            this.activateResource(appUrl.getFileSystem(), shell.fileSystems);
        } else {
            this.activateResource(appUrl.getCounterStorage(), shell.counterStorages);
        }

        this.navigate(resourceHash);
    }

    fetchStudioConfig() {
        new getDocumentWithMetadataCommand("Raven/StudioConfig", this.systemDatabase)
            .execute()
            .done((doc: document) => {
                appUrl.warnWhenUsingSystemDatabase = doc["WarnWhenUsingSystemDatabase"];
            });
    }

    private handleRavenConnectionFailure(result) {
        NProgress.done();
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
            this.showAlert(new alertArgs(alertType.danger, "Database load time is too long", "The database server might not be responding."));
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
            if (alert.title.indexOf("Changes stream was disconnected.") == 0) {
                fadeTime = 100000000;
            }
            else if (alert.type === alertType.danger || alert.type === alertType.warning) {
                fadeTime = 4000; // If there are pending alerts, show the error alert for 4 seconds before fading out.
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

    private updateDbChangesApi(db: database) {
        if (this.currentConnectedResource.name != db.name || this.currentConnectedResource.name == db.name && db.disabled()) {
            // disconnect from the current database changes api and set the current connected database
            this.disconnectFromResourceChangesApi();
            this.currentConnectedResource = db;
        }

        if (!db.disabled() && (shell.currentResourceChangesApi() == null || !this.appUrls.isAreaActive('databases')()) ||
                db.name == "<system>" && this.currentConnectedResource.name == db.name) {
            // connect to changes api, if it's not disabled and the changes api isn't already connected
            shell.currentResourceChangesApi(new changesApi(db, 5000));
            this.changeSubscriptionArray = [
                shell.currentResourceChangesApi().watchAllDocs(() => shell.fetchDbStats(db)),
                shell.currentResourceChangesApi().watchAllIndexes(() => shell.fetchDbStats(db)),
                shell.currentResourceChangesApi().watchBulks(() => shell.fetchDbStats(db))
            ];
        }
    }

    private updateFsChangesApi(fs: filesystem) {
        if (this.currentConnectedResource.name != fs.name || this.currentConnectedResource.name == fs.name && fs.disabled()) {
            // disconnect from the current filesystem changes api and set the current connected filesystem
            this.disconnectFromResourceChangesApi();
            this.currentConnectedResource = fs;
        }

        if (!fs.disabled() && (shell.currentResourceChangesApi() == null || !this.appUrls.isAreaActive('filesystems')())) {
            // connect to changes api, if it's not disabled and the changes api isn't already connected
            shell.currentResourceChangesApi(new changesApi(fs, 5000));
            this.changeSubscriptionArray = [
                shell.currentResourceChangesApi().watchFsFolders("", () => shell.fetchFsStats(fs))
            ];
        }
    }

    private updateCsChangesApi(cs: counterStorage) {
        if (this.currentConnectedResource.name != cs.name || this.currentConnectedResource.name == cs.name && cs.disabled()) {
            // disconnect from the current filesystem changes api and set the current connected filesystem
            this.disconnectFromResourceChangesApi();
            this.currentConnectedResource = cs;
        }

        if (!cs.disabled() && (shell.currentResourceChangesApi() == null || !this.appUrls.isAreaActive('counterstorages')())) {
            // connect to changes api, if it's not disabled and the changes api isn't already connected
            shell.currentResourceChangesApi(new changesApi(cs, 5000));
            this.changeSubscriptionArray = [
                //TODO: enable changes api for counter storages, server side
            ];
        }
    }

    private disconnectFromResourceChangesApi() {
        if (shell.currentResourceChangesApi()) {
            this.changeSubscriptionArray.forEach((subscripbtion: changeSubscription) => subscripbtion.off());
            this.changeSubscriptionArray = [];
            shell.currentResourceChangesApi().dispose();
            shell.currentResourceChangesApi(null);
        }
    }
    
    static fetchDbStats(db: database) {
        if (db && !db.disabled()) {
            new getDatabaseStatsCommand(db)
                .execute()
                .done(result => db.statistics(result));
        }
    }

    static fetchFsStats(fs: filesystem) {
        if (fs && !fs.disabled()) {
            new getFileSystemStatsCommand(fs)
                .execute()
                .done(result=> fs.statistics(result));
        }
    }

    static fetchCsStats(cs: counterStorage) {
        if (cs && !cs.disabled()) {
            //TODO: implememnt fetching of counter storage stats
/*            new getCounterStorageStatsCommand(cs)
                .execute()
                .done(result=> cs.statistics(result));*/
        }
    }

    getCurrentActiveFeatureName() {
        if (this.appUrls.isAreaActive('filesystems')() === true) {
            return 'File Systems';
        } else if (this.appUrls.isAreaActive('counterstorages')() === true) {
            return 'Counter Storages';
        } else {
            return 'Databases';
        }
    }

    getCurrentActiveFeatureHref() {
        if (this.appUrls.isAreaActive('filesystems')() === true) {
            return this.appUrls.filesystems();
        } else if (this.appUrls.isAreaActive('counterstorages')() === true) {
            return this.appUrls.counterStorageManagement();
        } else {
            return this.appUrls.databases();
        }
    }

    goToDoc(doc: documentMetadataDto) {
        this.goToDocumentSearch("");
        this.navigate(appUrl.forEditDoc(doc['@metadata']['@id'], null, null, this.activeDatabase()));
    }

    getDocCssClass(doc: documentMetadataDto) {
        return collection.getCollectionCssClass(doc['@metadata']['Raven-Entity-Name']);
    }

    fetchServerBuildVersion() {
        new getServerBuildVersionCommand()
            .execute()
            .done((result: serverBuildVersionDto) => { this.serverBuildVersion(result); });
    }

    fetchClientBuildVersion() {
        new getClientBuildVersionCommand()
            .execute()
            .done((result: clientBuildVersionDto) => { this.clientBuildVersion(result); });
    }

    fetchLicenseStatus() {
        new getLicenseStatusCommand()
            .execute()
            .done((result: licenseStatusDto) => shell.licenseStatus(result));
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
        return app.showDialog(dialog).then(() => window.location.href = "#");
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
            var dialog = new licensingStatus(shell.licenseStatus());
            app.showDialog(dialog);
        });
    }
}

export = shell;

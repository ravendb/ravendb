/// <reference path="../../Scripts/typings/nprogress/nprogress.d.ts" />
/// <reference path="../../Scripts/typings/bootstrap/bootstrap.d.ts" />

import router = require("plugins/router");
import app = require("durandal/app");
import sys = require("durandal/system");

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

import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getDatabasesCommand = require("commands/getDatabasesCommand");
import getServerBuildVersionCommand = require("commands/getServerBuildVersionCommand");
import getClientBuildVersionCommand = require("commands/getClientBuildVersionCommand");
import getLicenseStatusCommand = require("commands/getLicenseStatusCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/getDocumentsMetadataByIDPrefixCommand");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import getFilesystemsCommand = require("commands/filesystem/getFilesystemsCommand");
import getFilesystemStatsCommand = require("commands/filesystem/getFilesystemStatsCommand");
import getCounterStoragesCommand = require("commands/counter/getCounterStoragesCommand");

class shell extends viewModelBase {
    private router = router;

    databases = ko.observableArray<database>();
    listedDatabases: KnockoutComputed<database[]>;
    currentConnectedDatabase: database;
    systemDb: database;
    isDatabaseDisabled: KnockoutComputed<boolean>;
    databasesLoadedTask: JQueryPromise<any>;
    goToDocumentSearch = ko.observable<string>();
    goToDocumentSearchResults = ko.observableArray<string>();

    filesystems = ko.observableArray<filesystem>();
    listedFileSystems: KnockoutComputed<filesystem[]>;
    fileSystemsLoadedTask: JQueryPromise<any>;
    canShowFileSystemNavbar = ko.computed(() => this.filesystems().length > 0 && this.appUrls.isAreaActive('filesystems')());

    counterStorages = ko.observableArray<counterStorage>();
    listedCounterStorages: KnockoutComputed<counterStorage[]>;
    counterStoragesLoadedTask: JQueryPromise<any>;
    canShowCountersNavbar = ko.computed(() => this.counterStorages().length > 0 && this.appUrls.isAreaActive('counterstorages')());

    currentAlert = ko.observable<alertArgs>();
    queuedAlert: alertArgs;
    serverBuildVersion = ko.observable<serverBuildVersionDto>();
    clientBuildVersion = ko.observable<clientBuildVersionDto>();
    licenseStatus = ko.observable<licenseStatusDto>();
    windowHeightObservable: KnockoutObservable<number>;
    appUrls: computedAppUrls;
    recordedErrors = ko.observableArray<alertArgs>();
    newIndexUrl = appUrl.forCurrentDatabase().newIndex;
    newTransformerUrl = appUrl.forCurrentDatabase().newTransformer;
    currentRawUrl = ko.observable<string>("");
    rawUrlIsVisible = ko.computed(() => this.currentRawUrl().length > 0);
    activeArea = ko.observable<string>("Databases");

    static globalChangesApi: changesApi;
    static currentDbChangesApi = ko.observable<changesApi>(null);

    constructor() {
        super();
        ko.postbox.subscribe("Alert", (alert: alertArgs) => this.showAlert(alert));
        ko.postbox.subscribe("LoadProgress", (alertType?: alertType) => this.dataLoadProgress(alertType));
        ko.postbox.subscribe("ActivateDatabaseWithName", (databaseName: string) => this.activateDatabaseWithName(databaseName));
        ko.postbox.subscribe("ActivateFilesystemWithName", (filesystemName: string) => this.activateFilesystemWithName(filesystemName));
        ko.postbox.subscribe("ActivateCounterStorageWithName", (filesystemName: string) => this.activateFilesystemWithName(filesystemName));
        ko.postbox.subscribe("SetRawJSONUrl", (jsonUrl: string) => this.currentRawUrl(jsonUrl));
        ko.postbox.subscribe("ActivateDatabase", (db: database) => { this.updateChangesApi(db); this.fetchDbStats(db, true); });
        ko.postbox.subscribe("ActivateFilesystem", (fs: filesystem) => { this.fetchFSStats(fs, true); });
        ko.postbox.subscribe("UploadFileStatusChanged", (uploadStatus: uploadItem) => this.uploadStatusChanged(uploadStatus));

        this.systemDb = appUrl.getSystemDatabase();
        this.currentConnectedDatabase = this.systemDb;
        this.appUrls = appUrl.forCurrentDatabase();

        this.goToDocumentSearch.throttle(250).subscribe(search => this.fetchGoToDocSearchResults(search));
        dynamicHeightBindingHandler.install();
        autoCompleteBindingHandler.install();
        shell.globalChangesApi = new changesApi(appUrl.getSystemDatabase());

        this.isDatabaseDisabled = ko.computed(() => {
            var activeDb = this.activeDatabase();
            return !!activeDb ? activeDb.disabled() : false;
        });

        this.listedDatabases = ko.computed(() => {
            var currentDatabase = this.activeDatabase();
            if (!!currentDatabase) {
                return this.databases().filter(database => database.name != currentDatabase.name);
            }
            return this.databases();
        }, this);

        this.listedFileSystems = ko.computed(() => {
            var currentFileSystem = this.activeFilesystem();
            if (!!currentFileSystem) {
                return this.filesystems().filter(fileSystem => fileSystem.name != currentFileSystem.name);
            }
            return this.filesystems();
        }, this);

        this.listedCounterStorages = ko.computed(() => {
            var currentCounterStorage = this.activeCounterStorage();
            if (!!currentCounterStorage) {
                return this.counterStorages().filter(counterStorage => counterStorage.name != currentCounterStorage.name);
            }
            return this.counterStorages();
        }, this);
    }

    activate(args: any) {
        super.activate(args);

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
        this.connectToRavenServer();
    }

    // Called by Durandal when shell.html has been put into the DOM.
    attached() {
        // The view must be attached to the DOM before we can hook up keyboard shortcuts.
        jwerty.key("ctrl+alt+n", e=> {
            e.preventDefault();
            this.newDocument();
        });

        $("body").tooltip({
            delay: { show: 1000, hide: 100 },
            container: 'body',
            selector: '.use-bootstrap-tooltip',
            trigger: 'hover'
        });

        router.activeInstruction.subscribe(val => {
            if (val.config.route.split('/').length == 1) //if it's a root navigation item.
                this.activeArea(val.config.title);
        });
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

    createNotifications(): Array<changeSubscription> {
        return [
            shell.globalChangesApi.watchDocsStartingWith("Raven/Databases/", (e) => this.changesApiFiredForDatabases(e)),
            shell.globalChangesApi.watchDocsStartingWith("Raven/FileSystems/", (e) => this.changesApiFiredForFileSystems(e)),
            shell.globalChangesApi.watchDocsStartingWith("Raven/Counters/", (e) => this.changesApiFiredForCounterStorages(e))
        ];
    }

    private changesApiFiredForDatabases(e: documentChangeNotificationDto) {
        if (!!e.Id && (e.Type === documentChangeType.Delete ||
            e.Type === documentChangeType.SystemResourceEnabled || e.Type === documentChangeType.SystemResourceDisabled)) {
            var receivedDatabaseName = e.Id.slice(e.Id.lastIndexOf('/') + 1);

            if (e.Type === documentChangeType.Delete) {
                this.onDatabaseDeleted(receivedDatabaseName);
            } else {
                var existingDatabase = this.databases.first((db: database) => db.name == receivedDatabaseName);
                var receivedDatabaseDisabled: boolean = (e.Type === documentChangeType.SystemResourceDisabled);

                if (existingDatabase == null) {
                    var newDatabase = new database(receivedDatabaseName, receivedDatabaseDisabled);
                    this.databases.unshift(newDatabase);
                }
                else if (existingDatabase.disabled() != receivedDatabaseDisabled) {
                    existingDatabase.disabled(receivedDatabaseDisabled);
                }
            }
        }
    }

    private onDatabaseDeleted(databaseName: string) {
        var databaseInList = this.databases.first((db: database) => db.name == databaseName);
        if (!!databaseInList) {
            this.databases.remove(databaseInList);

            if (this.databases.contains(this.activeDatabase()) === false) {
                this.selectDatabase(this.databases().first());
            }
        }
    }

    private changesApiFiredForFileSystems(e: documentChangeNotificationDto) {
        if (!!e.Id && (e.Type === documentChangeType.Delete ||
            e.Type === documentChangeType.SystemResourceEnabled || e.Type === documentChangeType.SystemResourceDisabled)) {
            var receivedFileSystemName = e.Id.slice(e.Id.lastIndexOf('/') + 1);

            if (e.Type === documentChangeType.Delete) {
                this.onFileSystemDeleted(receivedFileSystemName);
            } else {
                var existingFileSystem = this.filesystems.first((fs: filesystem) => fs.name == receivedFileSystemName);
                var receivedFileSystemDisabled: boolean = (e.Type === documentChangeType.SystemResourceDisabled);

                if (existingFileSystem == null) {
                    var fileSystem = new filesystem(receivedFileSystemName, receivedFileSystemDisabled);
                    this.filesystems.unshift(fileSystem);
                }
                else if (existingFileSystem.disabled() != receivedFileSystemDisabled) {
                    existingFileSystem.disabled(receivedFileSystemDisabled);
                }
            }
        }
    }

    private onFileSystemDeleted(fileSystemName: string) {
        var fileSystemInList = this.filesystems.first((fs: filesystem) => fs.name == fileSystemName);
        if (!!fileSystemInList) {
            this.filesystems.remove(fileSystemInList);
        }

        if ((this.filesystems().length > 0) && (this.filesystems.contains(this.activeFilesystem()) === false)) {
            this.selectFileSystem(this.filesystems().first());
        }
    }

    private changesApiFiredForCounterStorages(e: documentChangeNotificationDto) {
        if (!!e.Id && (e.Type === documentChangeType.Delete ||
            e.Type === documentChangeType.SystemResourceEnabled || e.Type === documentChangeType.SystemResourceDisabled)) {
            var receivedCounterStoragesName = e.Id.slice(e.Id.lastIndexOf('/') + 1);

            if (e.Type === documentChangeType.Delete) {
                this.onCounterStorageDeleted(receivedCounterStoragesName);
            } else {
                var existingCounterStorage = this.counterStorages.first((cs: counterStorage) => cs.name == receivedCounterStoragesName);
                var receivedCounterStorageDisabled: boolean = (e.Type === documentChangeType.SystemResourceDisabled);

                if (existingCounterStorage == null) {
                    var newCounterStorage = new counterStorage(receivedCounterStoragesName, receivedCounterStorageDisabled);
                    this.counterStorages.unshift(newCounterStorage);
                }
                else if (existingCounterStorage.disabled() != receivedCounterStorageDisabled) {
                    existingCounterStorage.disabled(receivedCounterStorageDisabled);
                }
            }
        }
    }

    private onCounterStorageDeleted(counterStorageName: string) {
        var counterStoragesInList = this.counterStorages.first((cs: counterStorage) => cs.name == counterStorageName);
        if (!!counterStoragesInList) {
            this.counterStorages.remove(counterStoragesInList);

            if ((this.counterStorages().length > 0) && (this.counterStorages.contains(this.activeCounterStorage()) === false)) {
                this.selectCounterStorage(this.counterStorages().first());
            }
        }
    }

    databasesLoaded(databases) {
        var systemDatabase = new database("<system>");
        systemDatabase.isSystem = true;
        systemDatabase.isVisible(false);
        this.databases(databases.concat([systemDatabase]));
        if (this.databases().length == 1) {
            systemDatabase.activate();
        } else {
            var urlDatabase = appUrl.getDatabase();
            var newSelectedDb;
            if (urlDatabase != null && (newSelectedDb = this.databases.first(x => x.name == urlDatabase.name)) != null) {
                newSelectedDb.activate();
            } else {
                this.databases.first(x => x.isVisible()).activate();
            }
        }
    }

    filesystemsLoaded(filesystems) {
        this.filesystems(filesystems);
        if (this.filesystems().length != 0) {
            this.filesystems.first(x=> x.isVisible()).activate();
        }
    }

    counterStoragesLoaded(results: counterStorage[]) {
        this.counterStorages(results);
        if (this.counterStorages().length != 0) {
            this.counterStorages.first(x => x.isVisible()).activate();
        }
    }

    launchDocEditor(docId?: string, docsList?: pagedList) {
        var editDocUrl = appUrl.forEditDoc(docId, docsList ? docsList.collectionName : null, docsList ? docsList.currentItemIndex() : null, this.activeDatabase());
        this.navigate(editDocUrl);
    }

    connectToRavenServer() {
        this.databasesLoadedTask = new getDatabasesCommand()
            .execute()
            .fail(result => this.handleRavenConnectionFailure(result))
            .done(results => {
                this.databasesLoaded(results);
                this.fetchStudioConfig();
                this.fetchServerBuildVersion();
                this.fetchClientBuildVersion();
                this.fetchLicenseStatus();
                router.activate();
            });

        this.fileSystemsLoadedTask = new getFilesystemsCommand()
            .execute()
            .done((results: filesystem[]) => this.filesystemsLoaded(results));


        this.counterStoragesLoadedTask = new getCounterStoragesCommand()
            .execute()
            .done((results: counterStorage[]) => this.counterStoragesLoaded(results));
    }

    fetchStudioConfig() {
        new getDocumentWithMetadataCommand("Raven/StudioConfig", appUrl.getSystemDatabase())
            .execute()
            .done((doc: document) => {
                appUrl.warnWhenUsingSystemDatabase = doc["WarnWhenUsingSystemDatabase"];
            });
    }

    handleRavenConnectionFailure(result) {
        NProgress.done();
        sys.log("Unable to connect to Raven.", result);
        var tryAgain = 'Try again';
        var messageBoxResultPromise = app.showMessage("Couldn't connect to Raven. Details in the browser console.", ":-(", [tryAgain]);
        messageBoxResultPromise.done(messageBoxResult => {
            if (messageBoxResult === tryAgain) {
                NProgress.start();
                this.connectToRavenServer();
            }
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
        if (alert.type === alertType.danger || alert.type === alertType.warning) {
            this.recordedErrors.unshift(alert);
        }

        var currentAlert = this.currentAlert();
        if (currentAlert) {
            this.queuedAlert = alert;
            this.closeAlertAndShowNext(currentAlert);
        } else {
            this.currentAlert(alert);
            var fadeTime = 2000; // If there are no pending alerts, show it for 2 seconds before fading out.
            if (alert.type === alertType.danger || alert.type === alertType.warning) {
                fadeTime = 4000; // If there are no pending alerts, show the error alert for 4 seconds before fading out.
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
                var matchingDatabase = this.databases().first(db => db.name == databaseName);
                if (matchingDatabase && this.activeDatabase() !== matchingDatabase) {
                    ko.postbox.publish("ActivateDatabase", matchingDatabase);
                }
            });
        }
    }

    private activateFilesystemWithName(fileSystemName: string) {
        if (this.fileSystemsLoadedTask) {
            this.fileSystemsLoadedTask.done(() => {
                var matchingFileSystem = this.filesystems().first(fs => fs.name == fileSystemName);
                if (matchingFileSystem && this.activeFilesystem() !== matchingFileSystem) {
                    ko.postbox.publish("ActivateFilesystem", matchingFileSystem);
                }
            });
        }
    }

    private activateCounterStorageWithName(counterStorageName: string) {
        if (this.counterStoragesLoadedTask) {
            this.counterStoragesLoadedTask.done(() => {
                var matchingCounterStorage = this.counterStorages().first(cs => cs.name == counterStorageName);
                if (matchingCounterStorage && this.activeCounterStorage() !== matchingCounterStorage) {
                    ko.postbox.publish("ActivateCounterStorage", matchingCounterStorage);
                }
            });
        }
    }

    private updateChangesApi(newDb: database) {
        if (!newDb.disabled() && this.currentConnectedDatabase.name != newDb.name ||
            newDb.name == "<system>" && this.currentConnectedDatabase.name == newDb.name) {
            if (shell.currentDbChangesApi()) {
                shell.currentDbChangesApi().dispose();
            }

            shell.currentDbChangesApi(new changesApi(newDb,5000));

            shell.currentDbChangesApi().watchAllDocs(() => this.fetchDbStats(newDb));
            shell.currentDbChangesApi().watchAllIndexes(() => this.fetchDbStats(newDb));
            shell.currentDbChangesApi().watchBulks(() => this.fetchDbStats(newDb));

            this.currentConnectedDatabase = newDb;
        }
    }

    private fetchDbStats(db: database, forceFetch: boolean = false) {
        if (forceFetch)
        {
                if (db && !db.disabled()) {
                    new getDatabaseStatsCommand(db).execute().done(result => {db.statistics(result);});}
            
        } else {
            
            if (!db.isInStatsFetchCoolDown) {
                db.isInStatsFetchCoolDown = true;

                setTimeout(() => db.isInStatsFetchCoolDown = false, 5000);
                if (db && !db.disabled()) {
                    new getDatabaseStatsCommand(db).execute().done(result => { db.statistics(result); });

                }
            }
        }
    }

    private fetchFSStats(fs: filesystem, forceFetch: boolean = false) {
        var fs = this.activeFilesystem();
        if (fs) {
            new getFilesystemStatsCommand(fs)
                .execute()
                .done(result=> fs.statistics(result));
        }
    }

    selectDatabase(db: database) {
        db.activate();

        if (window.location.hash !== "#databases") {
            var updatedUrl = appUrl.forCurrentPage(db);
            this.navigate(updatedUrl);
        }
    }

    selectFileSystem(fs: filesystem) {
        fs.activate();

        if (window.location.hash !== "#filesystems") {
            var updatedUrl = appUrl.forCurrentPage(fs);
            this.navigate(updatedUrl);
        }
    }

    selectCounterStorage(cs: counterStorage) {
        cs.activate();

        if (window.location.hash !== "#counterstorages") {
            var updatedUrl = appUrl.forCurrentPage(cs);
            this.navigate(updatedUrl);
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
            .done((result: licenseStatusDto) => this.licenseStatus(result));
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

    showErrorsDialog() {
        require(["viewmodels/recentErrors"], ErrorDetails => {
            var dialog = new ErrorDetails(this.recordedErrors);
            app.showDialog(dialog);
        });
    }

    uploadStatusChanged(item: uploadItem) {
        var queue: uploadItem[] = uploadQueueHelper.parseUploadQueue(window.localStorage[uploadQueueHelper.localStorageUploadQueueKey + item.filesystem.name], item.filesystem);
        uploadQueueHelper.updateQueueStatus(item.id(), item.status(), queue);
        uploadQueueHelper.updateLocalStorage(queue, item.filesystem);
    }

    showLicenseStatusDialog() {
        require(["viewmodels/licensingStatus"], licensingStatus => {
            var dialog = new licensingStatus(this.licenseStatus());
            app.showDialog(dialog);
        });
    }
}

export = shell;
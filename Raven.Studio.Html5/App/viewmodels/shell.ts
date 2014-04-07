/// <reference path="../../Scripts/typings/nprogress/nprogress.d.ts" />
/// <reference path="../../Scripts/typings/bootstrap/bootstrap.d.ts" />

import router = require("plugins/router");
import app = require("durandal/app");
import sys = require("durandal/system");

import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");
import document = require("models/document");
import appUrl = require("common/appUrl");
import collection = require("models/collection");
import deleteDocuments = require("viewmodels/deleteDocuments");
import dialogResult = require("common/dialogResult");
import alertArgs = require("common/alertArgs");
import alertType = require("common/alertType");
import pagedList = require("common/pagedList");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getDatabasesCommand = require("commands/getDatabasesCommand");

import getBuildVersionCommand = require("commands/getBuildVersionCommand");
import getLicenseStatusCommand = require("commands/getLicenseStatusCommand");
import dynamicHeightBindingHandler = require("common/dynamicHeightBindingHandler");
import autoCompleteBindingHandler = require("common/autoCompleteBindingHandler");
import viewModelBase = require("viewmodels/viewModelBase");
import getDocumentsMetadataByIDPrefixCommand = require("commands/getDocumentsMetadataByIDPrefixCommand");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import changesApi = require("common/changesApi");

import getFilesystemsCommand = require("commands/filesystem/getFilesystemsCommand");
import getFilesystemStatsCommand = require("commands/filesystem/getFilesystemStatsCommand");


class shell extends viewModelBase {
    private router = router;

    databases = ko.observableArray<database>();
    currentAlert = ko.observable<alertArgs>();
    queuedAlert: alertArgs;
    databasesLoadedTask: JQueryPromise<any>;
    buildVersion = ko.observable<buildVersionDto>();
    licenseStatus = ko.observable<licenseStatusDto>();
    windowHeightObservable: KnockoutObservable<number>;
    appUrls: computedAppUrls;
    recordedErrors = ko.observableArray<alertArgs>();
    newIndexUrl = appUrl.forCurrentDatabase().newIndex;
    newTransformerUrl = appUrl.forCurrentDatabase().newTransformer;
    filesystems = ko.observableArray<filesystem>();
    filesystemsLoadedTask: JQueryPromise<any>;

    canShowFilesystemNavbar = ko.computed(() => this.filesystems().length > 0 && this.appUrls.isAreaActive('filesystems'));

    currentRawUrl = ko.observable<string>("");
    rawUrlIsVisible = ko.computed(() => this.currentRawUrl().length > 0);
    activeArea = ko.observable<string>("Databases");
    goToDocumentSearch = ko.observable<string>();
    goToDocumentSearchResults = ko.observableArray<string>();    

    static globalChangesApi: changesApi;
    static currentDbChangesApi = ko.observable<changesApi>(null);

    constructor() {
        super();
        ko.postbox.subscribe("Alert", (alert: alertArgs) => this.showAlert(alert));
        ko.postbox.subscribe("ActivateDatabaseWithName", (databaseName: string) => this.activateDatabaseWithName(databaseName));
        ko.postbox.subscribe("ActivateFilesystemWithName", (filesystemName: string) => this.activateFilesystemWithName(filesystemName));
        ko.postbox.subscribe("SetRawJSONUrl", (jsonUrl: string) => this.currentRawUrl(jsonUrl));
        ko.postbox.subscribe("ActivateDatabase", (db: database) => this.updateChangesApi(db));

        this.appUrls = appUrl.forCurrentDatabase();
        this.goToDocumentSearch.throttle(250).subscribe(search => this.fetchGoToDocSearchResults(search));
        dynamicHeightBindingHandler.install();
        autoCompleteBindingHandler.install();
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
            { route: 'filesystems/search', title: 'Search', moduleId: 'viewmodels/filesystem/filesystemSearch', nav: true, hash: this.appUrls.filesystemSearch },
            { route: 'filesystems/synchronization', title: 'Synchronization', moduleId: 'viewmodels/filesystem/filesystemSynchronization', nav: true, hash: this.appUrls.filesystemSynchronization },
            { route: 'filesystems/configuration', title: 'Configuration', moduleId: 'viewmodels/filesystem/configuration', nav: true, hash: this.appUrls.filesystemConfiguration },
            { route: 'filesystems/upload', title: 'Upload File', moduleId: 'viewmodels/filesystem/filesystemUploadFile', nav: false },
            { route: 'filesystems/edit', title: 'Upload File', moduleId: 'viewmodels/filesystem/filesystemEditFile', nav: false },
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

        shell.globalChangesApi = new changesApi(appUrl.getSystemDatabase());
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

    databasesLoaded(databases) {
        var systemDatabase = new database("<system>");
        systemDatabase.isSystem = true;
        systemDatabase.isVisible(false);
        this.databases(databases.concat([systemDatabase]));
        if (this.databases().length == 1) {
            systemDatabase.activate();
        } else {
            this.databases.first(x => x.isVisible()).activate();
        }
    }

    filesystemsLoaded(filesystems) {
        this.filesystems(filesystems);
        if (this.filesystems().length != 0) {
            this.filesystems.first(x=> x.isVisible()).activate();
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
                this.fetchBuildVersion();
                this.fetchLicenseStatus();
                router.activate();
            });

	    this.filesystemsLoadedTask = new getFilesystemsCommand()
	        .execute()
	        .fail(result => this.handleRavenConnectionFailure(result))
	        .done(results => {
	            this.filesystemsLoaded(results);
	            this.fetchBuildVersion();
	            this.fetchLicenseStatus();
	        });
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
            setTimeout(() => this.closeAlertAndShowNext(alert), fadeTime);
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

    activateDatabaseWithName(databaseName: string) {
        if (this.databasesLoadedTask) {
            this.databasesLoadedTask.done(() => {
                var matchingDatabase = this.databases().first(d => d.name == databaseName);
                if (matchingDatabase && this.activeDatabase() !== matchingDatabase) {
                    ko.postbox.publish("ActivateDatabase", matchingDatabase);
                }
            });
        }
    }

    activateFilesystemWithName(filesystemName: string) {
        if (this.filesystemsLoadedTask) {
            this.filesystemsLoadedTask.done(() => {
                var matchingFilesystem = this.filesystems().first(d => d.name == filesystemName);
                if (matchingFilesystem && this.activeFilesystem() !== matchingFilesystem) {
                    ko.postbox.publish("ActivateFilesystem", matchingFilesystem);
                }
            });
        }
    }

    updateChangesApi(newDb: database) {
        if (shell.currentDbChangesApi()) {
            shell.currentDbChangesApi().dispose();
        }
        shell.currentDbChangesApi(new changesApi(newDb));
    }

    modelPolling() {
        new getDatabasesCommand()
            .execute()
            .done(results => {
                ko.utils.arrayForEach(results, (result:database) => {
                    var existingDb = this.databases().first(d=> {
                        return d.name == result.name;
                    });
                if (!existingDb ) {
                    this.databases.unshift(result);
                    }
                });
            });
                
        new getFilesystemsCommand()
            .execute()
            .done(results => {
                ko.utils.arrayForEach(results, (result: filesystem) => {
                    var existingFs = this.filesystems().first(d=> {
                        return d.name == result.name;
                });
                    if (!existingFs) {
                        this.filesystems.unshift(result);
                    }
                });
        });

        var db = this.activeDatabase();
        if (db) {
            new getDatabaseStatsCommand(db)
                .execute()
                .done(result=> db.statistics(result));
        }

        var fs = this.activeFilesystem();
        if (fs) {
            new getFilesystemStatsCommand(fs)
                .execute()
                .done(result=> fs.statistics(result));
        }
    }

    selectDatabase(db: database) {
        if (db.name != this.activeDatabase().name) {
            db.activate();
            var updatedUrl = appUrl.forCurrentPage(db);
            this.navigate(updatedUrl);
        }
    }

    selectFilesystem(fs: filesystem) {
        if (fs.name != this.activeFilesystem().name) {
            fs.activate();
            var updatedUrl = appUrl.forCurrentPage(fs);
            this.navigate(updatedUrl);
        }
    }

    goToDoc(doc: documentMetadataDto) {
        this.goToDocumentSearch("");
        this.navigate(appUrl.forEditDoc(doc['@metadata']['@id'], null, null, this.activeDatabase()));
    }

    getDocCssClass(doc: documentMetadataDto) {
        return collection.getCollectionCssClass(doc['@metadata']['Raven-Entity-Name']);
    }

    fetchBuildVersion() {
        new getBuildVersionCommand()
            .execute()
            .done((result: buildVersionDto) => this.buildVersion(result));
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
        }
    }

    showErrorsDialog() {
        require(["viewmodels/recentErrors"], ErrorDetails => {
            var dialog = new ErrorDetails(this.recordedErrors);
            app.showDialog(dialog);
        });
    }
}

export = shell;

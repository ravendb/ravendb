/// <reference path="../../typings/tsd.d.ts" />

import router = require("plugins/router");
import app = require("durandal/app");
import sys = require("durandal/system");
import viewLocator = require("durandal/viewLocator");

import MENU_BASED_ROUTER_CONFIGURATION = require("common/shell/routerConfiguration");
import menu = require("common/shell/menu");
import generateMenuItems = require("common/shell/menu/generateMenuItems");
import activeResourceTracker = require("common/shell/activeResourceTracker");
import resourceSwitcher = require("common/shell/resourceSwitcher");
import searchBox = require("common/shell/searchBox");
import resource = require("models/resources/resource");
import database = require("models/resources/database");
import collection = require("models/database/documents/collection");
import uploadItem = require("models/filesystem/uploadItem");
import license = require("models/auth/license");
import topology = require("models/database/replication/topology");
import environmentColor = require("models/resources/environmentColor");

import appUrl = require("common/appUrl");
import uploadQueueHelper = require("common/uploadQueueHelper");
import pagedList = require("common/pagedList");
import dynamicHeightBindingHandler = require("common/bindingHelpers/dynamicHeightBindingHandler");
import autoCompleteBindingHandler = require("common/bindingHelpers/autoCompleteBindingHandler");
import enableResizeBindingHandler = require("common/bindingHelpers/enableResizeBindingHandler");
import helpBindingHandler = require("common/bindingHelpers/helpBindingHandler");
import oauthContext = require("common/oauthContext");
import messagePublisher = require("common/messagePublisher");
import apiKeyLocalStorage = require("common/apiKeyLocalStorage");
import extensions = require("common/extensions");
import notificationCenter = require("common/notifications/notificationCenter");

import getClientBuildVersionCommand = require("commands/database/studio/getClientBuildVersionCommand");
import getLicenseStatusCommand = require("commands/auth/getLicenseStatusCommand");
import getSupportCoverageCommand = require("commands/auth/getSupportCoverageCommand");
import getServerConfigsCommand = require("commands/database/studio/getServerConfigsCommand");
import getClusterTopologyCommand = require("commands/database/cluster/getClusterTopologyCommand");

import viewModelBase = require("viewmodels/viewModelBase");
import accessHelper = require("viewmodels/shell/accessHelper");
import licensingStatus = require("viewmodels/common/licensingStatus");
import enterApiKey = require("viewmodels/common/enterApiKey");

//TODO: extract cluster related logic to separate class
//TODO: extract api key related logic to separate class 
class shell extends viewModelBase {

    private router = router;
    static studioConfigDocumentId = "Raven/StudioConfig";
    private activeResource: KnockoutObservable<resource> = activeResourceTracker.default.resource;

    renewOAuthTokenTimeoutId: number;
    showContinueTestButton = ko.computed(() => viewModelBase.hasContinueTestOption()); //TODO:
    showLogOutButton: KnockoutComputed<boolean>; //TODO:
    
    notificationCenter = notificationCenter.instance;

    static clusterMode = ko.observable<boolean>(false); //TODO: extract from shell
    isInCluster = ko.computed(() => shell.clusterMode()); //TODO: extract from shell

    serverBuildVersion = ko.observable<serverBuildVersionDto>();
    static serverMainVersion = ko.observable<number>(4);
    clientBuildVersion = ko.observable<clientBuildVersionDto>();

    windowHeightObservable: KnockoutObservable<number>; //TODO: delete?
    currentRawUrl = ko.observable<string>("");
    rawUrlIsVisible = ko.computed(() => this.currentRawUrl().length > 0);
    showSplash = viewModelBase.showSplash;

    licenseStatus = license.licenseCssClass;
    supportStatus = license.supportCssClass;

    mainMenu = new menu(generateMenuItems(activeResourceTracker.default.resource()));
    searchBox = new searchBox();
    resourceSwitcher = new resourceSwitcher();

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
            /* TODO  this.globalChangesApi = new changesApi(appUrl.getSystemDatabase());
             this.notifications = this.createNotifications();*/
        });

        ko.postbox.subscribe("SetRawJSONUrl", (jsonUrl: string) => this.currentRawUrl(jsonUrl));
        ko.postbox.subscribe("UploadFileStatusChanged", (uploadStatus: uploadItem) => this.uploadStatusChanged(uploadStatus));

        dynamicHeightBindingHandler.install();
        autoCompleteBindingHandler.install();
        enableResizeBindingHandler.install();
        helpBindingHandler.install();

        this.clientBuildVersion.subscribe(v => viewModelBase.clientVersion("4.0." + v.BuildVersion));
    }

    // Override canActivate: we can always load this page, regardless of any system db prompt.
    canActivate(args: any): any {
        return true;
    }

    activate(args: any) {
        super.activate(args, true);

        oauthContext.enterApiKeyTask.done(() => this.connectToRavenServer());

        this.setupRouting();

        var self = this;

        $(window).bind("storage", (e: any) => {
            if (e.originalEvent.key === apiKeyLocalStorage.localStorageName) {
                this.onLogOut();
            }
        });

        $(window).resize(() => self.activeResource.valueHasMutated());
        //TODO: return shell.fetchLicenseStatus();

    }

    private setupRouting() {
        let routes = this.getRoutesForNewLayout();
        routes.pushAll(routes);
        router.map(routes).buildNavigationModel();
        //TODO: do we indicate this? router.isNavigating.subscribe(isNavigating => this.showNavigationProgress(isNavigating));
        //TODO: do we indicated this? router.on('router:navigation:cancelled', () => this.showNavigationProgress(false));

        appUrl.mapUnknownRoutes(router);
    }

    attached() {
        super.attached();

        sys.error = (e: any) => {
            console.error(e);
            messagePublisher.reportError("Failed to load routed module!", e);
        };
    }

    private initializeShellComponents() {
        this.mainMenu.initialize();
        let updateMenu = (resource: resource) => {
            let items = generateMenuItems(resource);
            this.mainMenu.update(items);
        };

        updateMenu(activeResourceTracker.default.resource());
        activeResourceTracker.default.resource.subscribe(updateMenu);

        this.resourceSwitcher.initialize();
        this.searchBox.initialize();
    }

    compositionComplete() {
        super.compositionComplete();
        $("#body").removeClass('loading-active');

        this.initializeShellComponents();
    }

    private preLoadRecentErrorsView() {
        // preload this view as in case of failure server can't serve it.
        viewLocator.locateView("views/common/recentErrors");
    }

    /*
    static fetchStudioConfig() {
        var hotSpareTask = new getHotSpareInformation().execute();
        var configTask = new getDocumentWithMetadataCommand(shell.studioConfigDocumentId, appUrl.getSystemDatabase(), true).execute();

        $.when(hotSpareTask, configTask).done((hotSpareResult, doc: documentClass) => {
            var hotSpare = <HotSpareDto>hotSpareResult[0];

            if (license.licenseStatus().Attributes.hotSpare === "true") {
                // override environment colors with hot spare
                this.activateHotSpareEnvironment(hotSpare);
            } else {
                var envColor = doc && doc["EnvironmentColor"];
                if (envColor != null) {
                    var color = new environmentColor(envColor.Name, envColor.BackgroundColor);
                    shell.selectedEnvironmentColorStatic(color);
                    shell.originalEnvironmentColor(color);
                }
            }
        });
    }*/

    private getRoutesForNewLayout() {
        let routes = [
            {
                route: "databases/upgrade",
                title: "Upgrade in progress",
                moduleId: "viewmodels/common/upgrade",
                nav: false,
                dynamicHash: this.appUrls.upgrade
            },
            {
                route: "databases/edit",
                title: "Edit Document",
                moduleId: "viewmodels/database/documents/editDocument",
                nav: false
            },
            {
                route: "filesystems/files",
                title: "Files",
                moduleId: "viewmodels/filesystem/files/filesystemFiles",
                nav: true,
                dynamicHash: this.appUrls.filesystemFiles
            },
            {
                route: "filesystems/search",
                title: "Search",
                moduleId: "viewmodels/filesystem/search/search",
                nav: true,
                dynamicHash: this.appUrls.filesystemSearch
            },
            {
                route: "filesystems/synchronization*details",
                title: "Synchronization",
                moduleId: "viewmodels/filesystem/synchronization/synchronization",
                nav: true,
                dynamicHash: this.appUrls.filesystemSynchronization
            },
            {
                route: "filesystems/status*details",
                title: "Status",
                moduleId: "viewmodels/filesystem/status/status",
                nav: true,
                dynamicHash: this.appUrls.filesystemStatus
            },
            {
                route: "filesystems/tasks*details",
                title: "Tasks",
                moduleId: "viewmodels/filesystem/tasks/tasks",
                nav: true,
                dynamicHash: this.appUrls.filesystemTasks
            },
            {
                route: "filesystems/settings*details",
                title: "Settings",
                moduleId: "viewmodels/filesystem/settings/settings",
                nav: true,
                dynamicHash: this.appUrls.filesystemSettings
            },
            {
                route: "filesystems/configuration",
                title: "Configuration",
                moduleId: "viewmodels/filesystem/configurations/configuration",
                nav: true,
                dynamicHash: this.appUrls.filesystemConfiguration
            },
            {
                route: "filesystems/edit",
                title: "Edit File",
                moduleId: "viewmodels/filesystem/files/filesystemEditFile",
                nav: false
            },
            {
                route: "counterstorages/counters",
                title: "Counters",
                moduleId: "viewmodels/counter/counters",
                nav: true,
                dynamicHash: this.appUrls.counterStorageCounters
            },
            {
                route: "counterstorages/replication",
                title: "Replication",
                moduleId: "viewmodels/counter/counterStorageReplication",
                nav: true,
                dynamicHash: this.appUrls.counterStorageReplication
            },
            {
                route: "counterstorages/tasks*details",
                title: "Stats",
                moduleId: "viewmodels/counter/tasks/tasks",
                nav: true,
                dynamicHash: this.appUrls.counterStorageStats
            },
            {
                route: "counterstorages/stats",
                title: "Stats",
                moduleId: "viewmodels/counter/counterStorageStats",
                nav: true,
                dynamicHash: this.appUrls.counterStorageStats
            },
            {
                route: "counterstorages/configuration",
                title: "Configuration",
                moduleId: "viewmodels/counter/counterStorageConfiguration",
                nav: true,
                dynamicHash: this.appUrls.counterStorageConfiguration
            },
            {
                route: "counterstorages/edit",
                title: "Edit Counter",
                moduleId: "viewmodels/counter/editCounter",
                nav: false
            },
            {
                route: "timeseries/types",
                title: "Types",
                moduleId: "viewmodels/timeSeries/timeSeriesTypes",
                nav: true,
                dynamicHash: this.appUrls.timeSeriesType
            },
            {
                route: "timeseries/points",
                title: "Points",
                moduleId: "viewmodels/timeSeries/timeSeriesPoints",
                nav: true,
                dynamicHash: this.appUrls.timeSeriesPoints
            },
            {
                route: "timeseries/stats",
                title: "Stats",
                moduleId: "viewmodels/timeSeries/timeSeriesStats",
                nav: true,
                dynamicHash: this.appUrls.timeSeriesStats
            },
            {
                route: "timeseries/configuration*details",
                title: "Configuration",
                moduleId: "viewmodels/timeSeries/configuration/configuration",
                nav: true,
                dynamicHash: this.appUrls.timeSeriesConfiguration
            }
        ] as Array<DurandalRouteConfiguration>;

        return routes.concat(MENU_BASED_ROUTER_CONFIGURATION);
    }

    setupApiKey() {
        // try to find api key as studio hash parameter
        var hash = window.location.hash;
        if (hash === "#has-api-key") {
            return this.showApiKeyDialog();
        } else if (hash.match(/#api-key/g)) {
            var match = /#api-key=(.*)/.exec(hash);
            if (match && match.length === 2) {
                oauthContext.apiKey(match[1]);
                apiKeyLocalStorage.setValue(match[1]);
            }
            var splittedHash = hash.split("&#api-key");
            var url = (splittedHash.length === 1) ? "#resources" : splittedHash[0];
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
        /* TODO: oauthContext.authHeader(null);
         new getDatabaseStatsCommand(null).execute();*/
    }
   
    private getIndexingDisbaledValue(indexingDisabledString: string) {
        if (indexingDisabledString === undefined || indexingDisabledString == null)
            return false;

        if (indexingDisabledString.toLowerCase() === "true")
            return true;

        return false;
    }

    launchDocEditor(docId?: string, docsList?: pagedList) {
        var editDocUrl = appUrl.forEditDoc(docId, this.activeDatabase());
        this.navigate(editDocUrl);
    }

    loadServerConfig(): JQueryPromise<void> {
        const deferred = $.Deferred<void>();

        new getServerConfigsCommand()
            .execute()
            .done((serverConfigs: serverConfigsDto) => {
                accessHelper.isGlobalAdmin(serverConfigs.IsGlobalAdmin);
                accessHelper.canReadWriteSettings(serverConfigs.CanReadWriteSettings);
                accessHelper.canReadSettings(serverConfigs.CanReadSettings);
                accessHelper.canExposeConfigOverTheWire(serverConfigs.CanExposeConfigOverTheWire);
            })
            .always(() => deferred.resolve());

        return deferred;
    }

    connectToRavenServer() {
        const serverConfigsLoadTask: JQueryPromise<void> = this.loadServerConfig();
        const resourcesTask = this.resourcesManager.init();
        return $.when<any>(serverConfigsLoadTask, resourcesTask);
    }

    private static activateHotSpareEnvironment(hotSpare: HotSpareDto) {
        var color = new environmentColor(hotSpare.ActivationMode === "Activated" ? "Active Hot Spare" : "Hot Spare", "#FF8585");
        license.hotSpare(hotSpare);
    }

    private handleRavenConnectionFailure(result: any) {
        if (result.status === 401) {
            // Unauthorized might be caused by invalid credentials. 
            // Remove them from both local storage and oauth context.
            apiKeyLocalStorage.clean();
            oauthContext.clean();
        }

        sys.log("Unable to connect to Raven.", result);
        const tryAgain = "Try again";
        this.confirmationMessage(':-(', "Couldn't connect to Raven. Details in the browser console.", [tryAgain])
            .done(() => {
                this.connectToRavenServer();
            });
    }

    getDocCssClass(doc: documentMetadataDto) {
        return collection.getCollectionCssClass((<any>doc)["@metadata"]["Raven-Entity-Name"], this.activeDatabase());
    }

    fetchServerBuildVersion() {
        /* TODO:
        new getServerBuildVersionCommand()
            .execute()
            .done((serverBuildResult: serverBuildVersionDto) => {
                this.serverBuildVersion(serverBuildResult);

                var currentBuildVersion = serverBuildResult.BuildVersion;
                if (currentBuildVersion !== 13) {
                    shell.serverMainVersion(Math.floor(currentBuildVersion / 10000));
                }

                if (serverBuildReminder.isReminderNeeded() && currentBuildVersion !== 13) {
                    new getLatestServerBuildVersionCommand(true, 35000, 39999) //pass false as a parameter to get the latest unstable
                        .execute()
                        .done((latestServerBuildResult: latestServerBuildVersionDto) => {
                            if (latestServerBuildResult.LatestBuild > currentBuildVersion) { //
                                var latestBuildReminderViewModel = new latestBuildReminder(latestServerBuildResult);
                                app.showDialog(latestBuildReminderViewModel);
                            }
                        });
                }
            });*/
    }

    fetchClientBuildVersion() {
        new getClientBuildVersionCommand()
            .execute()
            .done((result: clientBuildVersionDto) => { this.clientBuildVersion(result); });
    }

    fetchClusterTopology() {
        new getClusterTopologyCommand(null)
            .execute()
            .done((topology: topology) => {
                shell.clusterMode(topology && topology.allNodes().length > 0);
            });
    }

    static fetchLicenseStatus(): JQueryPromise<licenseStatusDto> {
        return new getLicenseStatusCommand()
            .execute()
            .done((result: licenseStatusDto) => {
                if (result.Status.contains("AGPL")) {
                    result.Status = "Development Only";
                }
                license.licenseStatus(result);
            });
    }

    fetchSupportCoverage() {
        new getSupportCoverageCommand()
            .execute()
            .done((result: supportCoverageDto) => {
                license.supportCoverage(result);
            });
    }

    showApiKeyDialog() {
        var dialog = new enterApiKey();
        return app.showDialog(dialog).then(() => window.location.href = "#resources");
    }

    uploadStatusChanged(item: uploadItem) {
        var queue: uploadItem[] = uploadQueueHelper.parseUploadQueue(window.localStorage[uploadQueueHelper.localStorageUploadQueueKey + item.filesystem.name], item.filesystem);
        uploadQueueHelper.updateQueueStatus(item.id(), item.status(), queue);
        uploadQueueHelper.updateLocalStorage(queue, item.filesystem);
    }

    showLicenseStatusDialog() {
        var dialog = new licensingStatus(license.licenseStatus(), license.supportCoverage(), license.hotSpare());
        app.showDialog(dialog);
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

    navigateToClusterSettings() {
        this.navigate(this.appUrls.adminSettingsCluster());
    }
}

export = shell;
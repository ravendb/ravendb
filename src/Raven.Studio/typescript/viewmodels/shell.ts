/// <reference path="../../typings/tsd.d.ts" />

import router = require("plugins/router");
import app = require("durandal/app");
import sys = require("durandal/system");
import viewLocator = require("durandal/viewLocator");

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
import changesContext = require("common/changesContext");
import changesApi = require("common/changesApi");
import allRoutes = require("common/shell/routes");

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
import getServerBuildVersionCommand = require("commands/resources/getServerBuildVersionCommand");
import getLatestServerBuildVersionCommand = require("commands/database/studio/getLatestServerBuildVersionCommand");

import viewModelBase = require("viewmodels/viewModelBase");
import accessHelper = require("viewmodels/shell/accessHelper");
import licensingStatus = require("viewmodels/common/licensingStatus");
import enterApiKey = require("viewmodels/common/enterApiKey");

import serverBuildReminder = require("common/serverBuildReminder");
import latestBuildReminder = require("viewmodels/common/latestBuildReminder")

import eventsCollector = require("common/eventsCollector");

//TODO: extract cluster related logic to separate class
//TODO: extract api key related logic to separate class 
class shell extends viewModelBase {

    private router = router;
    static studioConfigDocumentId = "Raven/StudioConfig";

    renewOAuthTokenTimeoutId: number;
    showContinueTestButton = ko.computed(() => viewModelBase.hasContinueTestOption()); //TODO:
    showLogOutButton: KnockoutComputed<boolean>; //TODO:
    
    notificationCenter = notificationCenter.instance;

    static clusterMode = ko.observable<boolean>(false); //TODO: extract from shell
    isInCluster = ko.computed(() => shell.clusterMode()); //TODO: extract from shell

    serverBuildVersion = ko.observable<serverBuildVersionDto>();
    static serverMainVersion = ko.observable<number>(4);
    static serverMinorVersion = ko.observable<number>(0);
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

    displayUsageStatsInfo = ko.observable<boolean>(false);
    trackingTask = $.Deferred();

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
            changesContext.default
                .connectGlobalChangesApi()
                .done(() => {
                    this.resourcesManager.createGlobalNotifications();
                });
        });

        ko.postbox.subscribe("SetRawJSONUrl", (jsonUrl: string) => this.currentRawUrl(jsonUrl));
        ko.postbox.subscribe("UploadFileStatusChanged", (uploadStatus: uploadItem) => this.uploadStatusChanged(uploadStatus));

        dynamicHeightBindingHandler.install();
        autoCompleteBindingHandler.install();
        enableResizeBindingHandler.install();
        helpBindingHandler.install();

        this.clientBuildVersion.subscribe(v =>
            viewModelBase.clientVersion("4.0." + v.BuildVersion));

        this.serverBuildVersion.subscribe(buildVersionDto => {
            this.initAnalytics({ SendUsageStats: true }, [ buildVersionDto ]);
        });
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

        this.fetchClientBuildVersion();
        this.fetchServerBuildVersion();
    }

    private setupRouting() {
        let routes = allRoutes.get(this.appUrls);
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
        new getServerBuildVersionCommand()
            .execute()
            .done((serverBuildResult: serverBuildVersionDto) => {
                this.serverBuildVersion(serverBuildResult);

                var currentBuildVersion = serverBuildResult.BuildNumber;
                if (currentBuildVersion !== DEV_BUILD_NUMBER) {
                    shell.serverMainVersion(Math.floor(currentBuildVersion / 10000));
                }

            });
        
    }

    fetchClientBuildVersion() {
        new getClientBuildVersionCommand()
            .execute()
            .done((result: clientBuildVersionDto) => {
                this.clientBuildVersion(result);
            });
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
        return app.showBootstrapDialog(dialog).then(() => window.location.href = "#resources");
    }

    uploadStatusChanged(item: uploadItem) {
        var queue: uploadItem[] = uploadQueueHelper.parseUploadQueue(window.localStorage[uploadQueueHelper.localStorageUploadQueueKey + item.filesystem.name], item.filesystem);
        uploadQueueHelper.updateQueueStatus(item.id(), item.status(), queue);
        uploadQueueHelper.updateLocalStorage(queue, item.filesystem);
    }

    showLicenseStatusDialog() {
        var dialog = new licensingStatus(license.licenseStatus(), license.supportCoverage(), license.hotSpare());
        app.showBootstrapDialog(dialog);
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

    private initAnalytics(config: any, buildVersionResult: [serverBuildVersionDto]) {
        if (eventsCollector.gaDefined()) {
            if (config == null || !("SendUsageStats" in config)) {
                // ask user about GA
                this.displayUsageStatsInfo(true);

                this.trackingTask.done((accepted: boolean) => {
                    this.displayUsageStatsInfo(false);

                    if (accepted) {
                        this.configureAnalytics(true, buildVersionResult);
                    }
                });
            } else {
                this.configureAnalytics(config.SendUsageStats, buildVersionResult);
            }
        } else {
            // user has uBlock etc?
            this.configureAnalytics(false, buildVersionResult);
        }
    }

    collectUsageData() {
        this.trackingTask.resolve(true);
    }

    doNotCollectUsageData() {
        this.trackingTask.resolve(false);
    }

    private configureAnalytics(track: boolean, [buildVersionResult]: [serverBuildVersionDto]) {
        let currentBuildVersion = buildVersionResult.BuildNumber;
        let shouldTrack = track && currentBuildVersion !== DEV_BUILD_NUMBER;
        if (currentBuildVersion !== DEV_BUILD_NUMBER) {
            shell.serverMainVersion(Math.floor(currentBuildVersion / 10000));
        } 

        var env = license.licenseStatus() && license.licenseStatus().IsCommercial ? "prod" : "dev";
        var version = buildVersionResult.Version;
        eventsCollector.default.initialize(
            shell.serverMainVersion() + "." + shell.serverMinorVersion(), currentBuildVersion, env, version, shouldTrack);
    }
}

export = shell;
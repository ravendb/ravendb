/// <reference path="../../typings/tsd.d.ts" />

import router = require("plugins/router");
import app = require("durandal/app");
import sys = require("durandal/system");
import menu = require("common/shell/menu");
import generateMenuItems = require("common/shell/menu/generateMenuItems");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import databaseSwitcher = require("common/shell/databaseSwitcher");
import searchBox = require("common/shell/searchBox");
import database = require("models/resources/database");
import license = require("models/auth/license");
import environmentColor = require("models/resources/environmentColor");
import changesContext = require("common/changesContext");
import allRoutes = require("common/shell/routes");
import registration = require("viewmodels/shell/registration");
import collection = require("models/database/documents/collection");

import appUrl = require("common/appUrl");
import dynamicHeightBindingHandler = require("common/bindingHelpers/dynamicHeightBindingHandler");
import autoCompleteBindingHandler = require("common/bindingHelpers/autoCompleteBindingHandler");
import helpBindingHandler = require("common/bindingHelpers/helpBindingHandler");
import oauthContext = require("common/oauthContext");
import messagePublisher = require("common/messagePublisher");
import apiKeyLocalStorage = require("common/storage/apiKeyLocalStorage");
import extensions = require("common/extensions");
import notificationCenter = require("common/notifications/notificationCenter");

import getClientBuildVersionCommand = require("commands/database/studio/getClientBuildVersionCommand");
import getSupportCoverageCommand = require("commands/auth/getSupportCoverageCommand");
import getServerConfigsCommand = require("commands/database/studio/getServerConfigsCommand");
import getServerBuildVersionCommand = require("commands/resources/getServerBuildVersionCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import accessHelper = require("viewmodels/shell/accessHelper");
import licensingStatus = require("viewmodels/common/licensingStatus");
import enterApiKey = require("viewmodels/common/enterApiKey");
import eventsCollector = require("common/eventsCollector");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import footer = require("common/shell/footer");
import feedback = require("viewmodels/shell/feedback");

import protractedCommandsDetector = require("common/notifications/protractedCommandsDetector");
import requestExecution = require("common/notifications/requestExecution");
import studioSettings = require("common/settings/studioSettings");

//TODO: extract cluster related logic to separate class
//TODO: extract api key related logic to separate class 
class shell extends viewModelBase {

    private router = router;
    static studioConfigDocumentId = "Raven/StudioConfig";

    renewOAuthTokenTimeoutId: number;
    showContinueTestButton = ko.pureComputed(() => viewModelBase.hasContinueTestOption()); //TODO:
    showLogOutButton: KnockoutComputed<boolean>; //TODO:
    
    notificationCenter = notificationCenter.instance;
    collectionsTracker = collectionsTracker.default;
    footer = footer.default;

    static clusterMode = ko.observable<boolean>(false); //TODO: extract from shell
    isInCluster = ko.computed(() => shell.clusterMode()); //TODO: extract from shell

    static serverBuildVersion = ko.observable<serverBuildVersionDto>();
    static serverMainVersion = ko.observable<number>(4);
    static serverMinorVersion = ko.observable<number>(0);
    clientBuildVersion = ko.observable<clientBuildVersionDto>();

    windowHeightObservable: KnockoutObservable<number>; //TODO: delete?
    currentRawUrl = ko.observable<string>("");
    rawUrlIsVisible = ko.computed(() => this.currentRawUrl().length > 0);
    showSplash = viewModelBase.showSplash;

    licenseStatus = license.licenseCssClass;
    supportStatus = license.supportCssClass;

    mainMenu = new menu(generateMenuItems(activeDatabaseTracker.default.database()));
    searchBox = new searchBox();
    databaseSwitcher = new databaseSwitcher();

    displayUsageStatsInfo = ko.observable<boolean>(false);
    trackingTask = $.Deferred();

    studioLoadingFakeRequest: requestExecution;

    constructor() {
        super();

        this.studioLoadingFakeRequest = protractedCommandsDetector.instance.requestStarted(0);

        extensions.install();

        this.showLogOutButton = ko.computed(() => {
            var lsApiKey = apiKeyLocalStorage.get();
            var contextApiKey = oauthContext.apiKey();
            return lsApiKey || contextApiKey;
        });
        oauthContext.enterApiKeyTask = this.setupApiKey();
        oauthContext.enterApiKeyTask.done(() => {
            changesContext.default
                .connectServerWideNotificationCenter();

            // load global settings
            studioSettings.default.globalSettings();

            // bind event handles before we connect to server wide notification center 
            // (connection will be started after executing this method) - it was just scheduled 2 lines above
            // please notice we don't wait here for connection to be established
            // since this invocation is sync we can't end up with race condition
            this.databasesManager.setupGlobalNotifications();
            this.notificationCenter.setupGlobalNotifications(changesContext.default.serverNotifications());
        });

        ko.postbox.subscribe("SetRawJSONUrl", (jsonUrl: string) => this.currentRawUrl(jsonUrl));

        dynamicHeightBindingHandler.install();
        autoCompleteBindingHandler.install();
        helpBindingHandler.install();

        this.clientBuildVersion.subscribe(v =>
            viewModelBase.clientVersion(v.Version));

        shell.serverBuildVersion.subscribe(buildVersionDto => {
            this.initAnalytics({ SendUsageStats: true }, [ buildVersionDto ]);
        });

        activeDatabaseTracker.default.database.subscribe(newDatabase => footer.default.forDatabase(newDatabase));
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

        $(window).resize(() => self.activeDatabase.valueHasMutated());

        this.fetchClientBuildVersion();
        this.fetchServerBuildVersion();

        return license.fetchLicenseStatus();
    }

    private setupRouting() {
        const routes = allRoutes.get(this.appUrls);
        routes.push(...routes);
        router.map(routes).buildNavigationModel();

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
        let updateMenu = (db: database) => {
            let items = generateMenuItems(db);
            this.mainMenu.update(items);
        };

        updateMenu(activeDatabaseTracker.default.database());
        activeDatabaseTracker.default.database.subscribe(updateMenu);

        this.databaseSwitcher.initialize();
        this.searchBox.initialize();
    }

    compositionComplete() {
        super.compositionComplete();
        $("body").removeClass('loading-active');

        this.studioLoadingFakeRequest.markCompleted();
        this.studioLoadingFakeRequest = null;

        this.initializeShellComponents();

        registration.showRegistrationDialogIfNeeded(license.licenseStatus());
    }

    urlForCollection(coll: collection) {
        return appUrl.forDocuments(coll.name, this.activeDatabase());
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
        const hash = window.location.hash;
        if (hash === "#has-api-key") {
            return this.showApiKeyDialog();
        } else if (hash.match(/#api-key/g)) {
            const match = /#api-key=(.*)/.exec(hash);
            if (match && match.length === 2) {
                oauthContext.apiKey(match[1]);
                apiKeyLocalStorage.setValue(match[1]);
            }
            const splittedHash = hash.split("&#api-key");
            const url = (splittedHash.length === 1) ? "#databases" : splittedHash[0];
            window.location.href = url;
        } else {
            const apiKeyFromStorage = apiKeyLocalStorage.get();
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
        const managerTask = this.databasesManager.init();
        return $.when<any>(serverConfigsLoadTask, managerTask);
    }

    private static activateHotSpareEnvironment(hotSpare: HotSpareDto) {
        const color = new environmentColor(hotSpare.ActivationMode === "Activated" ? "Active Hot Spare" : "Hot Spare", "#FF8585");
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

    fetchServerBuildVersion() {
        new getServerBuildVersionCommand()
            .execute()
            .done((serverBuildResult: serverBuildVersionDto) => {
                shell.serverBuildVersion(serverBuildResult);

                const currentBuildVersion = serverBuildResult.BuildVersion;
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

    fetchSupportCoverage() {
        new getSupportCoverageCommand()
            .execute()
            .done((result: supportCoverageDto) => {
                license.supportCoverage(result);
            });
    }

    showApiKeyDialog() {
        const dialog = new enterApiKey();
        return app.showBootstrapDialog(dialog).then(() => window.location.href = "#databases");
    }

    showLicenseStatusDialog() {
        const dialog = new licensingStatus(license.licenseStatus(), license.supportCoverage(), license.hotSpare());
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
        const currentBuildVersion = buildVersionResult.BuildVersion;
        const shouldTrack = track && currentBuildVersion !== DEV_BUILD_NUMBER;
        if (currentBuildVersion !== DEV_BUILD_NUMBER) {
            shell.serverMainVersion(Math.floor(currentBuildVersion / 10000));
        } 

        const env = license.licenseStatus() && license.licenseStatus().Type === "Commercial" ? "prod" : "dev";
        const version = buildVersionResult.FullVersion;
        eventsCollector.default.initialize(
            shell.serverMainVersion() + "." + shell.serverMinorVersion(), currentBuildVersion, env, version, shouldTrack);
    }

    static openFeedbackForm() {
        const dialog = new feedback(shell.clientVersion(), shell.serverBuildVersion().FullVersion);
        app.showBootstrapDialog(dialog);
    }
}

export = shell;
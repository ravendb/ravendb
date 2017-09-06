/// <reference path="../../typings/tsd.d.ts" />

import router = require("plugins/router");
import app = require("durandal/app");
import sys = require("durandal/system");
import menu = require("common/shell/menu");
import generateMenuItems = require("common/shell/menu/generateMenuItems");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import databaseSwitcher = require("common/shell/databaseSwitcher");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import favNodeBadge = require("common/shell/favNodeBadge");
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
import messagePublisher = require("common/messagePublisher");
import extensions = require("common/extensions");
import notificationCenter = require("common/notifications/notificationCenter");

import getClientBuildVersionCommand = require("commands/database/studio/getClientBuildVersionCommand");
import getSupportCoverageCommand = require("commands/auth/getSupportCoverageCommand");
import getServerConfigsCommand = require("commands/database/studio/getServerConfigsCommand");
import getServerBuildVersionCommand = require("commands/resources/getServerBuildVersionCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import accessHelper = require("viewmodels/shell/accessHelper");
import licensingStatus = require("viewmodels/common/licensingStatus");
import eventsCollector = require("common/eventsCollector");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import footer = require("common/shell/footer");
import feedback = require("viewmodels/shell/feedback");
import continueTest = require("common/shell/continueTest");

import protractedCommandsDetector = require("common/notifications/protractedCommandsDetector");
import requestExecution = require("common/notifications/requestExecution");
import studioSettings = require("common/settings/studioSettings");

//TODO: extract cluster related logic to separate class
//TODO: extract api key related logic to separate class 
class shell extends viewModelBase {

    private router = router;
    static studioConfigDocumentId = "Raven/StudioConfig";

    notificationCenter = notificationCenter.instance;
    collectionsTracker = collectionsTracker.default;
    footer = footer.default;
    clusterManager = clusterTopologyManager.default;
    continueTest = continueTest.default;

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
    favNodeBadge = new favNodeBadge();

    displayUsageStatsInfo = ko.observable<boolean>(false);
    trackingTask = $.Deferred();

    studioLoadingFakeRequest: requestExecution;

    private onBootstrapFinishedTask = $.Deferred<void>();
    
    showConnectionLost = ko.pureComputed(() => {
        const serverWideWebSocket = changesContext.default.serverNotifications();
        
        if (!serverWideWebSocket) {
            return false;
        }
        
        const errorState = serverWideWebSocket.inErrorState();
        const ignoreError = serverWideWebSocket.ignoreWebSocketConnectionError();
        
        return errorState && !ignoreError;
    });

    constructor() {
        super();

        this.studioLoadingFakeRequest = protractedCommandsDetector.instance.requestStarted(0);

        extensions.install();

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

        const licenseTask = license.fetchLicenseStatus();
        const topologyTask = this.clusterManager.init();

        $.when<any>(licenseTask, topologyTask)
            .done(() => {
                changesContext.default
                    .connectServerWideNotificationCenter();

                // load global settings
                studioSettings.default.globalSettings();

                // bind event handles before we connect to server wide notification center 
                // (connection will be started after executing this method) - it was just scheduled 2 lines above
                // please notice we don't wait here for connection to be established
                // since this invocation is sync we can't end up with race condition
                this.databasesManager.setupGlobalNotifications();
                this.clusterManager.setupGlobalNotifications();
                this.notificationCenter.setupGlobalNotifications(changesContext.default.serverNotifications());

                this.connectToRavenServer();
            })
            .then(() => this.onBootstrapFinishedTask.resolve(), () => this.onBootstrapFinishedTask.reject());

        this.setupRouting();

        //TODO: should we await for api key here? 
        this.fetchClientBuildVersion();
        this.fetchServerBuildVersion();
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
        this.favNodeBadge.initialize();
    }

    compositionComplete() {
        super.compositionComplete();
        $("body").removeClass('loading-active');

        this.studioLoadingFakeRequest.markCompleted();
        this.studioLoadingFakeRequest = null;

        this.initializeShellComponents();

        this.onBootstrapFinishedTask
            .done(() => {
                registration.showRegistrationDialogIfNeeded(license.licenseStatus());
            });
    }

    urlForCollection(coll: collection) {
        return appUrl.forDocuments(coll.name, this.activeDatabase());
    }

    urlForRevisionsBin() {
        return appUrl.forRevisionsBin(this.activeDatabase());
    }

    /*
    static fetchStudioConfig() {
        new getDocumentWithMetadataCommand(shell.studioConfigDocumentId, appUrl.getSystemDatabase(), true)
            .execute()
            .done((doc: documentClass) => {

            var envColor = doc && doc["EnvironmentColor"];
            if (envColor != null) {
                var color = new environmentColor(envColor.Name, envColor.BackgroundColor);
                shell.selectedEnvironmentColorStatic(color);
                shell.originalEnvironmentColor(color);
            }
        });
    }*/

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

    private handleRavenConnectionFailure(result: any) {
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

    showLicenseStatusDialog() {
        const dialog = new licensingStatus(license.licenseStatus(), license.supportCoverage());
        app.showBootstrapDialog(dialog);
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

        const env = license.licenseStatus().Type;
        const version = buildVersionResult.FullVersion;
        eventsCollector.default.initialize(
            shell.serverMainVersion() + "." + shell.serverMinorVersion(), currentBuildVersion, env, version, shouldTrack);
    }

    static openFeedbackForm() {
        const dialog = new feedback(shell.clientVersion(), shell.serverBuildVersion().FullVersion);
        app.showBootstrapDialog(dialog);
    }
    
    ignoreWebSocketError() {
        changesContext.default.serverNotifications().ignoreWebSocketConnectionError(true);
    }
}

export = shell;

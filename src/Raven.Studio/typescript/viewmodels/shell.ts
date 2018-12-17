/// <reference path="../../typings/tsd.d.ts" />

import router = require("plugins/router");
import app = require("durandal/app");
import sys = require("durandal/system");
import menu = require("common/shell/menu");
import generateMenuItems = require("common/shell/menu/generateMenuItems");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import databaseSwitcher = require("common/shell/databaseSwitcher");
import accessManager = require("common/shell/accessManager");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import favNodeBadge = require("common/shell/favNodeBadge");
import searchBox = require("common/shell/searchBox");
import database = require("models/resources/database");
import license = require("models/auth/licenseModel");
import buildInfo = require("models/resources/buildInfo");
import changesContext = require("common/changesContext");
import allRoutes = require("common/shell/routes");
import popoverUtils = require("common/popoverUtils");
import registration = require("viewmodels/shell/registration");
import collection = require("models/database/documents/collection");
import constants = require("common/constants/constants");

import appUrl = require("common/appUrl");
import autoCompleteBindingHandler = require("common/bindingHelpers/autoCompleteBindingHandler");
import helpBindingHandler = require("common/bindingHelpers/helpBindingHandler");
import messagePublisher = require("common/messagePublisher");
import extensions = require("common/extensions");
import notificationCenter = require("common/notifications/notificationCenter");

import getClientBuildVersionCommand = require("commands/database/studio/getClientBuildVersionCommand");
import getServerBuildVersionCommand = require("commands/resources/getServerBuildVersionCommand");
import getGlobalStudioConfigurationCommand = require("commands/resources/getGlobalStudioConfigurationCommand");
import getStudioConfigurationCommand = require("commands/resources/getStudioConfigurationCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import eventsCollector = require("common/eventsCollector");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import footer = require("common/shell/footer");
import feedback = require("viewmodels/shell/feedback");
import chooseTheme = require("viewmodels/shell/chooseTheme");
import continueTest = require("common/shell/continueTest");
import globalSettings = require("common/settings/globalSettings");

import protractedCommandsDetector = require("common/notifications/protractedCommandsDetector");
import requestExecution = require("common/notifications/requestExecution");
import studioSettings = require("common/settings/studioSettings");
import simpleStudioSetting = require("common/settings/simpleStudioSetting");
import clientCertificateModel = require("models/auth/clientCertificateModel");
import certificateModel = require("models/auth/certificateModel");
import serverTime = require("common/helpers/database/serverTime");
import saveGlobalStudioConfigurationCommand = require("commands/resources/saveGlobalStudioConfigurationCommand");
import saveStudioConfigurationCommand = require("commands/resources/saveStudioConfigurationCommand");
import studioSetting = require("common/settings/studioSetting");

class shell extends viewModelBase {

    private router = router;
    static studioConfigDocumentId = "Raven/StudioConfig";

    notificationCenter = notificationCenter.instance;
    collectionsTracker = collectionsTracker.default;
    footer = footer.default;
    clusterManager = clusterTopologyManager.default;
    accessManager = accessManager.default;
    continueTest = continueTest.default;
    static buildInfo = buildInfo;

    clientBuildVersion = ko.observable<clientBuildVersionDto>();

    currentRawUrl = ko.observable<string>("");
    rawUrlIsVisible = ko.computed(() => this.currentRawUrl().length > 0);
    showSplash = viewModelBase.showSplash;
    browserAlert = ko.observable<boolean>(false);
    dontShowBrowserAlertAgain = ko.observable<boolean>(false);
    currentUrlHash = ko.observable<string>(window.location.hash);

    licenseStatus = license.licenseCssClass;
    supportStatus = license.supportCssClass;
    developerLicense = license.developerLicense;
    
    clientCertificate = clientCertificateModel.certificateInfo;

    mainMenu = new menu(generateMenuItems(activeDatabaseTracker.default.database()));
    searchBox = new searchBox();
    databaseSwitcher = new databaseSwitcher();
    favNodeBadge = new favNodeBadge();
    
    static instance: shell;

    displayUsageStatsInfo = ko.observable<boolean>(false);
    trackingTask = $.Deferred<boolean>();

    studioLoadingFakeRequest: requestExecution;
    
    serverEnvironment = ko.observable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>();
    serverEnvironmentClass = database.createEnvironmentColorComputed("text", this.serverEnvironment);
    
    private onBootstrapFinishedTask = $.Deferred<void>();
    
    static showConnectionLost = ko.pureComputed(() => {
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

        autoCompleteBindingHandler.install();
        helpBindingHandler.install();

        this.clientBuildVersion.subscribe(v =>
            viewModelBase.clientVersion(v.Version));

        buildInfo.serverBuildVersion.subscribe(buildVersionDto => {
            this.initAnalytics([ buildVersionDto ]);        
        });

        activeDatabaseTracker.default.database.subscribe(newDatabase => footer.default.forDatabase(newDatabase));

        studioSettings.default.configureLoaders(() => new getGlobalStudioConfigurationCommand().execute(),
            (db) => new getStudioConfigurationCommand(db).execute(),
            settings => new saveGlobalStudioConfigurationCommand(settings).execute(),
            (settings, db) => new saveStudioConfigurationCommand(settings, db).execute()
        );
        
        this.detectBrowser();
        
        window.addEventListener("hashchange", e => {
            this.currentUrlHash(location.hash);
        });
    }
    
    // Override canActivate: we can always load this page, regardless of any system db prompt.
    canActivate(args: any): any {
        return true;
    }

    activate(args: any) {
        super.activate(args, true);

        this.fetchClientBuildVersion();
        this.fetchServerBuildVersion();

        const licenseTask = license.fetchLicenseStatus();
        const topologyTask = this.clusterManager.init();
        const clientCertificateTask = clientCertificateModel.fetchClientCertificate();
        
        licenseTask.done((result) => {
            if (result.Type !== "None") {
                license.fetchSupportCoverage();
            }
        });
        
        $.when<any>(licenseTask, topologyTask, clientCertificateTask)
            .done(([license]: [Raven.Server.Commercial.LicenseStatus], 
                   [topology]: [Raven.Server.NotificationCenter.Notifications.Server.ClusterTopologyChanged],
                   [certificate]: [Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition]) => {
            
                changesContext.default
                    .connectServerWideNotificationCenter();

                // load global settings
                studioSettings.default.globalSettings()
                    .done((settings: globalSettings) => this.onGlobalConfiguration(settings));
                studioSettings.default.registerOnSettingChangedHandler(name => true, (name: string, setting: studioSetting<any>) => {
                    // if any remote configuration was changed, then force reload
                    if (setting.saveLocation === "remote") {
                        studioSettings.default.globalSettings()
                            .done(settings => this.onGlobalConfiguration(settings));
                        
                    }
                });

                // bind event handles before we connect to server wide notification center 
                // (connection will be started after executing this method) - it was just scheduled 2 lines above
                // please notice we don't wait here for connection to be established
                // since this invocation is sync we can't end up with race condition
                this.databasesManager.setupGlobalNotifications();
                this.clusterManager.setupGlobalNotifications();
                this.notificationCenter.setupGlobalNotifications(changesContext.default.serverNotifications());
                
                const serverWideClient = changesContext.default.serverNotifications();
                serverWideClient.watchReconnect(() => studioSettings.default.globalSettings(true));

                this.connectToRavenServer();
                
                // "http"
                if (location.protocol === "http:") {
                    this.accessManager.securityClearance("ClusterAdmin");
                } else {
                    // "https"
                    if (certificate) {
                        this.accessManager.securityClearance(certificate.SecurityClearance);
                    } else {
                        this.accessManager.securityClearance("ValidUser");
                    }
                }
            })
            .then(() => this.onBootstrapFinishedTask.resolve(), () => this.onBootstrapFinishedTask.reject());

        this.setupRouting();
        
        // we await here only for certificate task, as downloading license can take longer
        return clientCertificateTask;
    }
    
    private onGlobalConfiguration(settings: globalSettings) {
        if (!settings.disabled.getValue()) {
            const envValue = settings.environment.getValue();
            if (envValue && envValue !== "None") {
                this.serverEnvironment(envValue);
            } else {
                this.serverEnvironment(null);
            }
        }
    }
    
    private setupRouting() {
        const routes = allRoutes.get(this.appUrls);
        routes.push(...routes);
        router.map(routes).buildNavigationModel();

        appUrl.mapUnknownRoutes(router);
    }

    attached() {
        super.attached();

        if (this.clientCertificate() && this.clientCertificate().Name) {
            
            const dbAccess = certificateModel.resolveDatabasesAccess(this.clientCertificate())
                .map(x => `<div>${x}</div>`)
                .join("");
            
            popoverUtils.longWithHover($(".js-client-cert"),
            {
                content: `<dl class="dl-horizontal margin-none client-certificate-info">
                            <dt>Client Certificate</dt>
                            <dd><strong>${this.clientCertificate().Name}</strong></dd>
                            <dt>Thumbprint</dt>
                            <dd><strong>${this.clientCertificate().Thumbprint}</strong></dd>
                            <dt><span>Security Clearance</span></dt>
                            <dd><strong>${certificateModel.clearanceLabelFor(this.clientCertificate().SecurityClearance)}</strong></dd>
                            <dt><span>Access to databases:</span></dt>
                            <dd><strong>${dbAccess}</strong></dd>
                          </dl>`
                ,
                placement: 'top'
            });    
        }
        
        sys.error = (e: any) => {
            console.error(e);
            messagePublisher.reportWarning("Failed to load routed module!", e);
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
        
        notificationCenter.instance.initialize();
    }

    compositionComplete() {
        super.compositionComplete();
        $("body").removeClass('loading-active');
        $(".loading-overlay").remove();

        this.studioLoadingFakeRequest.markCompleted();
        this.studioLoadingFakeRequest = null;
        
        this.initializeShellComponents();

        this.onBootstrapFinishedTask
            .done(() => {
                registration.showRegistrationDialogIfNeeded(license.licenseStatus());
                this.tryReopenRegistrationDialog();
            });
    }

    private tryReopenRegistrationDialog() {
        const random = Math.random() * 5;
        setTimeout(() => {
            registration.showRegistrationDialogIfNeeded(license.licenseStatus(), true);
            this.tryReopenRegistrationDialog();
        }, random * 1000 * 60);
    }

    urlForCollection(coll: collection) {
        return appUrl.forDocuments(coll.name, this.activeDatabase());
    }

    urlForRevisionsBin() {
        return appUrl.forRevisionsBin(this.activeDatabase());
    }
    
    urlForCertificates() {
        return appUrl.forCertificates();
    }

    private getIndexingDisbaledValue(indexingDisabledString: string) {
        if (indexingDisabledString === undefined || indexingDisabledString == null)
            return false;

        if (indexingDisabledString.toLowerCase() === "true")
            return true;

        return false;
    }

    connectToRavenServer() {
        return this.databasesManager.init();
    }

    fetchServerBuildVersion() {
        new getServerBuildVersionCommand()
            .execute()
            .done((serverBuildResult: serverBuildVersionDto, status: string,  response: JQueryXHR) => {            
               
                serverTime.default.calcTimeDifference(response.getResponseHeader("Date"));
                serverTime.default.setStartUpTime(response.getResponseHeader("Server-Startup-Time"));
                
                buildInfo.serverBuildVersion(serverBuildResult);

                const currentBuildVersion = serverBuildResult.BuildVersion;
                if (currentBuildVersion !== constants.DEV_BUILD_NUMBER) {
                    buildInfo.serverMainVersion(Math.floor(currentBuildVersion / 10000));
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

    navigateToClusterSettings() {
        this.navigate(this.appUrls.adminSettingsCluster());
    }

    private initAnalytics(buildVersionResult: [serverBuildVersionDto]) {
        if (eventsCollector.gaDefined()) {
            
            studioSettings.default.globalSettings()
                .done(settings => {
                    const shouldTraceUsageMetrics = settings.sendUsageStats.getValue();
                    if (_.isUndefined(shouldTraceUsageMetrics)) {
                        // ask user about GA
                        this.displayUsageStatsInfo(true);
        
                        this.trackingTask.done((accepted: boolean) => {
                            this.displayUsageStatsInfo(false);
        
                            if (accepted) {
                                this.configureAnalytics(true, buildVersionResult);
                            }
                            
                            settings.sendUsageStats.setValue(accepted);
                        });
                    } else {
                        this.configureAnalytics(shouldTraceUsageMetrics, buildVersionResult);
                    }
            });
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
        const shouldTrack = track && currentBuildVersion !== constants.DEV_BUILD_NUMBER;
        if (currentBuildVersion !== constants.DEV_BUILD_NUMBER) {
            buildInfo.serverMainVersion(Math.floor(currentBuildVersion / 10000));
        }

        const licenseStatus = license.licenseStatus();
        const env = licenseStatus ? licenseStatus.Type : "N/A";
        const version = buildVersionResult.FullVersion;
        eventsCollector.default.initialize(
            buildInfo.serverMainVersion() + "." + buildInfo.serverMinorVersion(), currentBuildVersion, env, version, shouldTrack);
        
        studioSettings.default.registerOnSettingChangedHandler(
            name => name === "sendUsageStats",
            (name, track: simpleStudioSetting<boolean>) => eventsCollector.default.enabled = track.getValue() && eventsCollector.gaDefined());
    }

    static openFeedbackForm() {
        const dialog = new feedback(shell.clientVersion(), buildInfo.serverBuildVersion().FullVersion);
        app.showBootstrapDialog(dialog);
    }
    
    static chooseTheme() {
        const dialog = new chooseTheme();
        app.showBootstrapDialog(dialog);
    }
    
    ignoreWebSocketError() {
        changesContext.default.serverNotifications().ignoreWebSocketConnectionError(true);
    }
    
    detectBrowser() {
        const isChrome = /Chrome/.test(navigator.userAgent) && /Google Inc/.test(navigator.vendor);
        const isFirefox = navigator.userAgent.toLowerCase().indexOf('firefox') > -1;

        if (!isChrome && !isFirefox) {
            // it isn't supported browser, check if user already said: Don't show this again

            studioSettings.default.globalSettings()
                .done(settings => {
                    if (settings.dontShowAgain.shouldShow("UnsupportedBrowser")) {
                        this.browserAlert(true);
                    }
                });
        }
    }

    browserAlertContinue() {
        const dontShowAgain = this.dontShowBrowserAlertAgain();
        
        if (dontShowAgain) {
            studioSettings.default.globalSettings()
                .done(settings => {
                    settings.dontShowAgain.ignore("UnsupportedBrowser");
                });
        }
        
        this.browserAlert(false);
    }
    
    createUrlWithHashComputed(serverUrlProvider: KnockoutComputed<string>) {
        return ko.pureComputed(() => {
            const serverUrl = serverUrlProvider();
            const hash = this.currentUrlHash();
            
            if (!serverUrl) {
                return "#";
            }
            
            return serverUrl + "/studio/index.html" + hash;
        })
    }
}

export = shell;

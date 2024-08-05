/// <reference path="../../typings/tsd.d.ts" />

import router = require("plugins/router");
import app = require("durandal/app");
import sys = require("durandal/system");
import menu = require("common/shell/menu");
import generateMenuItems = require("common/shell/menu/generateMenuItems");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import accessManager = require("common/shell/accessManager");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import favNodeBadge = require("common/shell/favNodeBadge");
import database = require("models/resources/database");
import license = require("models/auth/licenseModel");
import buildInfo = require("models/resources/buildInfo");
import changesContext = require("common/changesContext");
import allRoutes = require("common/shell/routes");
import popoverUtils = require("common/popoverUtils");
import registration = require("viewmodels/shell/registration");
import collection = require("models/database/documents/collection");

import appUrl = require("common/appUrl");
import autoCompleteBindingHandler = require("common/bindingHelpers/autoCompleteBindingHandler");
import helpBindingHandler = require("common/bindingHelpers/helpBindingHandler");
import messagePublisher = require("common/messagePublisher");
import extensions = require("common/extensions");
import notificationCenter = require("common/notifications/notificationCenter");

import getClientBuildVersionCommand = require("commands/database/studio/getClientBuildVersionCommand");
import getServerBuildVersionCommand = require("commands/resources/getServerBuildVersionCommand");
import getGlobalStudioConfigurationCommand = require("commands/resources/getGlobalStudioConfigurationCommand");
import getDatabaseStudioConfigurationCommand = require("commands/resources/getDatabaseStudioConfigurationCommand");
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
import saveDatabaseStudioConfigurationCommand = require("commands/resources/saveDatabaseStudioConfigurationCommand");
import studioSetting = require("common/settings/studioSetting");
import detectBrowser = require("viewmodels/common/detectBrowser");
import genUtils = require("common/generalUtils");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import connectionStatus from "models/resources/connectionStatus";
import shard from "models/resources/shard";
import moment from "moment";
import databasesManager from "common/shell/databasesManager";
import { globalDispatch } from "components/storeCompat";
import { accessManagerActions } from "components/common/shell/accessManagerSlice";
import UpgradeModal from "./shell/UpgradeModal";
import getStudioBootstrapCommand from "commands/resources/getStudioBootstrapCommand";
import serverSettings from "common/settings/serverSettings";
import StudioSearchWithDatabaseSwitcher from "components/shell/studioSearchWithDatabaseSelector/StudioSearchWithDatabaseSwitcher";

class shell extends viewModelBase {

    private router = router;
    view = require("views/shell.html");
    usageStatsView = require("views/usageStats.html");
    
    notificationCenterView = require("views/notifications/notificationCenter.html");
    graphHelperView = require("views/common/graphHelper.html");

    static studioConfigDocumentId = "Raven/StudioConfig";
    
    static showConnectionLost = connectionStatus.showConnectionLost;

    notificationCenter = notificationCenter.instance;
    
    collectionsTracker = collectionsTracker.default;
    collectionsCountText: KnockoutObservable<string>;
    
    footer = footer.default;
    clusterManager = clusterTopologyManager.default;
    accessManager = accessManager.default;
    continueTest = continueTest.default;
    static buildInfo = buildInfo;
    
    clientBuildVersion = ko.observable<clientBuildVersionDto>();

    currentRawUrl = ko.observable<string>("");
    rawUrlIsVisible = ko.computed(() => this.currentRawUrl().length > 0);
    showSplash = viewModelBase.showSplash;
    
    browserAlert: detectBrowser;
    collapseMenu = ko.observable<boolean>(false);
    currentUrlHash = ko.observable<string>(window.location.hash);

    licenseStatus = license.licenseCssClass;
    supportStatus = license.supportCssClass;
    developerLicense = license.developerLicense;
    
    cloudClusterAdmin: KnockoutObservable<boolean>;
    colorCustomizationDisabled = ko.observable<boolean>(false);
    applyColorCustomization: KnockoutObservable<boolean>;
    singleShardName: KnockoutObservable<string>;
    allShardsUrl: KnockoutObservable<string>;
    clientCertificate = clientCertificateModel.certificateInfo;
    certificateExpirationState = clientCertificateModel.certificateExpirationState;
    
    mainMenu = new menu(generateMenuItems(activeDatabaseTracker.default.database()));
    favNodeBadge = new favNodeBadge();

    smallScreen = ko.observable<boolean>(false);
    
    static instance: shell;

    displayUsageStatsInfo = ko.observable<boolean>(false);
    trackingTask = $.Deferred<boolean>();

    studioLoadingFakeRequest: requestExecution;
    
    serverEnvironment = ko.observable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>();
    serverEnvironmentClass: KnockoutComputed<string>;
    serverEnvironmentShortText: KnockoutComputed<string>;

    private onBootstrapFinishedTask = $.Deferred<void>();

    upgradeModalView: ReactInKnockout<typeof UpgradeModal>;
    isUpgradeModalVisible = ko.observable<boolean>(false);

    studioSearchWithDatabaseSwitcherView: ReactInKnockout<typeof StudioSearchWithDatabaseSwitcher>;

    isEnabled = ko.observable<boolean>(false);
    
    constructor() {
        super();
        
        shell.instance = this;

        this.studioLoadingFakeRequest = protractedCommandsDetector.instance.requestStarted(0);

        extensions.install();

        autoCompleteBindingHandler.install();
        helpBindingHandler.install();

        this.collectionsCountText = ko.pureComputed(() => {
            // One collection is 'All Documents' - so we must subtract 1 ...
            // Value shows in UI only if there is at least 1 collection other than 'All Documents' 
            return (this.collectionsTracker.collections().length - 1).toLocaleString();
        });
        
        this.clientBuildVersion.subscribe(v =>
            viewModelBase.clientVersion(v.Version));

        activeDatabaseTracker.default.database.subscribe(newDatabase => footer.default.forDatabase(newDatabase));

        studioSettings.default.configureLoaders(() => new getGlobalStudioConfigurationCommand().execute(),
            (db) => new getDatabaseStudioConfigurationCommand(db).execute(),
            settings => new saveGlobalStudioConfigurationCommand(settings).execute(),
            (settings, db) => new saveDatabaseStudioConfigurationCommand(settings, db).execute()
        );
        
        this.browserAlert = new detectBrowser(true);
        
        window.addEventListener("hashchange", () => {
            this.currentUrlHash(location.hash);
        });
        
        this.cloudClusterAdmin = ko.pureComputed(() => {
            const isCloud = license.cloudLicense();
            const isClusterAdmin = this.accessManager.securityClearance() === "ClusterAdmin";
            return isCloud && isClusterAdmin;
        });
        
        this.applyColorCustomization = ko.pureComputed(() => {
            const cloudClusterAdmin = this.cloudClusterAdmin();
            const disableColors = this.colorCustomizationDisabled();
            
            return !disableColors && cloudClusterAdmin;
        });
        
        this.singleShardName = ko.pureComputed(() => {
            
            // just to register subscription and be able to watch db in url
            router.activeInstruction();
            
            const dbInUrl = appUrl.getDatabaseNameFromUrl();

            if (!dbInUrl) {
                return null;
            }
            
            const db = this.activeDatabase();
            if (!db) {
                return null;
            }
            
            const isShard = db instanceof shard;
            return isShard ? db.shardName : null;
        });
        
        this.allShardsUrl = ko.pureComputed(() => {
            const db = this.activeDatabase();
            if (!db || !(db instanceof shard)) {
                return "";
            }
            const shardedDb = db.root;
            
            const localUrl = appUrl.forCurrentPage(shardedDb);
            
            const firstNode = shardedDb.nodes()[0];

            return shardedDb.relevant() ? localUrl : appUrl.toExternalUrl(firstNode.nodeUrl, localUrl);
        });

        this.bindToCurrentInstance("toggleMenu");

        this.upgradeModalView = ko.pureComputed(() => ({ component: UpgradeModal }))
        
        this.studioSearchWithDatabaseSwitcherView = ko.computed(() => {
            return {
                component: StudioSearchWithDatabaseSwitcher,
                props: {
                    menuItems: this.mainMenu.items(),
                    isEnabled: this.isEnabled(),
                },
            };
        });

        this.mainMenu.items.subscribe(items => {
            this.studioSearchWithDatabaseSwitcherView().props= {
                getMenuItems: () => items,
            };
        });

        this.serverEnvironmentClass = ko.pureComputed(() => {
            const env = this.serverEnvironment();
            
            switch (env) {
                case "Production":
                    return "bg-danger";
                case "Testing":
                    return "bg-success";
                case "Development":
                    return "bg-info";
                default:
                    return "";
            }
        })

        this.serverEnvironmentShortText = ko.pureComputed(() => {
            const env = this.serverEnvironment();
            
            switch (env) {
                case "Production":
                    return "PROD";
                case "Testing":
                    return "TEST";
                case "Development":
                    return "DEV";
                default:
                    return "";
            }
        });
    }
    
    // Override canActivate: we can always load this page, regardless of any system db prompt.
    canActivate(): any {
        return true;
    }

    activate(args: any) {
        super.activate(args, { shell: true });

        this.fetchClientBuildVersion();
        const buildVersionTask = this.fetchServerBuildVersion();

        const licenseTask = license.fetchLicenseStatus();
        const topologyTask = this.clusterManager.init();
        const clientCertificateTask = clientCertificateModel.fetchClientCertificate();
        const studioBootstrapTask = new getStudioBootstrapCommand().execute();
        
        licenseTask.done((result) => {
            if (result.Type !== "None") {
                license.fetchSupportCoverage();
            }
        });
        
        $.when<any>(buildVersionTask, licenseTask)
            .done(() => {
                this.initAnalytics();
            });
        
        $.when<any>(licenseTask, topologyTask, clientCertificateTask, studioBootstrapTask)
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            .done(([license]: [LicenseStatus], 
                   // eslint-disable-next-line @typescript-eslint/no-unused-vars
                   [topology]: [Raven.Server.NotificationCenter.Notifications.Server.ClusterTopologyChanged],
                   [certificate]: [Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition], 
                   [studioConfig]: [Raven.Server.Web.Studio.StudioTasksHandler.StudioBootstrapConfiguration]) => {
            
                changesContext.default
                    .connectServerWideNotificationCenter();

                serverSettings.default.onConfigLoaded(studioConfig);
                
                // load global settings
                studioSettings.default.globalSettings()
                    .done((settings: globalSettings) => this.onGlobalConfiguration(settings));
                
                studioSettings.default.registerOnSettingChangedHandler(() => true, (name: string, setting: studioSetting<any>) => {
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
                databasesManager.default.setupGlobalNotifications();
                this.clusterManager.setupGlobalNotifications();
                this.notificationCenter.setupGlobalNotifications(changesContext.default.serverNotifications());
                
                const serverWideClient = changesContext.default.serverNotifications();
                serverWideClient.watchReconnect(() => studioSettings.default.globalSettings(true));

                this.connectToRavenServer();
                
                // "http"
                if (location.protocol === "http:") {
                    this.accessManager.securityClearance("ClusterAdmin");
                    this.accessManager.secureServer(false);
                } else {
                    // "https"
                    if (certificate) {
                        this.accessManager.securityClearance(certificate.SecurityClearance);
                        accessManager.clientCertificateThumbprint(certificate.Thumbprint);

                        const databasesAccess: dictionary<databaseAccessLevel> = {};
                        for (const key in certificate.Permissions) {
                            const access = certificate.Permissions[key];
                            databasesAccess[`${key}`] = `Database${access}` as databaseAccessLevel;
                        }
                        accessManager.databasesAccess = databasesAccess;
                        globalDispatch(accessManagerActions.onDatabaseAccessLoaded(databasesAccess));
                        this.accessManager.secureServer(true);
                        
                    } else {
                        this.accessManager.securityClearance("ValidUser");
                        this.accessManager.secureServer(false);
                    }
                }
            })
            .then(() => this.onBootstrapFinishedTask.resolve(), () => this.onBootstrapFinishedTask.reject());

        this.setupRouting();
        
        // we await here only for certificate task, as downloading license can take longer
        return clientCertificateTask;
    }

    toggleMenu() {
        this.collapseMenu.toggle();
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
        const routes = allRoutes.get();
        routes.push(...routes);
        router.map(routes).buildNavigationModel();

        appUrl.mapUnknownRoutes(router);
    }

    attached() {
        super.attached();

        if (this.clientCertificate() && this.clientCertificate().Name) {
            const tooltipProvider = () => {
                const dbAccessArray = certificateModel.resolveDatabasesAccess(this.clientCertificate());

                const allowedDatabasesText = dbAccessArray.length ?
                    dbAccessArray.map(x => `<div>
                                            <strong>${genUtils.escapeHtml(x.dbName)}</strong>
                                            <span class="${this.accessManager.getAccessColor(x.accessLevel)} margin-left">
                                                         ${accessManager.default.getAccessLevelText(x.accessLevel)}
                                            </span>
                                        </div>`).join("")
                    : "No access granted";

                const notAfter = this.clientCertificate().NotAfter;
                const notAfterUtc = moment(notAfter).utc();

                const expirationDate = notAfter ? `${notAfter.substring(0, 10)} <span class="${this.getExpirationDurationClass()}">(${genUtils.formatDurationByDate(notAfterUtc, true)})</span>` : "n/a";

                const twoFactorPart = this.footer.twoFactorSessionExpiration() ?
                    `<dt>2FA Session Expiration:</dt>
                      <dd><strong>${this.footer.twoFactorSessionExpiration().local().format("YYYY-MM-DD HH:mm:ss")}
                       (${genUtils.formatDurationByDate(this.footer.twoFactorSessionExpiration(), true)})</strong></dd>`
                    : "";

                return `<dl class="dl-horizontal margin-none client-certificate-info">
                            <dt>Client Certificate</dt>
                            <dd><strong>${this.clientCertificate().Name}</strong></dd>
                            <dt>Expiration Date</dt>
                            <dd><strong>${expirationDate}</strong></dd>
                            <dt>Thumbprint</dt>
                            <dd><strong>${this.clientCertificate().Thumbprint}</strong></dd>
                            <dt><span>Security Clearance</span></dt>
                            <dd><strong>${certificateModel.clearanceLabelFor(this.clientCertificate().SecurityClearance)}</strong></dd>
                            <dt><span>Access to databases:</span></dt>
                            <dd><span>${allowedDatabasesText}</span></dd>
                            ${twoFactorPart}
                          </dl>`;
            }
            
            popoverUtils.longWithHover($(".js-client-cert"),
                {
                    content: tooltipProvider,
                    placement: 'top',
                    sanitize: false
                } as PopoverOptions);
        }
        
        if (!this.clientCertificate()) {
            const authenticationInfo = "No authentication is set. Running in an unsecure mode.";
            
            popoverUtils.longWithHover($(".js-client-cert"),
                {
                    content: authenticationInfo,
                    placement: 'top'
                });
        }
        
        sys.error = (e: any) => {
            console.error(e);
            messagePublisher.reportWarning("Failed to load routed module!", e);
        };
    }
    
    private getExpirationDurationClass() {
        switch (this.certificateExpirationState()) {
            case "expired":
                return "text-danger";
            case "aboutToExpire":
                return "text-warning";
            default:
                return "text-success";
        }
    }

    private initializeShellComponents() {
        this.mainMenu.initialize();
        const updateMenu = (db: database) => {
            const items = generateMenuItems(db);
            this.mainMenu.update(items);
        };
        
        const checkScreenSize = () => {
            if ($(window).width() < 992) {
                this.smallScreen(true);
            } else {
                this.smallScreen(false);
            }
        }

        checkScreenSize();

        $(window).resize(checkScreenSize);

        updateMenu(activeDatabaseTracker.default.database());
        activeDatabaseTracker.default.database.subscribe(updateMenu);

        this.favNodeBadge.initialize(); 
        
        notificationCenter.instance.initialize();
    }

    compositionComplete() {
        super.compositionComplete();
        $("body").removeClass('loading-active');
        $(".splash-screen").remove();

        this.studioLoadingFakeRequest.markCompleted();
        this.studioLoadingFakeRequest = null;
        
        this.initializeShellComponents();

        this.onBootstrapFinishedTask
            .done(() => {
                if (license.getStatusValue("UpgradeRequired")) {
                    this.isUpgradeModalVisible(true);
                    return;
                }

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
    
    urlForCluster() {
        return appUrl.forCluster();
    }

    connectToRavenServer() {
        return databasesManager.default.init();
    }

    fetchServerBuildVersion(): JQueryPromise<serverBuildVersionDto> {
        return new getServerBuildVersionCommand()
            .execute()
            .done((serverBuildResult: serverBuildVersionDto, status: any, response: any) => {
                serverTime.default.calcTimeDifference(response.getResponseHeader("Date"));
                serverTime.default.setStartUpTime(response.getResponseHeader("Server-Startup-Time"));
                
                buildInfo.onServerBuildVersion(serverBuildResult);
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

    private initAnalytics() {
        if (eventsCollector.gaDefined()) {
            
            studioSettings.default.globalSettings()
                .done(settings => {
                    const shouldTraceUsageMetrics = settings.sendUsageStats.getValue();
                    if (shouldTraceUsageMetrics === undefined) {
                        // using location.hash instead of shell activation data - which is not available in shell activate method
                        const suppressTraceUsage = window.location.hash ? window.location.hash.includes("disableAnalytics=true") : false; 
                        
                        if (suppressTraceUsage) {
                            // persist forced option
                            settings.sendUsageStats.setValue(false);
                        } else {
                            // ask user about GA
                            this.displayUsageStatsInfo(true);

                            this.trackingTask.done((accepted: boolean) => {
                                this.displayUsageStatsInfo(false);

                                if (accepted) {
                                    this.configureAnalytics(true);
                                }

                                settings.sendUsageStats.setValue(accepted);
                            });
                        }
                    } else {
                        this.configureAnalytics(shouldTraceUsageMetrics);
                    }
            });
        } else {
            // user has uBlock etc?
            this.configureAnalytics(false);
        }
    }

    collectUsageData() {
        this.trackingTask.resolve(true);
    }

    doNotCollectUsageData() {
        this.trackingTask.resolve(false);
    }

    private configureAnalytics(track: boolean) {
        const currentBuildVersion = buildInfo.serverBuildVersion().BuildVersion;
        const shouldTrack = track && !buildInfo.isDevVersion();

        const licenseStatus = license.licenseStatus();
        const env = licenseStatus ? licenseStatus.Type : "N/A";
        const fullVersion = buildInfo.serverBuildVersion().FullVersion;
        eventsCollector.default.initialize(buildInfo.mainVersion(), currentBuildVersion, env, fullVersion, shouldTrack);
        
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

    disableColorCustomization() {
        this.colorCustomizationDisabled(true);
    }
    
    disableReason(menuItem: leafMenuItem) {
        return ko.pureComputed<string>(() => {
            const requiredAccess = menuItem.requiredAccess;
            if (!requiredAccess) {
                return "";
            }

            const activeDatabase = activeDatabaseTracker.default.database();
            
            const canHandleOperation = accessManager.canHandleOperation(requiredAccess, activeDatabase?.name);
                      
            return canHandleOperation ? "" : accessManager.getDisableReasonHtml(requiredAccess);
        })
    }
}

export = shell;

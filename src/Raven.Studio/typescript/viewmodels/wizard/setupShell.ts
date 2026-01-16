/// <reference path="../../../typings/tsd.d.ts" />
import router = require("plugins/router");
import sys = require("durandal/system");
import getClientBuildVersionCommand = require("commands/database/studio/getClientBuildVersionCommand");
import getServerBuildVersionCommand = require("commands/resources/getServerBuildVersionCommand");
import messagePublisher = require("common/messagePublisher");
import extensions = require("common/extensions");
import viewModelBase = require("viewmodels/viewModelBase");
import autoCompleteBindingHandler = require("common/bindingHelpers/autoCompleteBindingHandler");
import requestExecution = require("common/notifications/requestExecution");
import protractedCommandsDetector = require("common/notifications/protractedCommandsDetector");
import buildInfo = require("models/resources/buildInfo");
import chooseTheme = require("viewmodels/shell/chooseTheme");
import app = require("durandal/app");
import serverSetup = require("models/wizard/serverSetup");
import SetupWizard = require("components/setupWizard/SetupWizard");
import studioSettings = require("common/settings/studioSettings");
import eventsCollector = require("common/eventsCollector");
import simpleStudioSetting = require("common/settings/simpleStudioSetting");
import license = require("models/auth/licenseModel");
import getGlobalStudioConfigurationCommand = require("commands/resources/getGlobalStudioConfigurationCommand");
import getDatabaseStudioConfigurationCommand = require("commands/resources/getDatabaseStudioConfigurationCommand");
import saveGlobalStudioConfigurationCommand = require("commands/resources/saveGlobalStudioConfigurationCommand");
import saveDatabaseStudioConfigurationCommand = require("commands/resources/saveDatabaseStudioConfigurationCommand");

class setupShell extends viewModelBase {

    view = require("views/wizard/setupShell.html");
    usageStatsView = require("views/usageStats.html");

    private router = router;
    studioLoadingFakeRequest: requestExecution;
    clientBuildVersion = ko.observable<clientBuildVersionDto>();
    static deploymentEnvironment = serverSetup.deploymentEnvironment;
    static buildInfo = buildInfo;

    showSplash = viewModelBase.showSplash;

    displayUsageStatsInfo = ko.observable<boolean>(false);
    trackingTask = $.Deferred<boolean>();
    serverEnvironment = ko.observable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>();

    setupWizardView: ReactInKnockout<typeof SetupWizard.default>;
    theme: chooseTheme = new chooseTheme();

    constructor() {
        super();

        autoCompleteBindingHandler.install();
        this.theme.useTheme(chooseTheme.defaultTheme) // in new setup wizard we want to use default theme.

        this.studioLoadingFakeRequest = protractedCommandsDetector.instance.requestStarted(0);
        
        extensions.install();
 
        studioSettings.default.configureLoaders(() => new getGlobalStudioConfigurationCommand().execute(),
            (db) => new getDatabaseStudioConfigurationCommand(db).execute(),
            settings => new saveGlobalStudioConfigurationCommand(settings).execute(),
            (settings, db) => new saveDatabaseStudioConfigurationCommand(settings, db).execute()
        )

        this.setupWizardView = ko.pureComputed(() => ({
            component: SetupWizard.default
        }))
    }
    
    private initAnalytics() {
        if (buildInfo.isDevVersion()) {
            // don't track dev versions
            return;
        }

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
    }

    collectUsageData() {
        this.trackingTask.resolve(true);
    }

    doNotCollectUsageData() {
        this.trackingTask.resolve(false);
    }

    private configureAnalytics(shouldTrack: boolean) {
        const serverBuildVersion = buildInfo.serverBuildVersion();
        const currentBuildVersion = serverBuildVersion.BuildVersion;
        const fullVersion = serverBuildVersion.FullVersion;

        eventsCollector.default.initialize(buildInfo.mainVersion(),
            currentBuildVersion,
            this.serverEnvironment(),
            fullVersion,
            license.licenseStatus,
            license.supportCoverage,
            shouldTrack);

        studioSettings.default.registerOnSettingChangedHandler(
            name => name === "sendUsageStats",
            (name, track: simpleStudioSetting<boolean>) => eventsCollector.default.setEnabled(track.getValue()));
    }


    // Override canActivate: we can always load this page, regardless of any system db prompt.
    canActivate(): any {
        return true;
    }

    activate(args: any) {
        super.activate(args, { shell: true });

        // return this.router.activate()
        //     .then(() => {
        //         this.fetchClientBuildVersion();
        //         this.fetchServerBuildVersion();
        //     })
    }

    private fetchServerBuildVersion() {
        new getServerBuildVersionCommand()
            .execute()
            .done((serverBuildResult: serverBuildVersionDto) => {
                buildInfo.onServerBuildVersion(serverBuildResult);
                this.initAnalytics();
            });
    }

    private fetchClientBuildVersion() {
        new getClientBuildVersionCommand()
            .execute()
            .done((result: clientBuildVersionDto) => {
                this.clientBuildVersion(result);
                viewModelBase.clientVersion(result.Version);
            });
    }

    attached() {
        super.attached();

        sys.error = (e: any) => {
            console.error(e);
            messagePublisher.reportError("Failed to load routed module!", e);
        };
    }

    compositionComplete() {
        super.compositionComplete();
        $("body")
            .removeClass('loading-active')
            .addClass("setup-shell");
        $(".splash-screen").remove();

        this.studioLoadingFakeRequest.markCompleted();
        this.studioLoadingFakeRequest = null;
    }

    static chooseTheme() {
        const dialog = new chooseTheme();
        app.showBootstrapDialog(dialog);
    }
}

export = setupShell;

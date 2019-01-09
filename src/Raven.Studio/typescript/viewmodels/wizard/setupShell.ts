/// <reference path="../../../typings/tsd.d.ts" />
import router = require("plugins/router");
import sys = require("durandal/system");
import setupRoutes = require("common/setup/routes");
import getClientBuildVersionCommand = require("commands/database/studio/getClientBuildVersionCommand");
import getServerBuildVersionCommand = require("commands/resources/getServerBuildVersionCommand");
import messagePublisher = require("common/messagePublisher");
import extensions = require("common/extensions");
import viewModelBase = require("viewmodels/viewModelBase");
import autoCompleteBindingHandler = require("common/bindingHelpers/autoCompleteBindingHandler");
import requestExecution = require("common/notifications/requestExecution");
import protractedCommandsDetector = require("common/notifications/protractedCommandsDetector");
import buildInfo = require("models/resources/buildInfo");
import constants = require("common/constants/constants");
import chooseTheme = require("viewmodels/shell/chooseTheme");
import app = require("durandal/app");

type deploymentEnvironment = "AwsLinux" | "AwsWindows" | "Azure" | "Custom";

class setupShell extends viewModelBase {

    private router = router;
    studioLoadingFakeRequest: requestExecution;
    clientBuildVersion = ko.observable<clientBuildVersionDto>();
    static deploymentEnvironment = ko.observable<deploymentEnvironment>("Custom");
    static buildInfo = buildInfo;

    showSplash = viewModelBase.showSplash;

    constructor() {
        super();

        autoCompleteBindingHandler.install();

        this.studioLoadingFakeRequest = protractedCommandsDetector.instance.requestStarted(0);
        
        extensions.install();
    }

    // Override canActivate: we can always load this page, regardless of any system db prompt.
    canActivate(args: any): any {
        return true;
    }

    activate(args: any) {
        super.activate(args, true);

        this.setupRouting();
        
        return this.router.activate()
            .then(() => {
                this.fetchClientBuildVersion();
                this.fetchServerBuildVersion();
            })
    }

    private fetchServerBuildVersion() {
        new getServerBuildVersionCommand()
            .execute()
            .done((serverBuildResult: serverBuildVersionDto) => {
                buildInfo.serverBuildVersion(serverBuildResult);

                const currentBuildVersion = serverBuildResult.BuildVersion;
                if (currentBuildVersion !== constants.DEV_BUILD_NUMBER) {
                    buildInfo.serverMainVersion(Math.floor(currentBuildVersion / 10000));
                }
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

    private setupRouting() {
        router.map(setupRoutes.get()).buildNavigationModel();

        router.mapUnknownRoutes((instruction: DurandalRouteInstruction) => {
            const queryString = !!instruction.queryString ? ("?" + instruction.queryString) : "";

            messagePublisher.reportError("Unknown route", "The route " + instruction.fragment + queryString + " doesn't exist, redirecting...");

            location.href = "#welcome";
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
        $(".loading-overlay").remove();

        this.studioLoadingFakeRequest.markCompleted();
        this.studioLoadingFakeRequest = null;
    }

    static chooseTheme() {
        const dialog = new chooseTheme();
        app.showBootstrapDialog(dialog);
    }
}

export = setupShell;

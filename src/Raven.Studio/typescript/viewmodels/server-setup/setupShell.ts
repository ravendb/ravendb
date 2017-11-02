/// <reference path="../../../typings/tsd.d.ts" />

import router = require("plugins/router");
import sys = require("durandal/system");
import setupRoutes = require("common/setup/routes");
import appUrl = require("common/appUrl");
import dynamicHeightBindingHandler = require("common/bindingHelpers/dynamicHeightBindingHandler");
import autoCompleteBindingHandler = require("common/bindingHelpers/autoCompleteBindingHandler");
import helpBindingHandler = require("common/bindingHelpers/helpBindingHandler");
import messagePublisher = require("common/messagePublisher");
import extensions = require("common/extensions");
import viewModelBase = require("viewmodels/viewModelBase");
import requestExecution = require("common/notifications/requestExecution");
import protractedCommandsDetector = require("common/notifications/protractedCommandsDetector");

class setupShell extends viewModelBase {

    private router = router;
    studioLoadingFakeRequest: requestExecution;

    showSplash = viewModelBase.showSplash;

    constructor() {
        super();

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
        
        return this.router.activate();
    }

    private setupRouting() {
        router.map(setupRoutes.get()).buildNavigationModel();

        appUrl.mapUnknownRoutes(router);
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
        $("body").removeClass('loading-active');

        this.studioLoadingFakeRequest.markCompleted();
        this.studioLoadingFakeRequest = null;
    }
}

export = setupShell;

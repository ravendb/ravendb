import viewModelBase = require("viewmodels/viewModelBase");
import endpoints = require("endpoints");
import appUrl = require("common/appUrl");
import eventsCollector = require("common/eventsCollector");

class infoPackage extends viewModelBase {

    downloadServerWidePackage() {
        eventsCollector.default.reportEvent("info-package", "server-wide");
        this.startDownload(endpoints.global.serverWideDebugInfoPackage.adminDebugInfoPackage);
    }

    downloadClusterWidePackage() {
        eventsCollector.default.reportEvent("info-package", "cluster-wide");
        this.startDownload(endpoints.global.serverWideDebugInfoPackage.adminDebugClusterInfoPackage);
    }

    private startDownload(url: string) {
        const $form = $("#downloadInfoPackageForm");
        $form.attr("action", appUrl.baseUrl + url);
        $form.submit();
    }
}

export = infoPackage;

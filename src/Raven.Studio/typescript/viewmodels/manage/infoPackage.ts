import viewModelBase = require("viewmodels/viewModelBase");
import endpoints = require("endpoints");
import appUrl = require("common/appUrl");

class infoPackage extends viewModelBase {

    downloadServerWidePackage() {
        this.startDownload(endpoints.global.serverWideDebugInfoPackage.adminDebugInfoPackage);
    }

    downloadClusterWidePackage() {
        this.startDownload(endpoints.global.serverWideDebugInfoPackage.adminDebugClusterInfoPackage);
    }

    private startDownload(url: string) {
        const $form = $("#downloadInfoPackageForm");
        $form.attr("action", appUrl.baseUrl + url);
        $form.submit();
    }
}

export = infoPackage;

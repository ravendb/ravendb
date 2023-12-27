/// <reference path="../../../typings/tsd.d.ts" />

class serverSettings {

    static default = new serverSettings();

    certificateExpiringThresholdInDays = ko.observable<number>(14);
 
    onConfigLoaded(dto: Raven.Server.Web.Studio.StudioTasksHandler.StudioBootstrapConfiguration) {
        this.certificateExpiringThresholdInDays(dto.CertificateExpiringThresholdInDays);
    }
}

export = serverSettings;

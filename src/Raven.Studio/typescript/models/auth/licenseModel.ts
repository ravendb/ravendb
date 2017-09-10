/// <reference path="../../../typings/tsd.d.ts" />

import getLicenseStatusCommand = require("commands/auth/getLicenseStatusCommand");

class licenseModel {
    static licenseStatus = ko.observable<Raven.Server.Commercial.LicenseStatus>();
    static supportCoverage = ko.observable<supportCoverageDto>();

    static fetchLicenseStatus(): JQueryPromise<Raven.Server.Commercial.LicenseStatus> {
        return new getLicenseStatusCommand()
            .execute()
            .done((result: Raven.Server.Commercial.LicenseStatus) => {
                if (result.Status.includes("AGPL")) {
                    result.Status = "Development Only";
                }
                licenseModel.licenseStatus(result);
            });
    }

    static licenseShortDescription = ko.computed(() => {
        var status = licenseModel.licenseStatus();
        if (!status) {
            return 'no-license';
        }
       
        const maxMemory = status.MaxMemory === 0 ? "Unlimited" : `${status.MaxMemory} GB RAM` ;
        return `${status.MaxCores} Cores, ${maxMemory}, Cluster size: ${status.MaxClusterSize}`;
    });


    static licenseCssClass = ko.computed(() => {
        var status = licenseModel.licenseStatus();
        if (!status) {
            return 'no-license';
        }
        if (status.Status.includes("Expired")) {
            return 'expired';
        } else {
            return 'commercial';
        }
    });

    static supportCssClass = ko.computed(() => {
        var support = licenseModel.supportCoverage();
        if (!support) {
            return 'no-support';
        }
        switch (support.Status) {
            case 'ProductionSupport':
                return 'production-support';
            case 'ProfessionalSupport':
                return 'professional-support';
            case 'PartialSupport':
                return 'partial-support';
            default:
                return 'no-support';
        }
    });
}

export = licenseModel;

/// <reference path="../../../typings/tsd.d.ts" />

import getLicenseStatusCommand = require("commands/auth/getLicenseStatusCommand");

class license {
    static licenseStatus = ko.observable<Raven.Server.Commercial.LicenseStatus>();
    static supportCoverage = ko.observable<supportCoverageDto>();
    static hotSpare = ko.observable<HotSpareDto>();

    static fetchLicenseStatus(): JQueryPromise<Raven.Server.Commercial.LicenseStatus> {
        return new getLicenseStatusCommand()
            .execute()
            .done((result: Raven.Server.Commercial.LicenseStatus) => {
                if (result.Status.includes("AGPL")) {
                    result.Status = "Development Only";
                }
                license.licenseStatus(result);
            });
    }

    static licenseCssClass = ko.computed(() => {
        var status = license.licenseStatus();
        var hotSpare = license.hotSpare();
        if (hotSpare) {
            return 'hot-spare';
        }
        if (status == null || status.Type !== "Commercial") {
            return 'dev-only';
        }
        if (status.Status.includes("Expired")) {
            return 'expired';
        } else {
            return 'commercial';
        }
    });

    static supportCssClass = ko.computed(() => {
        var support = license.supportCoverage();
        if (support == null) {
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

export = license;

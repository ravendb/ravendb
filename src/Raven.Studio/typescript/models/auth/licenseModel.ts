/// <reference path="../../../typings/tsd.d.ts" />

import getLicenseStatusCommand = require("commands/licensing/getLicenseStatusCommand");
import buildInfo = require("models/resources/buildInfo");
import licenseSupportInfoCommand = require("commands/licensing/licenseSupportInfoCommand");

class licenseModel {
    static licenseStatus = ko.observable<Raven.Server.Commercial.LicenseStatus>();
    static supportCoverage = ko.observable<Raven.Server.Commercial.LicenseSupportInfo>();

    private static baseUrl = "https://ravendb.net/license/request";

    static generateLicenseRequestUrl(limitType: Raven.Client.Exceptions.Commercial.LimitType = null): string {
        let url = `${licenseModel.baseUrl}?`;

        const build = buildInfo.serverBuildVersion();
        if (build) {
            url += `&build=${build.BuildVersion}`;
        }

        const status = this.licenseStatus();
        if (status && status.Id) {
            url += `&id=${btoa(status.Id)}`;
        }

        if (limitType) {
            url += `&limit=${btoa(limitType)}`;
        }

        return url;
    }

    static fetchSupportCoverage(): JQueryPromise<Raven.Server.Commercial.LicenseSupportInfo> {
        return new licenseSupportInfoCommand()
            .execute()
            .done((result: Raven.Server.Commercial.LicenseSupportInfo) => {
                licenseModel.supportCoverage(result);
            });
    }
    
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

    static licenseShortDescription = ko.pureComputed(() => {
        const status = licenseModel.licenseStatus();
        if (!status || status.Type === "None") {
            return null;
        }
       
        const maxMemory = status.MaxMemory === 0 ? "Unlimited" : `${status.MaxMemory} GB RAM`;
        const maxClusterSize = status.MaxClusterSize === 0 ? "Unlimited" : status.MaxClusterSize;
        return `${status.MaxCores} Cores, ${maxMemory}, Max cluster size: ${maxClusterSize}`;
    });

    static licenseId = ko.pureComputed(() => {
        const status = licenseModel.licenseStatus();
        if (!status || status.Type === "None") {
            return null;
        }

        return status.Id;
    });

    static developerLicense = ko.pureComputed(() => {
        const licenseStatus = licenseModel.licenseStatus();
        
        if (!licenseStatus || licenseStatus.Type !== "Developer") {
            return false;
        }

        return true;
    });
    
    static licenseCssClass = ko.pureComputed(() => {
        const status = licenseModel.licenseStatus();
        if (!status || status.Type === "None") {
            return 'no-license';
        }
        if (status.Status.includes("Expired")) {
            return 'expired';
        } else if (status.Type === "Invalid") {
            return 'invalid';
        } else {
            return 'valid';
        }
    });

    static supportCssClass = ko.pureComputed(() => {
        const support = licenseModel.supportCoverage();
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
    
    static supportLabel = ko.pureComputed(() => {
        const licenseStatus = licenseModel.licenseStatus();
        if (!licenseStatus || licenseStatus.Type === "None") {
            return 'Community';
        }
        
        const supportInfo = licenseModel.supportCoverage();
        if (!supportInfo) {
            return 'Community';
        }

        const supportType = supportInfo.Status || "NoSupport";
        switch (supportType) {
            case 'ProductionSupport':
                return 'Production';
            case 'ProfessionalSupport':
                return 'Professional';
            case 'PartialSupport':
                return 'Partial';
            default:
                return 'Community';
        }
    });
    
    static supportTableCssClass = ko.pureComputed(() => {
        const label = licenseModel.supportLabel();
        return label.toLocaleLowerCase();
    });
}

export = licenseModel;

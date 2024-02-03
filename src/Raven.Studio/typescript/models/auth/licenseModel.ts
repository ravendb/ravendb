/// <reference path="../../../typings/tsd.d.ts" />

import getLicenseStatusCommand = require("commands/licensing/getLicenseStatusCommand");
import buildInfo = require("models/resources/buildInfo");
import generalUtils = require("common/generalUtils");
import moment = require("moment");
import licenseSupportInfoCommand = require("commands/licensing/licenseSupportInfoCommand");

class licenseModel {
    static licenseStatus = ko.observable<LicenseStatus>();
    static supportCoverage = ko.observable<Raven.Server.Commercial.LicenseSupportInfo>();

    private static baseUrl = "https://ravendb.net/license/request";

    static licenseTypeText = ko.pureComputed(() => licenseModel.licenseTypeTextProvider(licenseModel.licenseStatus())); 

    static licenseTypeTextProvider(licenseStatus: LicenseStatus) {
        if (!licenseStatus || licenseStatus.Type === "None") {
            return "No license - AGPLv3 Restrictions Applied";
        }

        let licenseType = licenseStatus.Type;
        if (licenseType === "Invalid") {
            return "Invalid license";
        }

        if (licenseStatus.IsCloud) {
            licenseType += " (Cloud)";
        }

        if (licenseStatus.IsIsv) {
            licenseType += " (ISV)";
        }

        return licenseType;
    }

    static formattedExpiration = ko.pureComputed(() => {
        const result =  licenseModel.formattedExpirationProvider(licenseModel.licenseStatus());
        if (!result ) {
            return null;
        }

        return `${result.formattedDate} <br /><small class="${result.timeClass}">(${result.relativeTime})</small>`;
    });

    static formattedExpirationProvider(licenseStatus: LicenseStatus): { formattedDate: string; timeClass: string; relativeTime: string } {
        if (!licenseStatus || !licenseStatus.Expiration) {
            return null;
        }

        const dateFormat = "YYYY MMMM Do";
        const expiration = moment(licenseStatus.Expiration);
        const now = moment();
        const month = 1 as const;
        const nextMonth = moment().add(month, 'month');
        if (now.isBefore(expiration)) {
            const relativeDurationClass = nextMonth.isBefore(expiration) ? "" : "text-warning";

            const fromDuration = generalUtils.formatDurationByDate(expiration, true);
            return {
                formattedDate: expiration.format(dateFormat),
                timeClass: relativeDurationClass, 
                relativeTime: fromDuration
            }
        }

        const expiredClass = licenseStatus.Expired ? "text-danger" : "";
        const duration = generalUtils.formatDurationByDate(expiration, true);
        return {
            formattedDate: expiration.format(dateFormat),
            timeClass: expiredClass,
            relativeTime: duration + " ago"
        }
       
    }
    
        
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
    
    static fetchLicenseStatus(): JQueryPromise<LicenseStatus> {
        return new getLicenseStatusCommand()
            .execute()
            .done((result: LicenseStatus) => {
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

    static licenseType = ko.pureComputed(() => {
        return licenseModel.licenseStatus()?.Type ?? null;
    });

    static getStatusValue<T extends keyof LicenseStatus>(key: T) {
        return licenseModel.licenseStatus()?.[key] ?? null;
    }

    static isEnterpriseOrDeveloper = ko.pureComputed(() => {
        const type = licenseModel.licenseType();

        if (type === "Enterprise" || type === "Developer") {
            return true;
        }

        return false;
    });

    static isProfessionalOrAbove = ko.pureComputed(() => {
        if (licenseModel.isEnterpriseOrDeveloper() || licenseModel.licenseType() === "Professional") {
            return true;
        }

        return false;
    });

    static developerLicense = ko.pureComputed(() => {
        const licenseStatus = licenseModel.licenseStatus();
        
        if (!licenseStatus || licenseStatus.Type !== "Developer") {
            return false;
        }

        return true;
    });
    
    static cloudLicense = ko.pureComputed(() => {
        const licenseStatus = licenseModel.licenseStatus();
        
        return licenseStatus && licenseStatus.IsCloud;
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
        return licenseModel.supportLabelProvider(licenseModel.licenseStatus(), licenseModel.supportCoverage());
    });
    
    static supportLabelProvider(licenseStatus: LicenseStatus, supportInfo: Raven.Server.Commercial.LicenseSupportInfo) {
        if (!licenseStatus || licenseStatus.Type === "None") {
            return 'Community';
        }

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
    }
    
    static supportTableCssClass = ko.pureComputed(() => {
        const label = licenseModel.supportLabel();
        return label.toLocaleLowerCase();
    });
}

export = licenseModel;

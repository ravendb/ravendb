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
            return "Cloud";
        }

        if (licenseType === "EnterpriseAi") {
            return "RavenDB AI";
        }

        if (licenseStatus.IsIsv) {
            licenseType += " (ISV)";
        }

        return licenseType;
    }
    
    static licenseTypeShortText = ko.pureComputed(() => licenseModel.licenseTypeShortTextProvider(licenseModel.licenseStatus()));
    
    static licenseTypeShortTextProvider(licenseStatus: LicenseStatus): string {
        if (!licenseStatus || licenseStatus.Type === "None") {
            return "AGPL";
        }
        
        if (licenseStatus.Type === "Invalid") {
            return "Invalid";
        }
        
        switch (licenseStatus.Type) {
            case "EnterpriseAi":
                return "AI";
            case "Enterprise":
                return "Enterprise";
            case "Professional":
                return "Professional";
            case "Community":
                return "Community";
            case "Developer":
                return "Developer";
            case "Essential":
                return "Essential";
            default:
                return licenseStatus.Type;
        }
    }
    
    
    static formattedExpiration = ko.pureComputed(() => {
        const licenseStatus = licenseModel.licenseStatus();
        if (!licenseStatus || !licenseStatus.SubscriptionExpiration) {
            return null;
        }

        const dateFormat = "YYYY MMMM Do";
        const expiration = moment.utc(licenseStatus.SubscriptionExpiration);
        const now = moment.utc();
        const nextMonth = moment.utc().add(1, 'month');
        if (now.isBefore(expiration)) {
            const relativeDurationClass = nextMonth.isBefore(expiration) ? "" : "text-warning";

            const fromDuration = generalUtils.formatDurationByDate(expiration, true);
            return `${expiration.format(dateFormat)} UTC ${this.getLicenseInfoIcon({ date: expiration, isExpired: false })}<br /><small class="${relativeDurationClass}">(${fromDuration})</small>`;
        }

        const expiredClass = licenseStatus.Expired ? "text-danger" : "";
        const duration = generalUtils.formatDurationByDate(expiration, true);

        return `${expiration.format(dateFormat)} UTC ${this.getLicenseInfoIcon({ date: expiration, isExpired: true })}<br /><Small class="${expiredClass}">(${duration})</Small>`;
    });

    static getLicenseInfoIcon({ date, isExpired, isSmall = true }: { date: moment.Moment, isExpired: boolean, isSmall?: boolean }): string {
        return `<i class="icon-info text-info"
            title="Your license ${isExpired ? "has expired on" : "will expire at the end of"} ${date.format("YYYY-MM-DD")} UTC, which ${isExpired ? "was" : "is"} ${date.local().format("YYYY-MM-DD HH:mm:ss")} your local time."
            style="font-size: ${isSmall ? "16px" : undefined}">
        </i>`;
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
    
    static licenseBgColorClass = ko.pureComputed(() => {
        const licenseType = licenseModel.licenseType();
        
        switch (licenseType) {
            case "Community":
                return "community";
            case "Developer":
                return "developer";
            case "Enterprise":
                return "enterprise";
            case "EnterpriseAi":
                return "enterprise-ai";
            case "Professional":
                return "professional";
            case "Invalid":
                return "danger";
            case "Essential":
            default:
                return "no-background";
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
    
    static licenseStatusTooltip = ko.pureComputed(() => {
        const status = licenseModel.licenseStatus();
        
        if (!status || status.Type === "None") {
            return 'No license - AGPLv3 restrictions applied';
        }
        
        if (status.Type === "Invalid") {
            return 'Invalid license';
        }
        
        if (status.Status.includes("Expired")) {
            return 'License has expired';
        }
        
        return 'Valid license';
    });
}

export = licenseModel;

import viewModelBase = require("viewmodels/viewModelBase");
import license = require("models/auth/licenseModel");
import registration = require("viewmodels/shell/registration");
import buildInfo = require("models/resources/buildInfo");
import accessManager = require("common/shell/accessManager");
import forceLicenseUpdateCommand = require("commands/licensing/forceLicenseUpdateCommand");
import getLatestVersionInfoCommand = require("commands/version/getLatestVersionInfoCommand");
import getLicenseConfigurationSettingsCommand = require("commands/licensing/getLicenseConfigurationSettingsCommand");
import getConnectivityToLicenseServerCommand = require("commands/licensing/getConnectivityToLicenseServerCommand");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import appUrl = require("common/appUrl");
import popoverUtils = require("common/popoverUtils");
import app = require("durandal/app");
import feedback from "viewmodels/shell/feedback";

type newVersionStatus = "available" | "latest" | "checksDisabled";

class about extends viewModelBase {

    view = require("views/shell/about.html");

    accessManager = accessManager.default.aboutView;
    
    connectedToLicenseServer = ko.observable<boolean>();
    connectionException = ko.observable<string>();
    
    clusterViewUrl = appUrl.forCluster();
    passiveNode = ko.pureComputed(() => clusterTopologyManager.default.topology().isPassive());
    
    licenseCssClass = license.licenseCssClass;
    supportCssClass = license.supportCssClass;
    
    supportLabel = license.supportLabel;
    supportTableCssClass = license.supportTableCssClass;
    
    clientVersion = viewModelBase.clientVersion;
    serverVersion = buildInfo.serverBuildVersion;

    developerLicense = license.developerLicense;

    static latestVersion = ko.observable<Raven.Server.ServerWide.BackgroundTasks.LatestVersionCheck.VersionInfo>();
    currentServerVersion = ko.pureComputed(() => this.serverVersion() ? this.serverVersion().FullVersion : "");

    newVersionStatus = ko.pureComputed((): newVersionStatus => {
        const latestVersionInfo = about.latestVersion();
        if (!latestVersionInfo) {
            return "checksDisabled";
        }

        const serverVersion = this.serverVersion();
        if (!serverVersion) {
            return "checksDisabled";
        }

        const isDevBuildNumber = (num: number) => num >= 40 && num < 60;

        return (!isDevBuildNumber(latestVersionInfo.BuildNumber) &&
            latestVersionInfo.BuildNumber > serverVersion.BuildVersion) ? "available" : "latest";
    });

    newVersionAvailableHtml = ko.pureComputed(() => {
        switch (this.newVersionStatus()) {
            case "available":
                return `New version available<br/> <span class="nobr">${ about.latestVersion().Version }</span>`;
            case "latest":
                return `You are using the latest version`;
            default:
                return null;
        }
    });
    
    latestVersionWhatsNewUrl = ko.pureComputed(() => 
        `https://ravendb.net/whats-new?buildNumber=${ about.latestVersion().BuildNumber }`);

    maxClusterSize = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        return licenseStatus ? licenseStatus.MaxClusterSize : 1;
    });
    
    maxCores = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        return licenseStatus ? licenseStatus.MaxCores : 3;
    });
    
    maxMemory = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        return licenseStatus ? licenseStatus.MaxMemory : 6;
    });
    
    canUpgradeSupport = ko.pureComputed(() => {
        const support = license.supportCoverage();
        if (!support || !support.Status) {
            return true;
        }
        
        return support.Status !== "ProductionSupport";
    });

    spinners = {
        forceLicenseUpdate: ko.observable<boolean>(false),
        renewLicense: ko.observable<boolean>(false),
        latestVersionUpdates: ko.observable<boolean>(false),
        checkConnectionToLicenseServer: ko.observable<boolean>(false)
    };

    expiresText = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        if (!licenseStatus || !licenseStatus.Expiration) {
            return null;
        }

        return licenseStatus.IsIsv ? "Updates Expiration" : "Expires";
    });

    formattedExpiration = license.formattedExpiration;

    isCloud = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        return licenseStatus && licenseStatus.IsCloud;
    });

    automaticRenewText = ko.pureComputed(() => {
        return this.isCloud() ? "Cloud licenses are automatically renewed" : "";
    });
    
    licenseType = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        return !licenseStatus ? "None" : licenseStatus.Type;
    });

    licenseTypeText = license.licenseTypeText;

    hasLicense = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        return licenseStatus && licenseStatus.Type !== "None";
    });

    shortDescription = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        const shortDescription = license.licenseShortDescription();
        if (!licenseStatus || !shortDescription) {
            return null;
        }

        return shortDescription;
    });

    licenseId = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        const licenseId = license.licenseId();
        if (!licenseStatus || !licenseId) {
            return null;
        }

        return licenseId;
    });
    
    licensedTo = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        if (!licenseStatus) {
            return null;
        }

        return licenseStatus.LicensedTo;
    });

    registered = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        if (!licenseStatus) {
            return false;
        }

        return licenseStatus.Type !== "None" && licenseStatus.Type !== "Invalid";
    });
    
    canRenewLicense = ko.pureComputed(() => {
        return this.licenseType() === 'Developer' || this.licenseType() === 'Community';
    });
    
    licenseAttribute(name: keyof Raven.Server.Commercial.LicenseStatus) {
        return ko.pureComputed(() => {
           const licenseStatus = license.licenseStatus();
           if (licenseStatus) {
               return licenseStatus[name] ? "icon-checkmark" : "icon-cancel";
           }
           return "icon-cancel";
        });
    }

    isRegisterLicenseEnabled = ko.observable<boolean>(false);
    registerTooltip = ko.observable<string>();
    
    isReplaceLicenseEnabled = ko.observable<boolean>(false);
    replaceTooltip = ko.observable<string>();

    isForceUpdateEnabled = ko.observable<boolean>(false);
    forceUpdateTooltip = ko.observable<string>();
    
    isRenewLicenseEnabled = ko.observable<boolean>(false);
    renewTooltip = ko.observable<string>();
    
    register() {
        registration.showRegistrationDialog(license.licenseStatus(), false, true);
    }

    private getLicenseConfigurationSettings() {
        return new getLicenseConfigurationSettingsCommand()
            .execute()
            .done((result: Raven.Server.Config.Categories.LicenseConfiguration) => {
                const access = this.accessManager;
                
                const canRegister = result.CanActivate && access.canRegisterLicense();
                this.isRegisterLicenseEnabled(canRegister);
                
                const registerMsg = this.getTooltipContent(result.CanActivate, access.canRegisterLicense(),
                    "register a new license", "Registering new license");
                this.registerTooltip(registerMsg);
                
                const canReplace = result.CanActivate && access.canReplaceLicense();
                this.isReplaceLicenseEnabled(canReplace);

                const replaceMsg = this.getTooltipContent(result.CanActivate, access.canReplaceLicense(),
                    "replace the current license with another license", "Replacing license");
                this.replaceTooltip(replaceMsg);
                
                const canForceUpdate = result.CanForceUpdate && access.canForceUpdate();
                this.isForceUpdateEnabled(canForceUpdate);

                const forceUpdateMsg = this.getTooltipContent(result.CanForceUpdate, access.canForceUpdate(),
                    "apply the license that was set for you", "Force license update");
                this.forceUpdateTooltip(forceUpdateMsg);

                const canRenew = result.CanRenew && access.canRenewLicense();
                this.isRenewLicenseEnabled(canRenew);

                const renewMsg = this.getTooltipContent(result.CanRenew, access.canRenewLicense(),
                    "renew license. Expiration date will be extended", "Renew");
                this.renewTooltip(renewMsg);
            });
    }
    
    private getTooltipContent(operationEnabledInConfiguration: boolean, hasPrivileges: boolean, operationAction: string, operationTitle: string) {
        let msg = operationEnabledInConfiguration && hasPrivileges ? `Click to ${operationAction}` : "";

        if (!operationEnabledInConfiguration) {
            msg = `${operationTitle} is disabled in the server configuration.`;
        }
        
        if (!hasPrivileges) {
            msg += " You have insufficient privileges. Only a Cluster Admin can do this.";
        }
        
        return msg;
    }
    
    private pullLatestVersionInfo() {
        this.spinners.latestVersionUpdates(true);
        return new getLatestVersionInfoCommand()
            .execute()
            .done(versionInfo => {
                if (versionInfo && versionInfo.Version) {
                    about.latestVersion(versionInfo);
                } else {
                    about.latestVersion(null);
                }
            })
            .always(() => this.spinners.latestVersionUpdates(false));
    }

    refreshLatestVersionInfo() {
        this.spinners.latestVersionUpdates(true);
        const cmd = new getLatestVersionInfoCommand(true);
        cmd.execute()
            .done(versionInfo => {
                if (versionInfo && versionInfo.Version) {
                    about.latestVersion(versionInfo);
                } else {
                    about.latestVersion(null);
                }
            })
            .always(() => this.spinners.latestVersionUpdates(false));
    }

    openLatestVersionDownload() {
        window.open("https://ravendb.net/downloads", "_blank");
    }

    async forceLicenseUpdate() {
        const isConfirmed = await this.confirmationMessage("Force License Update", "Are you sure that you want to force license update?");
        if (!isConfirmed) {
            return;
        }

        try {
            this.spinners.forceLicenseUpdate(true);

            const updateResult = await new forceLicenseUpdateCommand().execute();
            const licenseStatus = await license.fetchLicenseStatus();

            if (updateResult.Status === "NotModified") {
                forceLicenseUpdateCommand.handleNotModifiedStatus(licenseStatus.Expired);
            }

            await license.fetchSupportCoverage();
            
        } finally {
            this.spinners.forceLicenseUpdate(false);
        }
    }

    renewLicense() {
        registration.showRegistrationDialog(license.licenseStatus(), false, true, true);
    }

    openFeedbackForm() {
        const dialog = new feedback(viewModelBase.clientVersion(), buildInfo.serverBuildVersion().FullVersion);
        app.showBootstrapDialog(dialog);
    }

    checkConnectionToLicenseServer() {
        this.spinners.checkConnectionToLicenseServer(true);
        return new getConnectivityToLicenseServerCommand()
            .execute()
            .done((connectionResult: Raven.Server.Web.Studio.LicenseHandler.ConnectivityToLicenseServer) => {
                this.connectedToLicenseServer(connectionResult.StatusCode === "OK");
                this.connectionException(connectionResult.Exception || "");
            })
            .always(() => this.spinners.checkConnectionToLicenseServer(false));
    }

    activate(args: any) {
        super.activate(args, { shell: true });
        return $.when<any>(this.getLicenseConfigurationSettings(),
                           this.pullLatestVersionInfo(),
                           license.fetchLicenseStatus(),
                           this.checkConnectionToLicenseServer());
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        popoverUtils.longWithHover($(".not-connected"),
            {
                content:
                    `<small><small>Unable to reach the RavenDB License Server at <code>api.ravendb.net</code><br>
                     ${this.connectionException()}</small></small>`,
                placement: "bottom"
            });
}
}


export = about;

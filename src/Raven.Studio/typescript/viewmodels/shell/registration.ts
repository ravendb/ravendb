import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import app = require("durandal/app");
import dialog = require("plugins/dialog");
import licenseActivateCommand = require("commands/licensing/licenseActivateCommand");
import moment = require("moment");
import license = require("models/auth/licenseModel");
import messagePublisher = require("common/messagePublisher");
import forceLicenseUpdateCommand = require("commands/licensing/forceLicenseUpdateCommand");
import renewLicenseCommand = require("commands/licensing/renewLicenseCommand");
import getServerCertificateSetupModeCommand = require("commands/auth/getServerCertificateSetupModeCommand");

class licenseKeyModel {

    key = ko.observable<string>();

    constructor() {
        this.setupValidation();
    }

    private setupValidation() {

        this.key.extend({
            required: true,
            validLicense: true 
        });
    }
}

class registrationDismissStorage {

    private static readonly storageKey = "registrationDismiss";

    static getDismissedUntil(): Date {
        const storedValue = localStorage.getObject(registrationDismissStorage.storageKey);
        if (storedValue) {
            return new Date(storedValue);
        }

        return null;
    }

    static dismissFor(days: number) {
        localStorage.setObject(registrationDismissStorage.storageKey, moment().add(days, "days").toDate().getTime());
    }

    static clearDismissStatus() {
        localStorage.removeItem(registrationDismissStorage.storageKey);
    }
}

class registration extends dialogViewModelBase {

    view = require("views/shell/registration.html");
    
    static readonly licenseDialogSelector = "#licenseModal";

    licenseExpired = ko.observable<boolean>(false);
    licenseType = ko.observable<Raven.Server.Commercial.LicenseType>();
    licenseId = ko.observable<string>();
    
    dismissVisible = ko.observable<boolean>(true);
    canBeClosed = ko.observable<boolean>(false);
    daysToRegister: KnockoutComputed<number>;

    forceUpdateWhenExpired: KnockoutComputed<boolean>;
    renewWhenExpired: KnockoutComputed<boolean>;
    renewWhenNotExpired = ko.observable<boolean>(false);

    renewMessage = ko.observable<string>();
    renewError= ko.observable<string>();
    
    registrationUrl = ko.observable<string>();
    error = ko.observable<string>();

    private licenseKeyModel = ko.validatedObservable(new licenseKeyModel());
    private licenseStatus: Raven.Server.Commercial.LicenseStatus;

    private hasInvalidLicense = ko.observable<boolean>(false);
    private letsEncryptMode = ko.observable<boolean>(false); 

    spinners = {
        forceLicenseUpdate: ko.observable<boolean>(false),
        activateLicense: ko.observable<boolean>(false),
        renewLicense: ko.observable<boolean>(false)      
    };

    constructor(licenseStatus: Raven.Server.Commercial.LicenseStatus, canBeDismissed: boolean, canBeClosed: boolean, renewNonExpiredLicense: boolean = false) {
        super();
        
        this.licenseStatus = licenseStatus;
        
        this.licenseExpired(licenseStatus.Expired);
        this.licenseType(licenseStatus.Type);
        this.licenseId(licenseStatus.Id);

        this.bindToCurrentInstance("dismiss");

        this.dismissVisible(canBeDismissed);        
        this.canBeClosed(canBeClosed);

        let error: string = null;
        if (licenseStatus.Type === "Invalid") {
            error = "Invalid license";
            if (licenseStatus.ErrorMessage) {
                error += `: ${licenseStatus.ErrorMessage}`;
            }
        } else if (licenseStatus.Expired) {
            const expiration = moment(licenseStatus.Expiration);
            error = "License has expired";
            if (expiration.isValid()) {
                error += ` on ${expiration.format("YYYY MMMM Do")}`;
            }
        }
        this.error(error);

        const firstStart = moment(licenseStatus.FirstServerStartDate)
            .add("1", "week").add("1", "day");

        this.daysToRegister = ko.pureComputed(() => {
            const now = moment();
            return firstStart.diff(now, "days");
        });        
        
        this.forceUpdateWhenExpired = ko.pureComputed(() => {
            // Force Update is for: ALL licenses EXCEPT Developer and Community
            return this.licenseExpired() && 
                   this.licenseType() !== 'Developer' && this.licenseType() !== 'Community';
            
        });

        this.renewWhenExpired = ko.pureComputed(() => {
            // Renew is only for: Developer and community 
            return this.licenseExpired() &&
                   (this.licenseType() === 'Developer' || this.licenseType() === 'Community');
            
        });

        this.renewWhenNotExpired(renewNonExpiredLicense);

        this.registrationUrl(license.generateLicenseRequestUrl());
        
        this.registerDisposable(license.licenseStatus.subscribe(statusUpdated => {
            if (!statusUpdated.Expired && !statusUpdated.ErrorMessage && !this.renewWhenNotExpired()) {
                app.closeDialog(this);
            }
        }))
    }

    activate() {
        return new getServerCertificateSetupModeCommand()
            .execute()
            .done((setupMode: Raven.Server.Commercial.SetupMode) => {
                if (setupMode === "LetsEncrypt") {
                    this.letsEncryptMode(true);
                }
             });
    }
    
    static showRegistrationDialogIfNeeded(license: Raven.Server.Commercial.LicenseStatus, skipIfNoLicense = false) {
        switch (license.Type) {
            case "Invalid":
                registration.showRegistrationDialog(license, false, false);
                break;
                
            case "None":
                if (skipIfNoLicense) {
                    return;
                }

                const firstStart = moment(license.FirstServerStartDate);
                // add mutates the original moment
                const dayAfterFirstStart = firstStart.clone().add("1", "day");
                const weekAfterFirstStart = firstStart.clone().add("1", "week");

                const now = moment();
                if (now.isBefore(dayAfterFirstStart)) {
                    return;
                }

                let shouldShow: boolean;
                let canDismiss: boolean;

                if (now.isBefore(weekAfterFirstStart)) {
                    const dismissedUntil = registrationDismissStorage.getDismissedUntil();
                    shouldShow = !dismissedUntil || dismissedUntil.getTime() < new Date().getTime();
                    canDismiss = true;
                } else {
                    shouldShow = true;
                    canDismiss = false;
                }

                if (shouldShow) {
                    registration.showRegistrationDialog(license, canDismiss, false);
                }
                break;
            default:
                if (license.Expired) {
                    registration.showRegistrationDialog(license, false, false);
                }
                break;
        }
    }

    static showRegistrationDialog(license: Raven.Server.Commercial.LicenseStatus, canBeDismissed: boolean, canBeClosed: boolean, renewNonExpiredLicense: boolean = false) {
        if ($("#licenseModal").is(":visible") && $("#enterLicenseKey").is(":visible")) {
            return;
        }

        const vm = new registration(license, canBeDismissed, canBeClosed, renewNonExpiredLicense);
        app.showBootstrapDialog(vm);
    }

    forceLicenseUpdate() {
        this.spinners.forceLicenseUpdate(true);

        new forceLicenseUpdateCommand().execute()
            .done(() => {
                license.fetchLicenseStatus()
                    .done(() => {
                        const licenseStatus = license.licenseStatus();
                        if (!licenseStatus.Expired &&
                            !licenseStatus.ErrorMessage) {
                            app.closeDialog(this);
                        }
                        license.fetchSupportCoverage();
                    });
            })
            .always(() => this.spinners.forceLicenseUpdate(false));
    }

    renewLicense() {
        this.spinners.renewLicense(true);

        new renewLicenseCommand().execute()
            .done(result => {
                license.fetchLicenseStatus()
                    .done(() => {
                        const licenseStatus = license.licenseStatus();
                        if (!licenseStatus.Expired &&
                            !licenseStatus.ErrorMessage &&
                            !this.renewWhenNotExpired() ) {
                                app.closeDialog(this);
                        }
                        license.fetchSupportCoverage();
                    });
                
                if (result.Error) {
                    this.renewError(result.Error);
                    this.renewMessage(null);
                } else {
                    this.renewError(null);
                    this.renewMessage(this.composeRenewMessage(result.SentToEmail, result.NewExpirationDate));
                }
             })
            .always(() => this.spinners.renewLicense(false));
    }

    private composeRenewMessage(sentToEmail: string, newExpirationDate: string) : string {

        let newExpirationDateFormatted: string;
        const newExpiration = moment(newExpirationDate);
        if (newExpiration.isValid()) {
            newExpirationDateFormatted = newExpiration.format("YYYY MMMM Do");
        }
        
        return `The renewed license was sent to: <strong class="text-info">${sentToEmail}</strong><br/>The new expiration date is: <strong class="text-info">${newExpirationDateFormatted}</strong>`;
    }
    
    dismiss(days: number) {
        registrationDismissStorage.dismissFor(days);
        app.closeDialog(this);
    }

    close() {
        if (!this.canBeClosed()) {
            return;
        }

        super.close();
    }
    
    submit() {
        if (!this.isValid(this.licenseKeyModel)) {
            return;
        }

        this.spinners.activateLicense(true);

        const parsedLicense = JSON.parse(this.licenseKeyModel().key()) as Raven.Server.Commercial.License;
        new licenseActivateCommand(parsedLicense)
            .execute()
            .done(() => {
                license.fetchLicenseStatus()
                    .done(() => license.fetchSupportCoverage());

                dialog.close(this);
                messagePublisher.reportSuccess("Your license was successfully registered. Thank you for choosing RavenDB.");
            })
            .always(() => this.spinners.activateLicense(false));
    }
}

export = registration;




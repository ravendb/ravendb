import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import app = require("durandal/app");
import dialog = require("plugins/dialog");
import licenseActivateCommand = require("commands/licensing/licenseActivateCommand");
import moment = require("moment");
import license = require("models/auth/licenseModel");
import messagePublisher = require("common/messagePublisher");
import forceLicenseUpdateCommand = require("commands/licensing/forceLicenseUpdateCommand");

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
    
    static readonly licenseDialogSelector = "#licenseModal";

    dismissVisible = ko.observable<boolean>(true);
    canBeClosed = ko.observable<boolean>(false);
    daysToRegister: KnockoutComputed<number>;
    canForceUpdate: KnockoutComputed<boolean>;
    registrationUrl = ko.observable<string>();
    error = ko.observable<string>();

    private licenseKeyModel = ko.validatedObservable(new licenseKeyModel());
    private licenseStatus: Raven.Server.Commercial.LicenseStatus;

    private hasInvalidLicense = ko.observable<boolean>(false);

    spinners = {
        forceLicenseUpdate: ko.observable<boolean>(false),
        activateLicense: ko.observable<boolean>(false)
    };

    constructor(licenseStatus: Raven.Server.Commercial.LicenseStatus, canBeDismissed: boolean, canBeClosed: boolean) {
        super();
        this.licenseStatus = licenseStatus;

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
            error = "License expired";
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

        this.canForceUpdate = ko.pureComputed(() => {
            return licenseStatus.Expired || !!this.error();
        });

        this.registrationUrl(license.generateLicenseRequestUrl());
        
        this.registerDisposable(license.licenseStatus.subscribe(statusUpdated => {
            if (!statusUpdated.Expired && !statusUpdated.ErrorMessage) {
                app.closeDialog(this);
            }
        }))
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
            default:
                if (license.Expired) {
                    registration.showRegistrationDialog(license, false, false);
                }
                break;
        }
    }

    static showRegistrationDialog(license: Raven.Server.Commercial.LicenseStatus, canBeDismissed: boolean, canBeClosed: boolean) {
        if ($("#licenseModal").is(":visible") && $("#enterLicenseKey").is(":visible")) {
            return;
        }

        const vm = new registration(license, canBeDismissed, canBeClosed);
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




import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import app = require("durandal/app");
import dialog = require("plugins/dialog");
import licenseActivateCommand = require("commands/licensing/licenseActivateCommand");
import moment = require("moment");
import license = require("models/auth/license");

class licenseKeyModel {

    key = ko.observable<string>();

    constructor() {
        this.setupValidation();
    }

    private setupValidation() {

        const licenseValidator = (license: string) => {

            try {
                const parsedLicense = JSON.parse(license);

                const hasId = "Id" in parsedLicense;
                const hasName = "Name" in parsedLicense;
                const hasKeys = "Keys" in parsedLicense;

                return hasId && hasName && hasKeys;
            } catch (e) {
                return false;
            }
        }

        this.key.extend({
            required: true,
            validation: [{
                validator: licenseValidator,
                message: "Invalid license format"
            }]
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

    isBusy = ko.observable<boolean>(false);
    dismissVisible = ko.observable<boolean>(true);
    daysToRegister: KnockoutComputed<number>;

    private licenseKeyModel = ko.validatedObservable(new licenseKeyModel());
    private license: Raven.Server.Commercial.LicenseStatus;

    private hasInvalidLicense = ko.observable<boolean>(false);

    constructor(license: Raven.Server.Commercial.LicenseStatus, canBeDismissed: boolean) {
        super();
        this.license = license;

        this.bindToCurrentInstance("dismiss");

        this.dismissVisible(canBeDismissed);

        const firstStart = moment(license.FirstServerStartDate)
            .add("1", "week").add("1", "day");

        this.daysToRegister = ko.pureComputed(() => {
            const now = moment();
            return firstStart.diff(now, "days");
        });
    }

    static showRegistrationDialogIfNeeded(license: Raven.Server.Commercial.LicenseStatus) {
        if (license.Type === "Invalid") {
            registration.showRegistrationDialog(license, false);
            return;
        }

        if (license.Type === "None") {
            const firstStart = moment(license.FirstServerStartDate);
            // add mutates the original moment
            const dayAfterFirstStart = firstStart.clone().add("1", "day");
            const weekAfterFirstStart = dayAfterFirstStart.clone().add("1", "week");

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
                registration.showRegistrationDialog(license, canDismiss);
            }
        }
    }

    static showRegistrationDialog(license: Raven.Server.Commercial.LicenseStatus, canBeDismissed: boolean) {
        const vm = new registration(license, canBeDismissed);
        app.showBootstrapDialog(vm);
    }

    dismiss(days: number) {
        registrationDismissStorage.dismissFor(days);
        app.closeDialog(this);
    }

    submit() {
        if (!this.isValid(this.licenseKeyModel)) {
            return;
        }

        //TODO: parse pasted key into json and validate

        this.isBusy(true);

        const parsedLicense = JSON.parse(this.licenseKeyModel().key()) as Raven.Server.Commercial.License;
        new licenseActivateCommand(parsedLicense)
            .execute()
            .done(() => {
                license.fetchLicenseStatus();

                dialog.close(this);
            })
            .always(() => this.isBusy(false));
    }
}

export = registration;




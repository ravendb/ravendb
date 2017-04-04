import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import app = require("durandal/app");
import dialog = require("plugins/dialog");

import licenseRegistrationCommand = require("commands/licensing/licenseRegistrationCommand");
import licenseActivateCommand = require("commands/licensing/licenseActivateCommand");

import moment = require("moment");
import license = require("models/auth/license");


class registrationModel {
    firstName = ko.observable<string>();
    lastName = ko.observable<string>();
    email = ko.observable<string>();
    company = ko.observable<string>();

    constructor() {
        this.setupValidation();
    }

    private setupValidation() {
        this.firstName.extend({
            required: true
        });

        this.lastName.extend({
            required: true
        });

        this.email.extend({
            required: true,
            email: true
        });
    }

    toDto(): Raven.Server.Commercial.UserRegistrationInfo {
        return {
            FirstName: this.firstName(),
            LastName: this.lastName(),
            Email: this.email(),
            Company: this.company(),
            BuildInfo: null
        }
    }
}

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
        localStorage.setObject(registrationDismissStorage.storageKey, moment().add(5, "days").toDate().getTime());
    }

    static clearDismissStatus() {
        localStorage.removeItem(registrationDismissStorage.storageKey);
    }
}

class registration extends dialogViewModelBase {

    isBusy = ko.observable<boolean>(false);
    licenseKeySectionActive = ko.observable<boolean>(false);
    justRegistered = ko.observable<boolean>(false);
    dismissVisible = ko.observable<boolean>(true);

    private registrationModel = ko.validatedObservable(new registrationModel());
    private licenseKeyModel = ko.validatedObservable(new licenseKeyModel());
    private license: Raven.Server.Commercial.LicenseStatus;

    private hasInvalidLicense = ko.observable<boolean>(false);

    constructor(license: Raven.Server.Commercial.LicenseStatus, canBeDismissed: boolean) {
        super();
        this.license = license;

        this.bindToCurrentInstance("dismiss");

        this.dismissVisible(canBeDismissed);
    }


    static showRegistrationDialogIfNeeded(license: Raven.Server.Commercial.LicenseStatus) {
        if (license.Type === "Invalid") {
            registration.showRegistrationDialog(license, false);
            return;
        }

        if (license.Type === "None") {
            const firstStart = moment(license.FirstServerStartDate);
            const weekAfterFirstStart = firstStart.add("1", "week");
            const treeWeeksAfterFirstStart = firstStart.add("3", "weeks");

            const now = moment();

            let shouldShow = false;
            let canDismiss = false;

            if (now.isBefore(weekAfterFirstStart)) {
                shouldShow = false;
            } else {
                if (now.isAfter(treeWeeksAfterFirstStart)) {
                    shouldShow = true;
                    canDismiss = false;
                } else {
                    // show if not dismissed
                    const dismissedUntil = registrationDismissStorage.getDismissedUntil();

                    canDismiss = true;
                    shouldShow = !dismissedUntil || dismissedUntil.getTime() < new Date().getTime();
                }
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

    goToEnterLicense() {
        this.licenseKeySectionActive(true);
    }

    goToRegistration() {
        this.licenseKeySectionActive(false);
    }

    submit() {
        if (this.licenseKeySectionActive()) {
            this.submitLicenseKey();
        } else {
            this.submitRegistration();
        }
    }

    private submitRegistration() {
        if (!this.isValid(this.registrationModel)) {
            return;
        }

        this.isBusy(true);

        new licenseRegistrationCommand(this.registrationModel().toDto())
            .execute()
            .done(() => {
                this.justRegistered(true);
                this.licenseKeySectionActive(true);
            })
            .always(() => this.isBusy(false));
    }

    private submitLicenseKey() {
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




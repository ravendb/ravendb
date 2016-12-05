import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import app = require("durandal/app");
import dialog = require("plugins/dialog");

import licenseRegistrationCommand = require("commands/licensing/licenseRegistrationCommand");
import licenseActivateCommand = require("commands/licensing/licenseActivateCommand");


class registrationModel {
    name = ko.observable<string>();
    email = ko.observable<string>();
    company = ko.observable<string>();

    constructor() {
        this.setupValidation();
    }

    private setupValidation() {
        this.name.extend({
            required: true
        });

        this.email.extend({
            required: true,
            email: true
        });
    }

    toDto() { //TODO: use server side type
        return {
            Name: this.name(),
            Email: this.email(),
            Company: this.company()
        }
    }
}

class licenseKeyModel {

    key = ko.observable<string>();

    constructor() {
        this.setupValidation();
    }

    private setupValidation() {
        this.key.extend({
            required: true
        });
    }
}

class registration extends dialogViewModelBase {

    licenseKeySectionActive = ko.observable<boolean>(false);
    justRegistered = ko.observable<boolean>(false);
    dismissVisible = ko.observable<boolean>(true);

    private registrationModel = ko.validatedObservable(new registrationModel());
    private licenseKeyModel = ko.validatedObservable(new licenseKeyModel());
    private license: licenseStatusDto;

    private hasInvalidLicense = ko.observable<boolean>(false);

    constructor(license: licenseStatusDto) {
        super();
        this.license = license;

        this.dismissVisible(license.LicenseType !== "Invalid"); //TODO: use type
    }

    static showRegistrationDialog(license: licenseStatusDto) {
        const vm = new registration(license);



        //TODO: only show when not dismissed 

        app.showBootstrapDialog(vm);
    }

    dismiss() {
        //TODO: impolement me!
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

        new licenseRegistrationCommand(this.registrationModel().toDto())
            .execute()
            .done(() => {
                this.justRegistered(true);
                this.licenseKeySectionActive(true);
            });
    }

    private submitLicenseKey() {
        if (!this.isValid(this.licenseKeyModel)) {
            return;
        }

        new licenseActivateCommand(this.licenseKeyModel().key())
            .execute()
            .done(() => {
                // TODO: on activated action
                // TODO: fetch license status?

                dialog.close(this);
            });
    }
}

export = registration;




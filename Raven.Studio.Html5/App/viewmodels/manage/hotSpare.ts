import viewModelBase = require("viewmodels/viewModelBase");
import getHotSpareInformation = require("commands/licensing/GetHotSpareInformation");
import testHotSpareCommand = require("commands/licensing/testHotSpareCommand");
import activateHotSpareCommand = require("commands/licensing/activateHotSpareCommand");
import shell = require("viewmodels/shell");

class hotSpare extends viewModelBase {
    runningOnExpiredLicense = ko.observable<boolean>(false);
    activationMode = ko.observable<string>();
    activationTime = ko.observable<string>();
    remainingTestActivation = ko.observable<number>();	
    isActivationExpired = ko.observable(false);
    testLicenseRequestProcessing = ko.observable(false);
    isTestEnabled: KnockoutComputed<boolean>;
    activateLicenseRequestProcessing = ko.observable(false);
    isActivateEnabled: KnockoutComputed<boolean>;

    constructor() {
        super();

        this.isTestEnabled = ko.computed(() => {
            return this.activationMode() === "NotActivated" &&
                this.testLicenseRequestProcessing() === false &&
                this.activateLicenseRequestProcessing() === false &&
                this.remainingTestActivation() > 0;
        });

        this.isActivateEnabled = ko.computed(() => {
            return this.activationMode() !== "Activated" &&
                this.activateLicenseRequestProcessing() === false &&
                this.testLicenseRequestProcessing() === false;
        });
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink("SV6IMV");
        this.fetchHotSpareInformation();
    }

    fetchHotSpareInformation() {
        new getHotSpareInformation().execute().done((res: HotSpareDto) => {
            this.activationMode(res.ActivationMode);
            this.activationTime(res.ActivationTime);
            this.remainingTestActivation(res.RemainingTestActivations);
        });
    }

    testLicense() {
        this.testLicenseRequestProcessing(true);
        new testHotSpareCommand().execute()
            .done(() => {
                this.remainingTestActivation(this.remainingTestActivation() - 1);
                this.activationMode("Testing");

                //refresh top navbar
                shell.fetchStudioConfig();
            })
            .fail(() => this.fetchHotSpareInformation())
            .always(() => this.testLicenseRequestProcessing(false));
    }

    activateLicense() {
        var self = this;
        
        this.confirmationMessage("Hot Spare Activation", "This is a one time activation, valid for 96 hours, are you sure you want to activate the hot spare license?")
            .done(() => {
                this.activateLicenseRequestProcessing(true);
                new activateHotSpareCommand()
                    .execute()
                    .done(() => {
                        this.fetchHotSpareInformation();

                        //refresh top navbar
                        shell.fetchStudioConfig();
                    })
                    .fail((response: JQueryXHR) => {
                        if (response.status === 403) {
                            self.isActivationExpired(true);
                        }
                    })
                    .always(() => this.activateLicenseRequestProcessing(false));
            });
    }
}

export = hotSpare;

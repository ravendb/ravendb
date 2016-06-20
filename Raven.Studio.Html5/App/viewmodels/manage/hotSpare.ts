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
        }).fail(() => {
            alert("Can't fetch license information");
        });
    }

    isTestEnabled(): boolean {
        return this.activationMode() === 'NotActivated';
    }

    testLicense() {
        new testHotSpareCommand().execute();
    }

    activateLicense() {
        var self = this;

        this.confirmationMessage("Hot Spare Activation", "This is a one time activation, valid for 96 hours, are you sure you want to activate the hot spare license?")
            .done(() => {
                new activateHotSpareCommand()
                    .execute()
                    .done(() => {
                        this.fetchHotSpareInformation();

                        // refresh top navbar with 
                        shell.fetchStudioConfig();
                    })
                    .fail((response: JQueryXHR) => {
                    if (response.status === 403) {
                        self.isActivationExpired(true);
                    }
                });
            });
    }
}

export = hotSpare;

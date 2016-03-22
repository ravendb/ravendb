import viewModelBase = require("viewmodels/viewModelBase");
import getHotSpareInformation = require("commands/licensing/GetHotSpareInformation");
import testHotSpareCommand = require("commands/licensing/testHotSpareCommand");
import activateHotSpareCommand = require("commands/licensing/activateHotSpareCommand");

class hotSpare extends viewModelBase {
    runningOnExpiredLicense = ko.observable<boolean>(false);
    activationMode = ko.observable<string>();
    activationTime = ko.observable<string>();
    remainingTestActivation = ko.observable<number>();	
    isActivationExpired = ko.observable(false);

    activate(args: any) {
        super.activate(args);
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
        new activateHotSpareCommand().execute().fail((response: JQueryXHR) => {
            if (response.status === 403) {
                self.isActivationExpired(true);
            }
        });
    }
}

export = hotSpare;

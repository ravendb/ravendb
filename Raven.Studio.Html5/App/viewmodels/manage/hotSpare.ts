import viewModelBase = require("viewmodels/viewModelBase");
import GetHotSpareInformation = require("commands/licensing/GetHotSpareInformation");
import TestHotSpareCommand = require("commands/licensing/TestLicenseCommand");
import ActivateHotSpareCommand = require("commands/licensing/ActivateLicenseCommand");
class hotSpare extends viewModelBase {
	runningOnExpiredLicense = ko.observable<boolean>(false);
	ActivationMode = ko.observable<string>();
	ActivationTime = ko.observable<string>();
	RemainingTestActivation = ko.observable<number>();	
	isActivationExpired = ko.observable(false);
	activate(args: any) {
		var self = this;
		super.activate(args);
		new GetHotSpareInformation().execute().done((res: HotSpareDto) => {
			self.ActivationMode(res.ActivationMode);
			self.ActivationTime(res.ActivationTime);
			self.RemainingTestActivation(res.RemainingTestActivations);
		}).fail(() => {
			alert("Can't fetch license information");
			});
	}
	isTestEnabled(): boolean {
		return (this.ActivationMode() === 'NotActivated');
	}
	Testlicense() {
		new TestHotSpareCommand().execute();
	}
	ActivateLicense() {
		var self = this;
		new ActivateHotSpareCommand().execute().fail(() => {
			self.isActivationExpired(true);
		});
	}
}

export = hotSpare;
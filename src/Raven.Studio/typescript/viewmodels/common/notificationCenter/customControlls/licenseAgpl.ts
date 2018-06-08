/// <reference path="../../../../../typings/tsd.d.ts" />

import registration = require("viewmodels/shell/registration");
import license = require("models/auth/licenseModel");

class licenseAgpl  {
    
    canUseUntil = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        if (!licenseStatus) {
            return null;
        }

        const now = moment();
        const firstStart = moment(licenseStatus.FirstServerStartDate);
        const weekAfterFirstStart = firstStart.clone().add("1", "week");

        return now.isBefore(weekAfterFirstStart) ? weekAfterFirstStart.format("YYYY-MM-DD") : null; 
    });
    
    register() {
        registration.showRegistrationDialog(license.licenseStatus(), false, true);
    }
}

export = licenseAgpl;
